/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.runtime.operators.schema.common;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.common.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetOriginalSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetOriginalSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.common.event.SinkWriterRegisterEvent;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import static org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils.wrap;

/**
 * An abstract centralized schema registry that accepts requests from {@link SchemaEvolutionClient}.
 * A legit schema registry should be able to:
 *
 * <ul>
 *   <li>Handle schema retrieval requests from {@link SchemaEvolutionClient}s
 *   <li>Accept and trace sink writers' registering events
 *   <li>Snapshot & Restore its state during checkpoints
 * </ul>
 *
 * <br>
 * These abilities are done by overriding abstract methods of {@link SchemaRegistry}. All methods
 * will run in given {@link ExecutorService} asynchronously except {@code SchemaRegistry#restore}.
 */
// 一个抽象的中心化元数据注册中心。
// 它作为 OperatorCoordinator 的实现，运行在 JobManager 端，是整个 CDC 管道处理 Schema 演化（Schema Evolution）的“大脑”。
// 核心职责是协调和管理全表结构的变更。其具体作用包括：
// 元数据存储与版本管理：维护源表（Original Schema）和演变后表（Evolved Schema）的历史版本。
// 请求响应中心：处理来自下游 SinkWriter 或 SchemaEvolutionClient 的元数据查询请求。
// DDL 协调执行：通过 MetadataApplier 将最新的 Schema 变更应用到外部目标系统（如数据库、数据湖）。
// 同步对齐（Flush 机制）：在分布式环境下，协调多个上游算子暂停数据流，确保 DDL 执行时数据的强一致性。
// 状态持久化：支持 Flink 的 Checkpoint 机制，确保元数据管理器在作业失败重启后能恢复到一致的状态。


@Internal
public abstract class SchemaRegistry implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistry.class);

    // -------------------------
    // Static fields that got bind after construction
    // -------------------------
    // Flink 提供的协调器上下文，用于获取并行度、上报作业失败或获取类加载器
    protected final OperatorCoordinator.Context context;
    // 算子名称，主要用于日志记录和监控。
    protected final String operatorName;
    // 单线程事件循环执行器。所有的请求处理、DDL 执行都在此线程中顺序执行，保证了线程安全和顺序性。
    protected final ExecutorService coordinatorExecutor;
    // DDL 落地执行器，负责在目标端（如 StarRocks/Doris）执行真实的 SQL。
    protected final MetadataApplier metadataApplier;
    // 远程调用超时时间，防止 DDL 同步过程无限期卡死。
    protected final Duration rpcTimeout;
    // 路由规则，用于将上游源表 TableId 映射为下游目标表 TableId
    protected final List<RouteRule> routingRules;
    // Schema 变更策略（如 EVOLVE, IGNORE, EXCEPTION）
    protected final SchemaChangeBehavior behavior;

    // -------------------------
    // Dynamically initialized transient fields (after coordinator starts)
    // -------------------------
    // 记录当前作业的并行度
    protected transient int currentParallelism;
    // 记录当前已注册并存活的下游 Sink 算子的 Subtask ID 集合
    protected transient Set<Integer> activeSinkWriters;
    // 记录子任务失败的原因，方便排查由于 Schema 不一致导致的崩溃。
    protected transient Map<Integer, Throwable> failedReasons;
    // 核心内存数据库，存储所有表的版本化 Schema 信息
    protected transient SchemaManager schemaManager;
    // 路由处理器，根据 routingRules 执行实际的 TableId 转换
    protected transient TableIdRouter router;

    protected SchemaRegistry(
            OperatorCoordinator.Context context,
            String operatorName,
            ExecutorService coordinatorExecutor,
            MetadataApplier metadataApplier,
            List<RouteRule> routingRules,
            SchemaChangeBehavior schemaChangeBehavior,
            Duration rpcTimeout) {
        this.context = context;
        this.operatorName = operatorName;
        this.coordinatorExecutor = coordinatorExecutor;
        this.metadataApplier = metadataApplier;
        this.routingRules = routingRules;
        this.rpcTimeout = rpcTimeout;
        this.behavior = schemaChangeBehavior;
    }

    // ---------------
    // Lifecycle hooks
    // ---------------
    // start() 方法在其生命周期中仅执行一次，负责初始化运行时的关键数据结构。
    @Override
    public void start() throws Exception {
        LOG.info("Starting SchemaRegistry - {}.", operatorName);
        // SchemaRegistry 需要知道下游有多少个 SinkWriter 子任务。
        // 在分布式 DDL 对齐时，它必须等待所有（例如 10 个）并行子任务都上报“刷新成功（Flush Success）”后，才能认为全局对齐完成。
        this.currentParallelism = context.currentParallelism();
        this.activeSinkWriters = ConcurrentHashMap.newKeySet();
        this.failedReasons = new ConcurrentHashMap<>();
        // 初始化 Schema 管理器（单例/状态恢复判断）
        if (this.schemaManager == null) {
            this.schemaManager = new SchemaManager();
        }
        this.router = new TableIdRouter(routingRules);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing SchemaRegistry - {}.", operatorName);
        coordinatorExecutor.shutdown();
        try {
            metadataApplier.close();
        } catch (Exception e) {
            LOG.error("Failed to close metadata applier.", e);
            throw new IOException("Failed to close metadata applier.", e);
        }
    }

    // ------------------------------
    // Overridable checkpoint methods
    // ------------------------------
    /** Snapshot current schema registry state in byte array form. */
    protected abstract void snapshot(CompletableFuture<byte[]> resultFuture) throws Exception;

    /** Restore schema registry state from byte array. */
    protected abstract void restore(byte[] checkpointData) throws Exception;

    // ------------------------------------
    // Overridable event & request handlers
    // ------------------------------------

    /** Overridable handler for {@link SinkWriterRegisterEvent}s. */
    // 处理下游 Sink 算子注册事件 的回调方法
    // 在 Flink CDC 的协调机制中，这是一个至关重要的步骤，因为它建立了协调器对下游并行任务的感知。
    protected void handleSinkWriterRegisterEvent(SinkWriterRegisterEvent event) throws Exception {
        LOG.info("Sink subtask {} already registered.", event.getSubtask());
        activeSinkWriters.add(event.getSubtask());
    }

    /** Overridable handler for {@link FlushSuccessEvent}s. */
    protected abstract void handleFlushSuccessEvent(FlushSuccessEvent event) throws Exception;

    /** Overridable handler for {@link GetEvolvedSchemaRequest}s. */
    // 处理来自下游（通常是 Sink 端或其 Client）的 Schema 查询请求 的核心逻辑。
    // 其目的是根据请求的版本号，从内存管理器中提取对应的表结构。
    protected void handleGetEvolvedSchemaRequest(
            GetEvolvedSchemaRequest request, CompletableFuture<CoordinationResponse> responseFuture)
            throws Exception {
        LOG.info("Handling evolved schema request: {}", request);
        // 客户端期望的版本号
        int schemaVersion = request.getSchemaVersion();
        // 请求的目标表唯一标识
        TableId tableId = request.getTableId();
        // 当客户端请求 -1（即 LATEST_SCHEMA_VERSION 常量）时，返回该表的最新结构。
        if (schemaVersion == GetEvolvedSchemaRequest.LATEST_SCHEMA_VERSION) {
            responseFuture.complete(
                    wrap(
                            new GetEvolvedSchemaResponse(
                                    schemaManager.getLatestEvolvedSchema(tableId).orElse(null))));
        } else {
            try {
                responseFuture.complete(
                        wrap(
                                new GetEvolvedSchemaResponse(
                                        schemaManager.getEvolvedSchema(tableId, schemaVersion))));
            } catch (IllegalArgumentException iae) {
                LOG.warn(
                        "Some client is requesting an non-existed evolved schema for table {} with version {}",
                        tableId,
                        schemaVersion);
                responseFuture.complete(wrap(new GetEvolvedSchemaResponse(null)));
            }
        }
    }

    /** Overridable handler for {@link GetOriginalSchemaRequest}s. */
    // 处理获取**原始表结构（Original Schema）**请求的逻辑。
    // 它与之前看到的 handleGetEvolvedSchemaRequest 非常相似，但操作的对象是源头端（Upstream/Source）的元数据。
    protected void handleGetOriginalSchemaRequest(
            GetOriginalSchemaRequest request,
            CompletableFuture<CoordinationResponse> responseFuture)
            throws Exception {
        LOG.info("Handling original schema request: {}", request);
        int schemaVersion = request.getSchemaVersion();
        TableId tableId = request.getTableId();
        if (schemaVersion == GetOriginalSchemaRequest.LATEST_SCHEMA_VERSION) {
            responseFuture.complete(
                    wrap(
                            new GetOriginalSchemaResponse(
                                    schemaManager.getLatestOriginalSchema(tableId).orElse(null))));
        } else {
            try {
                responseFuture.complete(
                        wrap(
                                new GetOriginalSchemaResponse(
                                        schemaManager.getOriginalSchema(tableId, schemaVersion))));
            } catch (IllegalArgumentException iae) {
                LOG.warn(
                        "Some client is requesting an non-existed original schema for table {} with version {}",
                        tableId,
                        schemaVersion);
                responseFuture.complete(wrap(new GetOriginalSchemaResponse(null)));
            }
        }
    }

    /** Coordination handler for customized {@link CoordinationRequest}s. */
    protected abstract void handleCustomCoordinationRequest(
            CoordinationRequest request, CompletableFuture<CoordinationResponse> responseFuture)
            throws Exception;

    /** Last chance to execute codes before job fails globally. */
    protected void handleUnrecoverableError(String taskDescription, Throwable t) {
        LOG.error(
                "Uncaught exception in the Schema Registry ({}) event loop for {}.",
                operatorName,
                taskDescription,
                t);
        LOG.error("\tCurrent schema manager state: {}", schemaManager);
    }

    // ---------------------------------
    // Event & Request Dispatching Stuff
    // ---------------------------------
    // 它实现了 Flink 框架的 CoordinationRequestHandler 接口，负责将来自算子端的各种查询请求路由到具体的处理逻辑中。
    @Override
    public final CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
        runInEventLoop(
                () -> {
                    if (request instanceof GetEvolvedSchemaRequest) {
                        handleGetEvolvedSchemaRequest((GetEvolvedSchemaRequest) request, future);
                    } else if (request instanceof GetOriginalSchemaRequest) {
                        handleGetOriginalSchemaRequest((GetOriginalSchemaRequest) request, future);
                    } else {
                        handleCustomCoordinationRequest(request, future);
                    }
                },
                "Handling request - %s",
                request);
        return future;
    }
    // 处理**算子事件（Operator Event）**的统一入口。与之前的 handleCoordinationRequest（处理“请求/响应”模式）不同，
    // 这个方法专门处理从 TaskManager 端的算子（如 Source 或 Sink）主动推送到 JobManager 端的单向通知
    // 接收来自特定子任务（Subtask）的事件，并确保这些事件在单线程事件循环中处理，以维护元数据的一致性。
    // int subTaskId: 发送该事件的算子子任务索引（从 0 开始）。这让协调器知道是哪一个并行实例发来的消息。
    // int attemptNumber: 该子任务的执行尝试次数。用于处理任务失败重试时的陈旧事件过滤。
    // OperatorEvent event: 具体的事件对象，目前主要处理 FlushSuccessEvent 和 SinkWriterRegisterEvent。
    @Override
    public final void handleEventFromOperator(
            int subTaskId, int attemptNumber, OperatorEvent event) {
        runInEventLoop(
                () -> {
                    if (event instanceof FlushSuccessEvent) {
                        handleFlushSuccessEvent((FlushSuccessEvent) event);
                    } else if (event instanceof SinkWriterRegisterEvent) {
                        handleSinkWriterRegisterEvent((SinkWriterRegisterEvent) event);
                    } else {
                        throw new FlinkRuntimeException("Unrecognized Operator Event: " + event);
                    }
                },
                "Handling event - %s (from subTask %d)",
                event,
                subTaskId);
    }

    // --------------------------
    // Gateway registration stuff
    // --------------------------

    @Override
    public final void subtaskReset(int subTaskId, long checkpointId) {
        Throwable rootCause = failedReasons.get(subTaskId);
        LOG.error("Subtask {} reset at checkpoint {}.", subTaskId, checkpointId, rootCause);
    }

    @Override
    public final void executionAttemptFailed(
            int subTaskId, int attemptNumber, @Nullable Throwable reason) {
        if (reason != null) {
            failedReasons.put(subTaskId, reason);
        }
    }

    @Override
    public final void executionAttemptReady(
            int subTaskId, int attemptNumber, SubtaskGateway gateway) {
        // Needless to do anything. SchemaRegistry does not post message to the coordinator
        // spontaneously.
    }

    // ---------------------------
    // Checkpointing related stuff
    // ---------------------------
    @Override
    public final void checkpointCoordinator(
            long checkpointId, CompletableFuture<byte[]> completableFuture) throws Exception {
        LOG.info("Going to start checkpoint No.{}", checkpointId);
        runInEventLoop(() -> snapshot(completableFuture), "Taking checkpoint - %d", checkpointId);
    }

    @Override
    public final void notifyCheckpointComplete(long checkpointId) {
        LOG.info("Successfully completed checkpoint No.{}", checkpointId);
    }

    @Override
    public final void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        LOG.info("Going to restore from checkpoint No.{}", checkpointId);
        if (checkpointData == null) {
            return;
        }
        restore(checkpointData);
    }

    // ---------------------------
    // Utility functions
    // ---------------------------
    /**
     * Run a time-consuming task in given {@link ExecutorService}. All overridable functions have
     * been wrapped inside already, so there's no need to call this method again. However, if you're
     * overriding methods from {@link OperatorCoordinator} or {@link CoordinationRequestHandler}
     * directly, make sure you're running heavy logics inside, or the entire job might hang!
     */
    protected void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {
        coordinatorExecutor.execute(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        // if we have a JVM critical error, promote it immediately, there is a good
                        // chance the logging or job failing will not succeed anymore
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                        handleUnrecoverableError(
                                String.format(actionName, actionNameFormatParameters), t);
                        context.failJob(t);
                    }
                });
    }

    /**
     * Keeps checking if {@code conditionChecker} is satisfied. If not, emit a message and retry.
     */
    protected void loopUntil(
            BooleanSupplier conditionChecker, Runnable message, Duration timeout, Duration interval)
            throws TimeoutException {
        loopWhen(() -> !conditionChecker.getAsBoolean(), message, timeout, interval);
    }

    /**
     * Keeps checking if {@code conditionChecker} is satisfied. Otherwise, emit a message and retry.
     */
    protected void loopWhen(
            BooleanSupplier conditionChecker, Runnable message, Duration timeout, Duration interval)
            throws TimeoutException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        long intervalMs = interval.toMillis();
        while (conditionChecker.getAsBoolean()) {
            message.run();
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutException("Loop checking time limit has exceeded.");
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    protected <T extends Throwable> void failJob(String taskDescription, T t) {
        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
        LOG.error("An exception was triggered from {}. Job will fail now.", taskDescription, t);
        handleUnrecoverableError(taskDescription, t);
        context.failJob(t);
    }

    // ------------------------
    // Visible just for testing
    // ------------------------

    @VisibleForTesting
    public void emplaceOriginalSchema(TableId tableId, Schema schema) {
        schemaManager.registerNewOriginalSchema(tableId, schema);
    }

    @VisibleForTesting
    public void emplaceEvolvedSchema(TableId tableId, Schema schema) {
        schemaManager.registerNewEvolvedSchema(tableId, schema);
    }
}
