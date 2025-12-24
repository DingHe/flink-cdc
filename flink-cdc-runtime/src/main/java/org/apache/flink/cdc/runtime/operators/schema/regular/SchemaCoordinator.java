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

package org.apache.flink.cdc.runtime.operators.schema.regular;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaDerivator;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaManager;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaRegistry;
import org.apache.flink.cdc.runtime.operators.schema.common.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils.wrap;

/** Coordinator node for {@link SchemaOperator}. Registry actor in regular topology. */
// SchemaCoordinator 是 DDL（数据定义语言）变更的核心大脑。
// 它运行在 Flink 的 JobManager 端，作为 OperatorCoordinator 的实现，负责全局协调所有并行算子的 Schema 状态。
// SchemaCoordinator 的主要职责是实现 “分布式 DDL 一致性协议”。其核心作用包括：
// 全局屏障对齐：当上游发生 DDL 变更时，确保所有并行下游 Sink 已经处理完旧 Schema 下的所有数据（通过 FlushSuccess 事件对齐）。
// Schema 推导（Evolution）：根据路由规则（Route Rules），将上游源表的变更推导为下游目标表的变更。例如，将 MySQL 两个分表的 加列 操作合并为目标端一个大表的 加列 操作。
// 外部系统同步：调用 MetadataApplier 将变更应用到外部系统（如 Doris、StarRocks、Kafka 等）
// 状态持有与恢复：管理并持久化 SchemaManager 的状态，确保作业重启后元数据不丢失。
public class SchemaCoordinator extends SchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaCoordinator.class);

    /** Executor service to execute schema change. */
    // 单线程执行池。用于异步执行具体的 Schema 变更逻辑，避免阻塞协调器的主线程。
    private final ExecutorService schemaChangeThreadPool;

    /**
     * Sink writers which have sent flush success events for the request.<br>
     * {@code MapEntry.Key} is an {@code Integer}, indicating the upstream subTaskId that initiates
     * this schema change request.<br>
     * {@code MapEntry.Value} is a {@code Set<Integer>}, containing downstream subTasks' ID that
     * have acknowledged and successfully flushed pending events for this schema change event.
     */
    // 记录已完成数据刷新的 Sink 列表。
    // Key 是发起 DDL 的上游 Source 子任务 ID，Value 是已经上报成功刷新的下游 Sink 子任务 ID 集合
    private transient ConcurrentHashMap<Integer, Set<Integer>> flushedSinkWriters;

    /**
     * Schema evolution requests that we're currently handling.<br>
     * For each subTask executing in parallelism, they may initiate requests simultaneously, so we
     * use each task's unique subTaskId as Map key to distinguish each of them.
     */
    // 暂存正在处理中的 Schema 变更请求。在所有 Sink 完成对齐前，请求会被“挂起”在这里。
    private transient Map<
                    Integer, Tuple2<SchemaChangeRequest, CompletableFuture<CoordinationResponse>>>
            pendingRequests;

    // Static fields
    public SchemaCoordinator(
            String operatorName,
            OperatorCoordinator.Context context,
            ExecutorService coordinatorExecutor,
            MetadataApplier metadataApplier,
            List<RouteRule> routes,
            SchemaChangeBehavior schemaChangeBehavior,
            Duration rpcTimeout) {
        super(
                context,
                operatorName,
                coordinatorExecutor,
                metadataApplier,
                routes,
                schemaChangeBehavior,
                rpcTimeout);
        this.schemaChangeThreadPool = Executors.newSingleThreadExecutor();
    }

    @Override
    public void start() throws Exception {
        super.start();
        this.flushedSinkWriters = new ConcurrentHashMap<>();
        this.pendingRequests = new ConcurrentHashMap<>();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (schemaChangeThreadPool != null && !schemaChangeThreadPool.isShutdown()) {
            schemaChangeThreadPool.shutdownNow();
        }
    }

    @Override
    protected void snapshot(CompletableFuture<byte[]> resultFuture) throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            // Serialize SchemaManager
            int schemaManagerSerializerVersion = SchemaManager.SERIALIZER.getVersion();
            out.writeInt(schemaManagerSerializerVersion);
            byte[] serializedSchemaManager;
            serializedSchemaManager = SchemaManager.SERIALIZER.serialize(schemaManager);
            out.writeInt(serializedSchemaManager.length);
            out.write(serializedSchemaManager);

            // Length-bit for SchemaDerivation, which is no longer necessary.
            out.writeInt(0);
            resultFuture.complete(baos.toByteArray());
        }
    }

    @Override
    protected void restore(byte[] checkpointData) throws Exception {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
                DataInputStream in = new DataInputStream(bais)) {
            int schemaManagerSerializerVersion = in.readInt();

            switch (schemaManagerSerializerVersion) {
                case 0:
                    {
                        int length = in.readInt();
                        byte[] serializedSchemaManager = new byte[length];
                        in.readFully(serializedSchemaManager);
                        schemaManager =
                                SchemaManager.SERIALIZER.deserialize(
                                        schemaManagerSerializerVersion, serializedSchemaManager);
                        break;
                    }
                case 1:
                case 2:
                    {
                        int length = in.readInt();
                        byte[] serializedSchemaManager = new byte[length];
                        in.readFully(serializedSchemaManager);
                        schemaManager =
                                SchemaManager.SERIALIZER.deserialize(
                                        schemaManagerSerializerVersion, serializedSchemaManager);
                        consumeUnusedSchemaDerivationBytes(in);
                        break;
                    }
                default:
                    throw new IOException(
                            "Unrecognized serialization version " + schemaManagerSerializerVersion);
            }
        }
    }

    @Override
    protected void handleCustomCoordinationRequest(
            CoordinationRequest request, CompletableFuture<CoordinationResponse> responseFuture) {
        if (request instanceof SchemaChangeRequest) {
            handleSchemaChangeRequest((SchemaChangeRequest) request, responseFuture);
        } else {
            throw new UnsupportedOperationException(
                    "Unknown coordination request type: " + request);
        }
    }

    @Override
    protected void handleFlushSuccessEvent(FlushSuccessEvent event) throws TimeoutException {
        int sinkSubtask = event.getSinkSubTaskId();
        int sourceSubtask = event.getSourceSubTaskId();
        LOG.info(
                "Sink subtask {} succeed flushing from source subTask {}.",
                sinkSubtask,
                sourceSubtask);
        if (!flushedSinkWriters.containsKey(sourceSubtask)) {
            flushedSinkWriters.put(sourceSubtask, ConcurrentHashMap.newKeySet());
        }
        flushedSinkWriters.get(sourceSubtask).add(sinkSubtask);
        LOG.info(
                "Currently flushed sink writers for source task {} are: {}",
                sourceSubtask,
                flushedSinkWriters.get(sourceSubtask));

        if (flushedSinkWriters.get(sourceSubtask).size() >= currentParallelism) {
            LOG.info(
                    "Source SubTask {} have collected enough flush success event. Will start evolving schema changes...",
                    sourceSubtask);
            flushedSinkWriters.remove(sourceSubtask);
            startSchemaChangesEvolve(sourceSubtask);
        }
    }

    @Override
    protected void handleUnrecoverableError(String taskDescription, Throwable t) {
        super.handleUnrecoverableError(taskDescription, t);

        // For each pending future, release it exceptionally before quitting
        pendingRequests.forEach(
                (index, tuple) -> {
                    tuple.f1.completeExceptionally(t);
                });
    }

    /**
     * Handle the {@link SchemaChangeRequest} and wait for all sink subtasks flushing.
     *
     * @param request the received SchemaChangeRequest
     */
    // 在 Flink CDC 的常规拓扑中，上游的 SchemaOperator 发现物理表结构变化（DDL）时，会向 JobManager 端的 SchemaCoordinator 发送这个请求。
    // 由于 DDL 的执行必须在所有下游 Sink 算子都把“旧 Schema 下的数据”处理干净（Flush）之后才能进行，因此协调器不能立即处理这个请求。这行代码的作用就是将请求“锁”在内存中，直到对齐条件达成。
    // request: 包含了 DDL 的具体内容（例如 ADD COLUMN）、发起请求的源端子任务 ID（Subtask ID）以及受影响的表 ID。
    // responseFuture: 这是一个 异步响应句柄。协调器通过它保持与上游算子的连接。在上游看来，它正在“阻塞”等待 DDL 执行结果。
    public void handleSchemaChangeRequest(
            SchemaChangeRequest request, CompletableFuture<CoordinationResponse> responseFuture) {
        pendingRequests.put(request.getSubTaskId(), Tuple2.of(request, responseFuture));
    }

    private void startSchemaChangesEvolve(int sourceSubTaskId) {
        schemaChangeThreadPool.submit(
                () -> {
                    try {
                        applySchemaChange(sourceSubTaskId);
                    } catch (Throwable t) {
                        failJob(
                                "Schema change applying task",
                                new FlinkRuntimeException(
                                        "Failed to apply schema change event.", t));
                        throw t;
                    }
                });
    }

    private List<SchemaChangeEvent> deduceEvolvedSchemaChanges(SchemaChangeEvent event) {
        LOG.info("Step 1 - Start deducing evolved schema change for {}", event);

        TableId originalTableId = event.tableId();
        List<SchemaChangeEvent> deducedSchemaChangeEvents = new ArrayList<>();
        Set<TableId> originalTables = schemaManager.getAllOriginalTables();

        // First, grab all affected evolved tables.
        Set<TableId> affectedEvolvedTables =
                SchemaDerivator.getAffectedEvolvedTables(
                        router, Collections.singleton(originalTableId));
        LOG.info("Step 2 - Affected downstream tables are: {}", affectedEvolvedTables);

        // For each affected table, we need to...
        for (TableId evolvedTableId : affectedEvolvedTables) {
            Schema currentEvolvedSchema =
                    schemaManager.getLatestEvolvedSchema(evolvedTableId).orElse(null);
            LOG.info(
                    "Step 3.1 - For to-be-evolved table {} with schema {}...",
                    evolvedTableId,
                    currentEvolvedSchema);

            // ... reversely look up this affected sink table's upstream dependency
            Set<TableId> upstreamDependencies =
                    SchemaDerivator.reverseLookupDependingUpstreamTables(
                            router, evolvedTableId, originalTables);
            Preconditions.checkArgument(
                    !upstreamDependencies.isEmpty(),
                    "An affected sink table's upstream dependency cannot be empty.");
            LOG.info("Step 3.2 - upstream dependency tables are: {}", upstreamDependencies);

            List<SchemaChangeEvent> rawSchemaChangeEvents = new ArrayList<>();
            if (upstreamDependencies.size() == 1) {
                // If it's a one-by-one routing rule, we can simply forward it to downstream sink.
                SchemaChangeEvent rawEvent = event.copy(evolvedTableId);
                rawSchemaChangeEvents.add(rawEvent);
                LOG.info(
                        "Step 3.3 - It's an one-by-one routing and could be forwarded as {}.",
                        rawEvent);
            } else {
                Set<Schema> toBeMergedSchemas =
                        SchemaDerivator.reverseLookupDependingUpstreamSchemas(
                                router, evolvedTableId, schemaManager);
                LOG.info("Step 3.3 - Upstream dependency schemas are: {}.", toBeMergedSchemas);

                // We're in a table routing mode now, so we need to infer a widest schema for all
                // upstream tables.
                Schema mergedSchema = currentEvolvedSchema;
                for (Schema toBeMergedSchema : toBeMergedSchemas) {
                    mergedSchema =
                            SchemaMergingUtils.getLeastCommonSchema(mergedSchema, toBeMergedSchema);
                }
                LOG.info("Step 3.4 - Deduced widest schema is: {}.", mergedSchema);

                // Detect what schema changes we need to apply to get expected sink table.
                List<SchemaChangeEvent> rawEvents =
                        SchemaMergingUtils.getSchemaDifference(
                                evolvedTableId, currentEvolvedSchema, mergedSchema);
                LOG.info(
                        "Step 3.5 - It's an many-to-one routing and causes schema changes: {}.",
                        rawEvents);

                rawSchemaChangeEvents.addAll(rawEvents);
            }

            // Finally, we normalize schema change events, including rewriting events by current
            // schema change behavior configuration, dropping explicitly excluded schema change
            // event types.
            List<SchemaChangeEvent> normalizedEvents =
                    SchemaDerivator.normalizeSchemaChangeEvents(
                            currentEvolvedSchema, rawSchemaChangeEvents, behavior, metadataApplier);
            LOG.info(
                    "Step 4 - After being normalized with {} behavior, final schema change events are: {}",
                    behavior,
                    normalizedEvents);

            deducedSchemaChangeEvents.addAll(normalizedEvents);
        }

        return deducedSchemaChangeEvents;
    }

    /** Applies the schema change to the external system. */
    // applySchemaChange 是执行 DDL 变更的终点站。
    // 它负责将上游捕获的变更“落地”到外部系统，并同步更新内部的元数据状态。
    private void applySchemaChange(int sourceSubTaskId) {
        try {
            // 循环检查 pendingRequests 映射中是否已经包含了来自该 sourceSubTaskId 的 DDL 请求。
            // applySchemaChange 是在所有 Sink 刷新成功后由另一个线程触发的。这里是为了防止“信号先到，请求后到”的极端并发情况，确保 RPC 请求体已经安全存入 Map。
            loopUntil(
                    () -> pendingRequests.containsKey(sourceSubTaskId),
                    () ->
                            LOG.info(
                                    "SchemaOperator {} has not submitted schema change request yet. Waiting...",
                                    sourceSubTaskId),
                    rpcTimeout,
                    Duration.ofMillis(100));
        } catch (TimeoutException e) {
            throw new RuntimeException(
                    "Timeout waiting for schema change request from SchemaOperator.", e);
        }
        // 从挂起的请求队列中取出 DDL 请求对象 (request) 和对应的异步响应句柄 (responseFuture)
        Tuple2<SchemaChangeRequest, CompletableFuture<CoordinationResponse>> requestBody =
                pendingRequests.get(sourceSubTaskId);
        SchemaChangeRequest request = requestBody.f0;
        CompletableFuture<CoordinationResponse> responseFuture = requestBody.f1;

        SchemaChangeEvent originalEvent = request.getSchemaChangeEvent();
        // 提取 DDL 事件（如 AddColumnEvent）及其对应的物理表 ID，并从 SchemaManager 中查找该表在变更之前的物理结构。
        TableId originalTableId = originalEvent.tableId();
        Schema currentUpstreamSchema =
                schemaManager.getLatestOriginalSchema(originalTableId).orElse(null);

        List<SchemaChangeEvent> deducedSchemaChangeEvents = new ArrayList<>();

        // For redundant schema change events (possibly coming from duplicate emitted
        // CreateTableEvents in snapshot stage), we just skip them.
        // 冗余检查：判断该变更是否已经应用过（常见于快照阶段重复发送的 CreateTableEvent
        if (!SchemaUtils.isSchemaChangeEventRedundant(currentUpstreamSchema, originalEvent)) {
            // 应用物理变更：如果是非冗余变更，首先更新 SchemaManager 中记录的原始表结构。
            schemaManager.applyOriginalSchemaChange(originalEvent);
            // 计算出这个物理变更对应到下游逻辑表上应该是什么样（处理分表合并、路由映射等逻辑）
            deducedSchemaChangeEvents.addAll(deduceEvolvedSchemaChanges(originalEvent));
        } else {
            LOG.info(
                    "Schema change event {} is redundant for current schema {}, just skip it.",
                    originalEvent,
                    currentUpstreamSchema);
        }

        LOG.info(
                "All sink subtask have flushed for table {}. Start to apply schema change request: \n\t{}\nthat extracts to:\n\t{}",
                request.getTableId().toString(),
                request,
                deducedSchemaChangeEvents.stream()
                        .map(SchemaChangeEvent::toString)
                        .collect(Collectors.joining("\n\t")));
        // 如果用户配置了 EXCEPTION 策略，意味着该作业不允许任何非建表类的 DDL 变更。一旦检测到增删改列，直接报错停止作业
        if (SchemaChangeBehavior.EXCEPTION.equals(behavior)) {
            if (deducedSchemaChangeEvents.stream()
                    .anyMatch(evt -> !(evt instanceof CreateTableEvent))) {
                SchemaChangeEvent unacceptableSchemaChangeEvent =
                        deducedSchemaChangeEvents.stream()
                                .filter(evt -> !(evt instanceof CreateTableEvent))
                                .findAny()
                                .get();
                throw new SchemaEvolveException(
                        unacceptableSchemaChangeEvent,
                        "Unexpected schema change events occurred in EXCEPTION mode. Job will fail now.");
            }
        }

        // Tries to apply it to external system
        // 遍历推导出的所有变更事件，调用 applyAndUpdateEvolvedSchemaChange
        // 真正调用外部系统的 Client（如 JDBC）去执行 ALTER TABLE 语句，成功后再更新内部的“逻辑表（Evolved Schema）”元数据
        List<SchemaChangeEvent> appliedSchemaChangeEvents = new ArrayList<>();
        for (SchemaChangeEvent event : deducedSchemaChangeEvents) {
            if (applyAndUpdateEvolvedSchemaChange(event)) {
                appliedSchemaChangeEvents.add(event);
            }
        }

        Map<TableId, Schema> refreshedEvolvedSchemas = new HashMap<>();

        // We need to retrieve all possibly modified evolved schemas and refresh SchemaOperator's
        // local cache since it might have been altered by another SchemaOperator instance.
        // SchemaChangeEvents doesn't need to be emitted to downstream (since it might be broadcast
        // from other SchemaOperators) though.
        // 找到该物理表映射到的所有逻辑目标表，获取它们更新后的最新 Schema。
        for (TableId tableId : router.route(originalEvent.tableId())) {
            refreshedEvolvedSchemas.put(
                    tableId, schemaManager.getLatestEvolvedSchema(tableId).orElse(null));
        }

        pendingRequests.remove(sourceSubTaskId);

        LOG.info(
                "Finished handling schema change request from {}. Pending requests: {}",
                sourceSubTaskId,
                pendingRequests);

        // We release the response future at last to avoid leaking internal states to SchemaOperator
        // client accidentally.
        // 从挂起队列中移除该请求，释放内存。
        responseFuture.complete(
                wrap(new SchemaChangeResponse(appliedSchemaChangeEvents, refreshedEvolvedSchemas)));
    }
    // 核心职责是将推导出的 Schema 变更事件同时应用到“外部系统”和“内部管理中心”。
    // 核心作用：双写一致性
    // 该方法确保了 外部数据库的物理结构 与 Flink CDC 内部记录的逻辑结构 保持同步。
    // 如果外部修改失败，内部状态也不会更新，从而保证了两端的一致性。
    private boolean applyAndUpdateEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        try {
            // 这是真正发起网络请求的操作。例如，如果你使用的是 StarRocks 终端，这里会通过 JDBC 执行 ALTER TABLE ... ADD COLUMN ...。
            // MetadataApplier 是一个插件化接口，不同的 Sink 端（Doris, StarRocks, Kafka 等）有不同的实现。
            metadataApplier.applySchemaChange(schemaChangeEvent);
            // 一旦外部系统执行成功，内部立即更新该表的 Schema 版本。这样后续的数据流入时，协调器就能识别出这些数据是属于“新版本”结构的。
            schemaManager.applyEvolvedSchemaChange(schemaChangeEvent);
            LOG.info(
                    "Successfully applied schema change event {} to external system.",
                    schemaChangeEvent);
            return true;
        } catch (Throwable t) {
            if (shouldIgnoreException(t)) {
                LOG.warn(
                        "Failed to apply schema change {}, but keeps running in tolerant mode. Caused by: {}",
                        schemaChangeEvent,
                        t);
                return false;
            } else {
                throw t;
            }
        }
    }

    // -------------------------
    // Utilities
    // -------------------------

    private boolean shouldIgnoreException(Throwable throwable) {
        // In IGNORE mode, will never try to apply schema change events
        // In EVOLVE and LENIENT mode, such failure will not be tolerated
        // In EXCEPTION mode, an exception will be thrown once captured
        return (throwable instanceof UnsupportedSchemaChangeEventException)
                && (SchemaChangeBehavior.TRY_EVOLVE.equals(behavior));
    }

    /**
     * Before Flink CDC 3.3, we store routing rules into {@link SchemaCoordinator}'s state, which
     * turns out to be unnecessary since data stream topology might change after stateful restarts,
     * and stale routing status is both unnecessary and erroneous. This function consumes these
     * bytes from the state, but never returns them.
     */
    private void consumeUnusedSchemaDerivationBytes(DataInputStream in) throws IOException {
        TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
        int derivationMappingSize = in.readInt();
        Map<TableId, Set<TableId>> derivationMapping = new HashMap<>(derivationMappingSize);
        for (int i = 0; i < derivationMappingSize; i++) {
            // Routed table ID
            TableId routedTableId =
                    tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
            // Original table IDs
            int numOriginalTables = in.readInt();
            Set<TableId> originalTableIds = new HashSet<>(numOriginalTables);
            for (int j = 0; j < numOriginalTables; j++) {
                TableId originalTableId =
                        tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
                originalTableIds.add(originalTableId);
            }
            derivationMapping.put(routedTableId, originalTableIds);
        }
    }
}
