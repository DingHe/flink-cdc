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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.common.CoordinatorExecutorThreadFactory;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Provider of {@link SchemaCoordinator}. */
// 主要作用是 作为 SchemaCoordinator 的序列化描述符和启动器。
// 在 Flink 架构中，算子代码会被发送到 TaskManager 执行，而 Coordinator（协调器） 必须留在 JobManager 端运行。由于 JobManager 需要知道如何创建这个协调器，Provider 充当了中间人的角色：
// 解耦创建逻辑：它包含了创建协调器所需的所有配置参数。
// 生命周期挂钩：Flink 的 JobMaster 会调用它的 create 方法，从而在主节点启动负责处理 Schema 变更的核心大脑。
@Internal
public class SchemaCoordinatorProvider implements OperatorCoordinator.Provider {
    private static final long serialVersionUID = 1L;
    // 唯一标识该协调器所属的算子
    // Flink 依靠这个 ID 将 TaskManager 端算子上报的事件精确路由到 JobManager 端的对应协调器。
    private final OperatorID operatorID;
    // 算子的名称（通常为 "SchemaOperator"）。主要用于日志记录和线程命名。
    private final String operatorName;
    // 元数据应用器。这是协调器的“执行手”，负责真正去下游数据库（如 Sink 端）执行 DDL 语句。
    private final MetadataApplier metadataApplier;
    // 路由规则。协调器在处理 Schema 变更时，需要根据这些规则判断源表的变更应该如何映射到目标表
    private final List<RouteRule> routingRules;
    // Schema 变更策略。告知协调器在遇到 DDL 时是执行演进（EVOLVE）、忽略（IGNORE）还是抛异常。
    private final SchemaChangeBehavior schemaChangeBehavior;
    // 通信超时时长。定义协调器在等待所有 Subtask 反馈 DDL 确认信息时的最大耐心值。
    private final Duration rpcTimeout;

    public SchemaCoordinatorProvider(
            OperatorID operatorID,
            String operatorName,
            MetadataApplier metadataApplier,
            List<RouteRule> routingRules,
            SchemaChangeBehavior schemaChangeBehavior,
            Duration rpcTimeout) {
        this.operatorID = operatorID;
        this.operatorName = operatorName;
        this.metadataApplier = metadataApplier;
        this.routingRules = routingRules;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.rpcTimeout = rpcTimeout;
    }

    @Override
    public OperatorID getOperatorId() {
        return operatorID;
    }

    // 该类最核心的方法
    // 由 Flink 框架在 JobManager 端启动作业时调用
    @Override
    public OperatorCoordinator create(OperatorCoordinator.Context context) throws Exception {
        CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory(
                        "schema-evolution-coordinator", context.getUserCodeClassloader());
        // Schema 变更必须是串行的、顺序执行的。
        // 使用单线程执行器可以天然地避免多线程竞争导致的 DDL 执行乱序。
        ExecutorService coordinatorExecutor =
                Executors.newSingleThreadExecutor(coordinatorThreadFactory);
        return new SchemaCoordinator(
                operatorName,
                context,
                coordinatorExecutor,
                metadataApplier,
                routingRules,
                schemaChangeBehavior,
                rpcTimeout);
    }
}
