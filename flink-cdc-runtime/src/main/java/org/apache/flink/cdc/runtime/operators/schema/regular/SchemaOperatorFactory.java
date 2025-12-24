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
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;

import java.time.Duration;
import java.util.List;

/** Factory to create {@link SchemaOperator}. */
// SchemaOperatorFactory 的主要作用是实现 “协调算子工厂” (CoordinatedOperatorFactory) 模式。
// 在 Flink 中，普通的算子（如 Map、Filter）只负责处理其所在的 Subtask 数据。但 Schema 变更（DDL） 是一个全局行为，需要所有并行子任务协同一致：
// 同步对齐：当上游发生 DDL 时，所有并行的 SchemaOperator 必须停下手中的工作（暂停数据处理）。
// 中心化处理：由一个中心节点（Coordinator）负责去目标数据库执行 DDL 语句。
// 继续处理：DDL 执行完成后，通知所有子任务恢复工作。
// 该工厂类就是负责同时创建这个 “分布式的算子” 和 “中心化的协调器” 的容器。

@Internal
public class SchemaOperatorFactory extends SimpleOperatorFactory<Event>
        implements CoordinatedOperatorFactory<Event>, OneInputStreamOperatorFactory<Event, Event> {

    private static final long serialVersionUID = 1L;
    // 元数据应用器。
    // 它包含了将 Schema 变更应用到外部系统（如 MySQL, StarRocks, Kafka 等）的物理逻辑。
    // 它会被传递给 Coordinator，因为真正执行 DDL 的是中心化的协调器。
    private final MetadataApplier metadataApplier;
    // 路由规则列表。定义了源表到目标表的映射关系。
    private final List<RouteRule> routingRules;
    // Schema 变更行为（EVOLVE/IGNORE/EXCEPTION）。
    // 决定了系统在遇到 DDL 时该采取何种策略。
    private final SchemaChangeBehavior schemaChangeBehavior;
    // RPC 通信超时时间。
    // 算子与协调器之间进行 DDL 同步对齐时的等待时间上限。
    private final Duration rpcTimeout;

    public SchemaOperatorFactory(
            MetadataApplier metadataApplier,
            List<RouteRule> routingRules,
            Duration rpcTimeout,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        super(new SchemaOperator(routingRules, rpcTimeout, schemaChangeBehavior, timezone));
        this.metadataApplier = metadataApplier;
        this.routingRules = routingRules;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.rpcTimeout = rpcTimeout;
    }
    // 这是 CoordinatedOperatorFactory 接口的核心实现。它定义了如何为该算子创建一个“指挥官”（Coordinator）
    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new SchemaCoordinatorProvider(
                operatorID,
                operatorName,
                metadataApplier,
                routingRules,
                schemaChangeBehavior,
                rpcTimeout);
    }
}
