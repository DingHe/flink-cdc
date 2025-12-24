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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.runtime.operators.schema.regular.BatchSchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperatorFactory;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Translator used to build {@link SchemaOperator} for schema event process. */
// 负责将表结构演化逻辑（Schema Evolution）和路由逻辑（Routing）翻译为 Flink 物理算子的核心组件。
// 主要职责是根据用户配置的管道定义（Pipeline Definition），在 Flink 作业图中注入负责处理 Schema 变更的算子（SchemaOperator）
// 它解决了两个核心问题：
// DDL 传导：当上游数据库发生 ALTER TABLE 等 DDL 操作时，决定如何将这些变更应用到下游 Sink。
// 表名重映射（路由）：根据路由规则（Route Rules），将多个源表汇聚到同一个目标表，或对表名进行转换。
@Internal
public class SchemaOperatorTranslator {
    // 定义 Schema 变更的行为策略（如：EVOLVE 应用变更、IGNORE 忽略、EXCEPTION 抛出异常）
    private final SchemaChangeBehavior schemaChangeBehavior;
    // 为 Schema 算子分配的唯一标识符（UID）。
    // 这对于 Flink 作业从 Checkpoint/Savepoint 恢复至关重要。
    private final String schemaOperatorUid;
    // 算子与远程 Coordinator（协调器）通信时的超时时间，用于 DDL 同步
    private final Duration rpcTimeOut;
    // 指定本地时区，用于处理与时间相关的 Schema 转换逻辑
    private final String timezone;

    public SchemaOperatorTranslator(
            SchemaChangeBehavior schemaChangeBehavior,
            String schemaOperatorUid,
            Duration rpcTimeOut,
            String timezone) {
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.schemaOperatorUid = schemaOperatorUid;
        this.rpcTimeOut = rpcTimeOut;
        this.timezone = timezone;
    }
    // 处理常规拓扑（非分布式数据源）下的翻译。
    // 它会根据 isBatchMode 标志位自动分发给批处理算子或流处理算子。
    public DataStream<Event> translateRegular(
            DataStream<Event> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes) {
        return translateRegular(input, parallelism, false, metadataApplier, routes);
    }
    // 主要职责是根据当前的运行模式（流或批），决定如何将 Schema 变更处理逻辑（DDL 映射、路由等）挂载到 Flink 的数据流图中。
    public DataStream<Event> translateRegular(
            DataStream<Event> input, // 输入的数据流，包含数据变更和结构变更事件
            int parallelism, // 算子的并行度设置
            boolean isBatchMode, // 是否为批处理模式标志位
            MetadataApplier metadataApplier, // 元数据应用器，负责在目标端执行 DDL
            List<RouteDef> routes) { // 用户定义的路由规则列表（如分表合并规则）

        return isBatchMode
                ? addRegularSchemaBatchOperator(
                        input, parallelism, metadataApplier, routes, timezone)
                : addRegularSchemaOperator(
                        input,
                        parallelism,
                        metadataApplier,
                        routes,
                        schemaChangeBehavior,
                        timezone);
    }

    public DataStream<Event> translateDistributed(
            DataStream<PartitioningEvent> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes) {
        return addDistributedSchemaOperator(
                input, parallelism, metadataApplier, routes, schemaChangeBehavior, timezone);
    }

    public String getSchemaOperatorUid() {
        return schemaOperatorUid;
    }

    // 主要任务是将逻辑层面的路由定义转换为物理层面的算子规则，并将 SchemaOperator 注入到 Flink 的执行拓扑中。
    private DataStream<Event> addRegularSchemaOperator(
            DataStream<Event> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        // 将用户在 YAML 或配置中定义的 RouteDef（逻辑层对象）转换为 RouteRule（运行时层对象）。
        List<RouteRule> routingRules = new ArrayList<>();
        for (RouteDef route : routes) {
            routingRules.add(
                    new RouteRule(
                            route.getSourceTable(),
                            route.getSinkTable(),
                            route.getReplaceSymbol().orElse(null)));
        }
        SingleOutputStreamOperator<Event> stream =
                input.transform(
                        "SchemaOperator", // 这是该算子在 Flink Web UI 上显示的默认名称
                        new EventTypeInfo(), // 指定算子输出的数据类型信息。Flink CDC 使用统一的 Event 类型（包含数据和 Schema 变更）
                        new SchemaOperatorFactory(
                                metadataApplier,
                                routingRules,
                                rpcTimeOut,
                                schemaChangeBehavior,
                                timezone));
        stream.uid(schemaOperatorUid).setParallelism(parallelism);
        return stream;
    }

    // 用于处理**批模式（Batch Mode）**下 Schema 算子构建的方法。
    // 相比流模式，批模式的逻辑更为精简，因为它主要处理全量同步阶段的表结构映射
    private DataStream<Event> addRegularSchemaBatchOperator(
            DataStream<Event> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes,
            String timezone) {
        // 将用户定义的逻辑路由配置 (RouteDef) 转换为算子内部使用的物理路由规则 (RouteRule)
        List<RouteRule> routingRules = new ArrayList<>();
        for (RouteDef route : routes) {
            routingRules.add(
                    new RouteRule(
                            route.getSourceTable(),
                            route.getSinkTable(),
                            route.getReplaceSymbol().orElse(null)));
        }
        // 在 Flink 数据流图中插入一个专门用于批处理的 Schema 转换算子
        SingleOutputStreamOperator<Event> stream =
                input.transform(
                        "SchemaBatchOperator",
                        new EventTypeInfo(),
                        new BatchSchemaOperator(routingRules, metadataApplier, timezone));
        stream.uid(schemaOperatorUid).setParallelism(parallelism);
        return stream;
    }
    // 专门用于构建分布式拓扑（Distributed Topology）下 Schema 算子的私有方法。
    // 它通常用于分库分表合并场景，此时数据在进入 Sink 之前需要经过复杂的分布式同步对齐。
    private DataStream<Event> addDistributedSchemaOperator(
            DataStream<PartitioningEvent> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        // 强制校验 Schema 变更策略。在分布式拓扑下，不支持 EVOLVE（激进演进）模式
        Preconditions.checkArgument(
                schemaChangeBehavior == SchemaChangeBehavior.LENIENT
                        || schemaChangeBehavior == SchemaChangeBehavior.IGNORE
                        || schemaChangeBehavior == SchemaChangeBehavior.EXCEPTION,
                "Schema change behavior %s is not supported because you're trying to compose a "
                        + "pipeline with distributed topology, where data records from different partitions needs to "
                        + "be combined together.\n"
                        + "Use `LENIENT` mode to evolve downstream schema while keep all upstream data fields intact.\n"
                        + "Use `IGNORE` to get a static schema view and ignore any upstream schema changes.\n"
                        + "Use `EXCEPTION` to report error immediately as upstream schema changes are unacceptable.",
                schemaChangeBehavior);
        // 将用户定义的 RouteDef 列表转换为物理执行的 RouteRule
        List<RouteRule> routingRules = new ArrayList<>();
        for (RouteDef route : routes) {
            routingRules.add(
                    new RouteRule(
                            route.getSourceTable(),
                            route.getSinkTable(),
                            route.getReplaceSymbol().orElse(null)));
        }
        return input.transform(
                        "SchemaMapper",
                        new EventTypeInfo(),
                        // 在算子链中插入针对分布式场景优化的 SchemaOperator
                        new org.apache.flink.cdc.runtime.operators.schema.distributed
                                .SchemaOperatorFactory(
                                metadataApplier,
                                routingRules,
                                rpcTimeOut,
                                schemaChangeBehavior,
                                timezone))
                .uid(schemaOperatorUid)
                .setParallelism(parallelism);
    }
}
