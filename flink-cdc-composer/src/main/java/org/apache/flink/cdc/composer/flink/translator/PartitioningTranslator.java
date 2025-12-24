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
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.runtime.partitioning.BatchRegularPrePartitionOperator;
import org.apache.flink.cdc.runtime.partitioning.DistributedPrePartitionOperator;
import org.apache.flink.cdc.runtime.partitioning.EventPartitioner;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEventKeySelector;
import org.apache.flink.cdc.runtime.partitioning.PostPartitionProcessor;
import org.apache.flink.cdc.runtime.partitioning.RegularPrePartitionOperator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.cdc.runtime.typeutils.PartitioningEventTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Translator used to build {@link RegularPrePartitionOperator} or {@link
 * DistributedPrePartitionOperator}, {@link EventPartitioner} and {@link PostPartitionProcessor}
 * that are responsible for events partition.
 */
// PartitioningTranslator 是负责数据重分区逻辑的翻译官。
// 它的核心任务是确保数据在流入下游（通常是 Sink）之前，能够按照正确的规则（如主键哈希）分发到不同的并行线程中，
// 从而保证相同主键的数据有序处理，同时处理 Schema 变更时的算子同步。
// 实现数据洗牌（Shuffle）逻辑：它将上游产生的 Event 转换为 PartitioningEvent，并利用哈希函数计算数据应该去往哪个下游并行实例
// 保证数据有序性：通过自定义分区器（EventPartitioner），确保具有相同主键（Primary Key）的数据始终发送到同一个下游算子实例，防止出现“先改后删”在多并行度下乱序的问题。
// 衔接 Schema 变更控制：在处理 DDL（如加列、删表）时，它与 SchemaOperator 配合，通过分区操作来阻塞或释放数据流，确保 Schema 变更在所有并行任务中同步。

@Internal
public class PartitioningTranslator {
    // 是 translateRegular 的简化版，默认 isBatchMode 为 false（即流模式）
    public DataStream<Event> translateRegular(
            DataStream<Event> input, // 输入的数据流。
            int upstreamParallelism, // 上游算子的并行度。
            int downstreamParallelism, // 下游算子（Sink）的并行度。
            OperatorID schemaOperatorID, // 对应的 Schema 算子 ID，用于运行时状态关联。
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider) { // 提供哈希计算逻辑，决定数据分片规则
        return translateRegular(
                input,
                upstreamParallelism,
                downstreamParallelism,
                false,
                schemaOperatorID,
                hashFunctionProvider);
    }
    // Flink CDC 实现数据高性能重分区和端到端一致性的核心逻辑。它通过构建一个三阶段的算子链，确保数据在多并行度下依然能按主键顺序写入。
    public DataStream<Event> translateRegular(
            DataStream<Event> input,
            int upstreamParallelism,
            int downstreamParallelism,
            boolean isBatchMode,
            OperatorID schemaOperatorID,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider) {
        SingleOutputStreamOperator<Event> singleOutputStreamOperator =
                input.transform(
                                isBatchMode ? "BatchPrePartition" : "PrePartition",
                                new PartitioningEventTypeInfo(),
                                isBatchMode
                                        ? new BatchRegularPrePartitionOperator(
                                                downstreamParallelism, hashFunctionProvider)
                                        : new RegularPrePartitionOperator(
                                                schemaOperatorID,
                                                downstreamParallelism,
                                                hashFunctionProvider))
                        .setParallelism(upstreamParallelism)  // 设置上游并行度
                         // PartitionCustom（自定义物理分区）
                        .partitionCustom(new EventPartitioner(), new PartitioningEventKeySelector())
                         // 将中间转换用的 PartitioningEvent 还原为原始的 Event 类型，剥离分区元数据。
                        .map(new PostPartitionProcessor(), new EventTypeInfo())
                        .name(isBatchMode ? "BatchPostPartition" : "PostPartition");
        // 设置下游并行度并返回
        return isBatchMode
                ? singleOutputStreamOperator.setParallelism(downstreamParallelism)
                : singleOutputStreamOperator;
    }
    // 分表分库的场景
    public DataStream<PartitioningEvent> translateDistributed(
            DataStream<Event> input,
            int upstreamParallelism,
            int downstreamParallelism,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider) {
        return input.transform(
                        "Partitioning",
                        new PartitioningEventTypeInfo(),
                        new DistributedPrePartitionOperator(
                                downstreamParallelism, hashFunctionProvider))
                .setParallelism(upstreamParallelism)
                .partitionCustom(new EventPartitioner(), new PartitioningEventKeySelector());
    }
}
