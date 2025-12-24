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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** Operator for processing events from upstream before flowing to {@link SchemaOperator}. */
// DistributedPrePartitionOperator 是处理分布式数据源（如分库分表场景）的关键算子。
// 它与之前提到的 RegularPrePartitionOperator 类似，但在处理 Schema 演化和事件追踪方面有特定的分布式考量。
// 主要作用是：在分布式拓扑中，为下游的 SchemaOperator 准备带路由信息的事件。
// 本地 Schema 推理：由于是分布式模式，该算子通过维护本地的 schemaMap，自主推理最新的表结构，而不需要像常规模式那样频繁通过 RPC 向 Coordinator 请求。
// 物理分区路由：计算 DataChangeEvent 的哈希值，决定数据流向哪个下游并行实例。
// 源追踪：利用 subTaskId 标记事件的来源，这对于下游 SchemaOperator 对齐来自不同上游并发的 DDL 事件至关重要。
// 事件广播：将 DDL 事件（SchemaChangeEvent）复制并发送给下游所有并行任务。
@Internal
public class DistributedPrePartitionOperator extends AbstractStreamOperator<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent>, Serializable {
    private static final long serialVersionUID = 1L;
    // 下游算子的并行度。用于哈希取模计算，确定数据去往哪个下游槽位（Bucket）。
    private final int downstreamParallelism;
    // 哈希函数提供者。
    // 定义了如何根据特定的 Schema 为表生成哈希逻辑（通常基于主键）。
    private final HashFunctionProvider<DataChangeEvent> hashFunctionProvider;

    // Schema and HashFunctionMap used in schema inferencing mode.
    // 在内存中缓存每个表的最新结构（Schema）
    private transient Map<TableId, Schema> schemaMap;
    // 缓存每个表当前对应的哈希函数对象
    private transient Map<TableId, HashFunction<DataChangeEvent>> hashFunctionMap;
    // 记录当前算子实例在 Flink 任务中的子任务索引（Index）
    private transient int subTaskId;

    public DistributedPrePartitionOperator(
            int downstreamParallelism, HashFunctionProvider<DataChangeEvent> hashFunctionProvider) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.downstreamParallelism = downstreamParallelism;
        this.hashFunctionProvider = hashFunctionProvider;
    }

    @Override
    public void open() throws Exception {
        super.open();
        // 获取当前的 subTaskId
        subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        schemaMap = new HashMap<>();
        hashFunctionMap = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();

            // Update schema map
            // 根据当前的 DDL 动作（如加列、删列）更新本地 schemaMap 中的表结构。
            schemaMap.compute(
                    tableId,
                    (tId, oldSchema) ->
                            SchemaUtils.applySchemaChangeEvent(oldSchema, schemaChangeEvent));

            // Update hash function
            hashFunctionMap.put(tableId, recreateHashFunction(tableId));

            // Broadcast SchemaChangeEvent
            broadcastEvent(event);
        } else if (event instanceof DataChangeEvent) {
            // Partition DataChangeEvent by table ID and primary keys
            partitionBy((DataChangeEvent) event);
        } else {
            throw new IllegalStateException(
                    subTaskId + "> PrePartition operator received an unexpected event: " + event);
        }
    }

    private void partitionBy(DataChangeEvent dataChangeEvent) {
        output.collect(
                new StreamRecord<>(
                        PartitioningEvent.ofDistributed(
                                dataChangeEvent,
                                subTaskId,
                                hashFunctionMap
                                                .get(dataChangeEvent.tableId())
                                                .hashcode(dataChangeEvent)
                                        % downstreamParallelism)));
    }

    private void broadcastEvent(Event toBroadcast) {
        for (int i = 0; i < downstreamParallelism; i++) {
            // Deep-copying each event is required since downstream subTasks might run in the same
            // JVM
            Event copiedEvent = EventSerializer.INSTANCE.copy(toBroadcast);
            output.collect(
                    new StreamRecord<>(PartitioningEvent.ofDistributed(copiedEvent, subTaskId, i)));
        }
    }

    private HashFunction<DataChangeEvent> recreateHashFunction(TableId tableId) {
        return hashFunctionProvider.getHashFunction(tableId, schemaMap.get(tableId));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // Needless to do anything, since AbstractStreamOperator#snapshotState and #processElement
        // is guaranteed not to be mixed together.
    }
}
