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
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

/**
 * Operator for processing events from {@link SchemaOperator} before {@link EventPartitioner} with
 * regular topology.
 */
// 主要作用是**“为物理分区做准备”**。它负责处理所有流入的事件（数据变更、结构变更、刷新事件），并根据事件类型决定如何将它们路由到下游的并行子任务中。
// 哈希预计算：对于数据变更（DataChangeEvent），它计算每条数据的主键哈希值，并确定该数据所属的下游子任务索引。
// 事件广播：对于管理类事件（SchemaChangeEvent 和 FlushEvent），它负责将事件“广播”给下游所有的并行子任务，确保整个流水线的状态同步。
// Schema 动态响应：它持有一个 SchemaEvolutionClient，当表结构发生变化时，它能动态获取最新的 Schema 并重新构造哈希函数，确保分区的准确性。

@Internal
public class RegularPrePartitionOperator extends AbstractStreamOperator<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent>, Serializable {

    private static final long serialVersionUID = 1L;
    // 缓存过期时间（设置为 1 天）。用于清理长时间不活跃表的哈希函数缓存。
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);
    // 关联的 SchemaOperator 的唯一标识符，
    // 用于向 Coordinator 请求最新的 Schema 信息。
    private final OperatorID schemaOperatorId;
    // 下游算子（通常是 Sink 或分区处理器）的并行度，用于哈希取模运算。
    private final int downstreamParallelism;
    // 哈希函数提供者，定义了如何根据表 ID 和 Schema 生成具体的哈希算法。
    private final HashFunctionProvider<DataChangeEvent> hashFunctionProvider;
    // 内部 RPC 客户端，负责与 SchemaRegistry（通常在 Coordinator 端）通信，拉取最新的表结构。
    private transient SchemaEvolutionClient schemaEvolutionClient;
    // 缓存了每个表对应的 HashFunction，避免对每条数据都进行复杂的哈希函数初始化操作。
    private transient LoadingCache<TableId, HashFunction<DataChangeEvent>> cachedHashFunctions;

    public RegularPrePartitionOperator(
            OperatorID schemaOperatorId,
            int downstreamParallelism,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.schemaOperatorId = schemaOperatorId;
        this.downstreamParallelism = downstreamParallelism;
        this.hashFunctionProvider = hashFunctionProvider;
    }

    @Override
    public void open() throws Exception {
        super.open();
        // 获取 Coordinator 的网关（Gateway）
        TaskOperatorEventGateway toCoordinator =
                getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway();
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, schemaOperatorId);
        cachedHashFunctions = createCache();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            // Update hash function
            TableId tableId = ((SchemaChangeEvent) event).tableId();
            cachedHashFunctions.put(tableId, recreateHashFunction(tableId));
            // Broadcast SchemaChangeEvent
            // 将 DDL 事件发送给所有下游
            broadcastEvent(event);
        } else if (event instanceof FlushEvent) {
            // Broadcast FlushEvent
            broadcastEvent(event);
        } else if (event instanceof DataChangeEvent) {
            // Partition DataChangeEvent by table ID and primary keys
            partitionBy(((DataChangeEvent) event));
        }
    }

    private void partitionBy(DataChangeEvent dataChangeEvent) throws Exception {
        // 从缓存中获取当前表的哈希函数
        // 计算该行数据的哈希值
        // 对 downstreamParallelism 取模，得到目标分区索引
        // 封装成 PartitioningEvent 发送至下游
        output.collect(
                new StreamRecord<>(
                        PartitioningEvent.ofRegular(
                                dataChangeEvent,
                                cachedHashFunctions
                                                .get(dataChangeEvent.tableId())
                                                .hashcode(dataChangeEvent)
                                        % downstreamParallelism)));
    }
    // 模拟广播行为
    // 循环 downstreamParallelism 次，为每个下游子任务创建一个包含该事件的 PartitioningEvent
    private void broadcastEvent(Event toBroadcast) {
        for (int i = 0; i < downstreamParallelism; i++) {
            // Deep-copying each event is required since downstream subTasks might run in the same
            // JVM
            Event copiedEvent = EventSerializer.INSTANCE.copy(toBroadcast);
            output.collect(new StreamRecord<>(PartitioningEvent.ofRegular(copiedEvent, i)));
        }
    }
    // 通过 RPC 向中央注册表请求表结构。
    private Schema loadLatestSchemaFromRegistry(TableId tableId) {
        Optional<Schema> schema;
        try {
            schema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to request latest schema for table \"%s\"", tableId), e);
        }
        if (!schema.isPresent()) {
            throw new IllegalStateException(
                    String.format(
                            "Schema is never registered or outdated for table \"%s\"", tableId));
        }
        return schema.get();
    }

    private HashFunction<DataChangeEvent> recreateHashFunction(TableId tableId) {
        return hashFunctionProvider.getHashFunction(tableId, loadLatestSchemaFromRegistry(tableId));
    }

    private LoadingCache<TableId, HashFunction<DataChangeEvent>> createCache() {
        return CacheBuilder.newBuilder()
                .expireAfterAccess(CACHE_EXPIRE_DURATION)
                .build(
                        new CacheLoader<TableId, HashFunction<DataChangeEvent>>() {
                            @Override
                            public HashFunction<DataChangeEvent> load(TableId key) {
                                return recreateHashFunction(key);
                            }
                        });
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // Needless to do anything, since AbstractStreamOperator#snapshotState and #processElement
        // is guaranteed not to be mixed together.
    }
}
