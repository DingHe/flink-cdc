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

package org.apache.flink.cdc.common.sink;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.function.HashFunctionProvider;

/**
 * {@code DataSink} is used to write change data to external system and apply metadata changes to
 * external systems as well.
 */
// 定义了一个下游连接器（Connector）为了接入 Flink CDC 管道所必须具备的所有核心能力。
// DataSink 的主要作用是封装下游系统的写入逻辑和元数据演进逻辑。
// 传统的 Flink Sink 往往只负责“写数据”。但在 CDC 场景下，由于上游数据库会发生加列、删列等 DDL 变更，下游 Sink 必须同时具备处理结构变更的能力。DataSink 将以下两部分职责整合在一起：
// 数据写入：如何处理 INSERT、UPDATE、DELETE 事件。
// 结构演进：如何将上游的 DDL 变更同步应用到目标库（如 Doris, StarRocks, Kafka 等）。
// ataSink 实际上是一个“能力工厂”：
@PublicEvolving
public interface DataSink {

    /** Get the {@link EventSinkProvider} for writing changed data to external systems. */
    // 获取用于数据写入的提供者对象
    // 它向 Flink 框架提供具体的 Sink 实现。这个提供者会根据实际情况返回 Flink 的 Sink (V2) 或 SinkFunction。
    // 负责处理 DataChangeEvent，即真正把数据行写入外部存储。
    EventSinkProvider getEventSinkProvider();

    /** Get the {@link MetadataApplier} for applying metadata changes to external systems. */
    // 获取用于应用元数据变更（DDL）的执行器。
    // 当上游发生 Schema 变更（如 CreateTableEvent 或 AddColumnEvent）时，SchemaCoordinator 会调用此方法返回的执行器。
    // 核心任务：它通常包含数据库连接逻辑，负责在目标系统执行类似 ALTER TABLE 的操作。
    MetadataApplier getMetadataApplier();

    /**
     * Get the {@code HashFunctionProvider<DataChangeEvent>} for calculating hash value if you need
     * to partition by data change event before Sink.
     */
    // 提供一个 哈希计算器，用于在数据进入 Sink 之前进行分区（Shuffle）
    // CDC 数据通常需要按主键进行哈希，以确保相同主键的数据进入同一个并行子任务（Subtask），从而保证写入的有序性（避免先删后插变成先插后删）。
    default HashFunctionProvider<DataChangeEvent> getDataChangeEventHashFunctionProvider() {
        return new DefaultDataChangeEventHashFunctionProvider();
    }
    // 根据指定的并行度提供哈希计算器。
    // parallelism 是 Sink 算子的并行度。
    // 它是上述无参版本的增强版。在某些复杂的场景下（例如分库分表映射），哈希算法可能需要根据并行度的规模进行调整（如一致性哈希），以优化数据分布。
    default HashFunctionProvider<DataChangeEvent> getDataChangeEventHashFunctionProvider(
            int parallelism) {
        return getDataChangeEventHashFunctionProvider(); // fallback to nullary version if it isn't
        // overridden
    }
}
