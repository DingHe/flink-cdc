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
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/** {@code MetadataApplier} is used to apply metadata changes to external systems. */
// MetadataApplier 是一个至关重要的 SPI（服务提供者接口）。它定义了如何将逻辑层推导出的结构变更真正落地到物理存储介质的规范。
// MetadataApplier 的核心作用是 “充当下游系统的 DDL 执行代理”
// 在分布式 CDC 管道中，当上游源表（如 MySQL）发生加列、删列等 DDL 操作时，Flink CDC 会在内存中计算出下游表（如 Doris, StarRocks, Iceberg）应该如何同步变化。然而，Flink 自身并不直接操作外部数据库。
// MetadataApplier 接口就是为了给各种不同的 Sink 提供一个标准入口，让它们实现各自的物理 DDL 执行逻辑。
@PublicEvolving
public interface MetadataApplier extends Serializable, AutoCloseable {

    /** Apply the given {@link SchemaChangeEvent} to external systems. */
    // 执行真正的 Schema 变更操作
    // 具体的 Sink 实现类（例如 DorisMetadataApplier）会解析这个事件，
    // 构造出对应数据库的 SQL 语句（如 ALTER TABLE ... ADD COLUMN ...），然后通过 JDBC 或其他 API 提交给外部系统。
    void applySchemaChange(SchemaChangeEvent schemaChangeEvent) throws SchemaEvolveException;

    /** Sets enabled schema evolution event types of current metadata applier. */
    default MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        return this;
    }

    /** Checks if this metadata applier should this event type. */
    // 设置当前 Applier 允许接收的变更类型。
    default boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return true;
    }

    /** Checks what kind of schema change events downstream can handle. */
    // 检查是否支持/接受特定的变更类型。
    default Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet());
    }

    /** Closes the metadata applier and its underlying resources. */
    @Override
    default void close() throws Exception {}
}
