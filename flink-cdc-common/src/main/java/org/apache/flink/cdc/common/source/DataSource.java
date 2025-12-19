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

package org.apache.flink.cdc.common.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.annotation.PublicEvolving;

/**
 * {@code DataSource} is used to access metadata and read change data from external systems. It can
 * read data from multiple tables simultaneously.
 */
// DataSource 接口是 Flink CDC Pipeline 架构中对外部数据源（如 MySQL、PostgreSQL 等）进行抽象和建模的顶级接口。
// 核心作用是统一数据源的访问和行为，将外部系统视为一个可以提供两类信息的实体：
// 数据变更事件（Event）：用于实时捕获和传输数据的插入、更新、删除等操作。
// 元数据（Metadata）：用于访问数据库的表结构、Schema 信息等。
// 任何希望集成到 Flink CDC Pipeline 的外部系统（如 MySQL 连接器、MongoDB 连接器），都必须实现这个 DataSource 接口，从而向 Flink 运行时暴露其数据读取和元数据访问的能力。


@PublicEvolving
public interface DataSource {

    /** Get the {@link EventSourceProvider} for reading events from external systems. */
    // 获取事件源提供者。
    // 返回一个 EventSourceProvider 实例。
    // 这是 DataSource 的核心功能，它定义了如何从外部系统读取实时数据变更事件（Event）。
    // 在 Flink 运行时中，这个 Provider 将被用于构建最终的 Flink DataStreamSource。
    EventSourceProvider getEventSourceProvider();

    /** Get the {@link MetadataAccessor} for accessing metadata from external systems. */
    // 获取元数据访问器。 返回一个 MetadataAccessor 实例。
    // 这个访问器定义了如何从外部系统访问和获取元数据（如表结构、字段类型、主键信息等）。
    // 它通常用于 Pipeline 初始化时的 Schema 发现，或在运行时处理 Schema 变更事件。
    MetadataAccessor getMetadataAccessor();

    /** Get the {@link SupportedMetadataColumn}s of the source. */
    // 获取支持的元数据列。
    // 返回一个数组，列出该 Source 支持作为数据列读取的系统元数据列（例如，事务 ID、提交时间戳、Source 文件名等）。
    default SupportedMetadataColumn[] supportedMetadataColumns() {
        return new SupportedMetadataColumn[0];
    }

    /**
     * Indicating if this source may generate metadata events (SchemaChangeEvents) in parallel for
     * each table. If returns {@code false}, you'll get a regular operator topology that is
     * compatible with single-incremented sources like MySQL. Returns {@code true} for sources that
     * does not maintain a globally sequential schema change events stream, like MongoDB or Kafka.
     * <br>
     * Note that new topology still an experimental feature. Return {@code false} by default to
     * avoid unexpected behaviors.
     */
    // 指示是否为并行元数据源。
    // 标志该 Source 是否可以在每个表并行地生成元数据事件（如 SchemaChangeEvents），而不是遵循全局顺序。
    @Experimental
    default boolean isParallelMetadataSource() {
        return false;
    }
}
