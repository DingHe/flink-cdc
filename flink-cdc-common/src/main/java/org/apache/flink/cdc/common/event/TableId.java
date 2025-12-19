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

package org.apache.flink.cdc.common.event;

import org.apache.flink.cdc.common.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * The unique identifier used to represent the path of external table or external data collection,
 * all external system data collection could map to a dedicated {@code TableId}.
 *
 * <ul>
 *   <li>{@code TableId} contains at most three parts, it will be treated as namespace, schema name
 *       and table name.
 *   <li>{@code TableId} could contain two parts, it will be treated as schema name and table name.
 *   <li>{@code TableId} could contain only one part, it will be treated as table name.
 * </ul>
 *
 * <p>Connectors need to establish the mapping between {@code TableId} and external data collection
 * object path. For example,
 *
 * <ul>
 *   <li>The mapping relationship for Oracle is: (database, schema, table).
 *   <li>The mapping relationship for MySQL or Doris is: (database, table).
 *   <li>The mapping relationship for Kafka is: (topic).
 * </ul>
 */
// Flink CDC 统一架构中非常基础且重要的一个类，用于在分布式系统中唯一地标识一个外部数据集合（如数据库表、Kafka 主题等）
// TableId 的核心作用是为所有外部数据源提供一个标准化的三段式逻辑路径标识。
// 由于 Flink CDC 需要支持多种不同的外部系统（如 MySQL, Oracle, Kafka, MongoDB 等），而每种系统对“表”的层级定义都不一样，因此需要一个通用的模型来统一描述这些路径：
// 统一抽象：无论底层是 库.表 还是 库.模式.表，都映射为 namespace, schemaName, tableName 这三个字段。
@PublicEvolving
public class TableId implements Serializable {
    // 顶级命名空间。 对应最外层的层级（如 Oracle 的数据库名）。可选（可为 null）。
    @Nullable private final String namespace;
    // 模式名称。 对应中间层级（如 MySQL 的数据库名或 Postgres 的 Schema 名）。可选。
    @Nullable private final String schemaName;
    // 表名/实体名。 对应最底层的实体（如数据库表名或 Kafka Topic）。必填。
    private final String tableName;
    // 哈希码缓存。 使用 transient 修饰（不参与序列化）。为了提高性能，在第一次计算 hashCode 后将其缓存。
    private transient int cachedHashCode;

    private TableId(@Nullable String namespace, @Nullable String schemaName, String tableName) {
        this.namespace = namespace;
        this.schemaName = schemaName;
        this.tableName = Objects.requireNonNull(tableName);
    }

    /** The mapping relationship for external systems. e.g. Oracle (database, schema, table). */
    public static TableId tableId(String namespace, String schemaName, String tableName) {
        return new TableId(
                Objects.requireNonNull(namespace), Objects.requireNonNull(schemaName), tableName);
    }

    /** The mapping relationship for external systems. e.g. MySQL (database, table). */
    public static TableId tableId(String schemaName, String tableName) {
        return new TableId(null, Objects.requireNonNull(schemaName), tableName);
    }

    /** The mapping relationship for external systems. e.g. Kafka (topic). */
    public static TableId tableId(String tableName) {
        return new TableId(null, null, tableName);
    }

    public static TableId parse(String tableId) {
        String[] parts = Objects.requireNonNull(tableId).split("\\.");
        if (parts.length == 3) {
            return tableId(parts[0], parts[1], parts[2]);
        } else if (parts.length == 2) {
            return tableId(parts[0], parts[1]);
        } else if (parts.length == 1) {
            return tableId(parts[0]);
        }
        throw new IllegalArgumentException("Invalid tableId: " + tableId);
    }

    public String identifier() {
        if (namespace == null || namespace.isEmpty()) {
            if (schemaName == null || schemaName.isEmpty()) {
                return tableName;
            }
            return schemaName + "." + tableName;
        }
        return namespace + "." + schemaName + "." + tableName;
    }

    @Nullable
    public String getNamespace() {
        return namespace;
    }

    @Nullable
    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableId that = (TableId) o;
        return Objects.equals(namespace, that.namespace)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        if (cachedHashCode == 0) {
            cachedHashCode = Objects.hash(namespace, schemaName, tableName);
        }
        return cachedHashCode;
    }

    @Override
    public String toString() {
        return identifier();
    }
}
