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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.utils.MySqlSchemaUtils;

import io.debezium.connector.mysql.MySqlPartition;

import javax.annotation.Nullable;

import java.util.List;

/** {@link MetadataAccessor} for {@link MySqlDataSource}. */
// Flink CDC Pipeline 架构中专门为 MySQL 数据库实现的元数据访问组件。
// 核心作用是作为 Flink CDC 框架与 MySQL 数据库元数据之间的“通信桥梁”。
// 在 Flink CDC 的 Pipeline 同步过程中，框架不仅需要读取数据变更（Data Events），还需要了解数据库的结构（Metadata）。该类的具体职责包括：
// 库表发现：能够自动列出 MySQL 实例中存在的数据库和表，用于支持通配符匹配或全库同步时的表发现。
// 结构查询：能够查询指定表的具体结构（Schema），包括字段名、类型、主键、可空性等。
// 屏蔽底层差异：MySQL 和其他数据库（如 PostgreSQL）在元数据层级上有所不同（MySQL 没有 Namespace/Catalog 层级），该类负责将 MySQL 的层级映射到 Flink CDC 标准的 TableId 模型中。
@Internal
public class MySqlMetadataAccessor implements MetadataAccessor {
    // MySQL 数据源配置。
    // 包含了连接 MySQL 所需的所有信息（主机、端口、账号密码）以及过滤规则（白名单、黑名单）。它是获取元数据的依据。
    private final MySqlSourceConfig sourceConfig;
    // MySQL 分区标识。
    // 来源于 Debezium 库。它代表了当前数据源的逻辑分区，通常由 logicalName（通常是服务器 ID 或任务名）组成，用于在获取 Schema 时作为上下文标识。
    private final MySqlPartition partition;

    public MySqlMetadataAccessor(MySqlSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        this.partition =
                new MySqlPartition(sourceConfig.getMySqlConnectorConfig().getLogicalName());
    }

    /**
     * Always throw {@link UnsupportedOperationException} because MySQL does not support namespace.
     */
    @Override
    public List<String> listNamespaces() {
        throw new UnsupportedOperationException("List namespace is not supported by MySQL.");
    }

    /**
     * List all database from MySQL.
     *
     * @param namespace This parameter is ignored because MySQL does not support namespace.
     * @return The list of database
     */
    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        return MySqlSchemaUtils.listDatabases(sourceConfig);
    }

    /**
     * List tables from MySQL.
     *
     * @param namespace This parameter is ignored because MySQL does not support namespace.
     * @param dbName The database to list tables from. If null, list tables from all databases.
     * @return The list of {@link TableId}s.
     */
    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String dbName) {
        return MySqlSchemaUtils.listTables(sourceConfig, dbName);
    }

    /**
     * Get the {@link Schema} of the given table.
     *
     * @param tableId The {@link TableId} of the given table.
     * @return The {@link Schema} of the table.
     */
    @Override
    public Schema getTableSchema(TableId tableId) {
        return MySqlSchemaUtils.getTableSchema(sourceConfig, partition, tableId);
    }
}
