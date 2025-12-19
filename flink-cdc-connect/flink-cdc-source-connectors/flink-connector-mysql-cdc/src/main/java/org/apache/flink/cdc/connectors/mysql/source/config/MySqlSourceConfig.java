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

package org.apache.flink.cdc.connectors.mysql.source.config;

import org.apache.flink.cdc.connectors.mysql.schema.Selectors;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.table.catalog.ObjectPath;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A MySql Source configuration which is used by {@link MySqlSource}. */
// MySqlSourceConfig 是 MySqlSource 的属性持有者。它的主要作用是将用户在 Flink DDL 或 API 中定义的各种参数（如数据库地址、账号密码、并行读取策略、Debezium 特有配置等）封装成一个可序列化的对象。
// 在 Flink 分布式运行环境中，这个类会被分发到各个 TaskManager 上，指导每个 Source 读取算子如何连接数据库、如何进行分片（Snapshot Split）以及如何处理数据转换。

public class MySqlSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    // MySQL 服务器的地址和端口。
    private final String hostname;
    private final int port;
    // 数据库登录凭证。
    private final String username;
    private final String password;
    // 需要监控的数据库列表。
    private final List<String> databaseList;
    // 需要监控的表列表（通常格式为 db.table）。
    private final List<String> tableList;
    // 明确排除的表列表。
    private final String excludeTableList;
    // 定义 MySQL server-id 的范围。在多并行度读取 Binlog 时，每个并行实例需要唯一的 server-id。
    @Nullable private final ServerIdRange serverIdRange;
    // 启动模式（全量、增量、指定位点等）。
    private final StartupOptions startupOptions;
    // 存量读取阶段，每个分片（Chunk）的大小。
    private final int splitSize;
    // 负责分片的元数据分组大小。
    private final int splitMetaGroupSize;
    // 每次从数据库读取记录的行数。
    private final int fetchSize;
    // 服务器时区，用于正确解析 DATETIME 等时区相关字段。
    private final String serverTimeZone;
    // 连接超时时间。
    private final Duration connectTimeout;
    // 连接失败时的最大重试次数。
    private final int connectMaxRetries;
    // 接池大小。
    private final int connectionPoolSize;
    // 用于平衡分片分布的因子，防止数据倾斜。
    private final double distributionFactorUpper;
    private final double distributionFactorLower;
    // 是否捕获 DDL（结构变更）事件。
    private final boolean includeSchemaChanges;
    // 是否允许在作业运行过程中动态扫描新增加的表。
    private final boolean scanNewlyAddedTableEnabled;
    // 是否关闭空闲的读取器以节省资源。
    private final boolean closeIdleReaders;
    // 额外的 JDBC 参数（如 useSSL、allowPublicKeyRetrieval 等）。
    private final Properties jdbcProperties;
    // 指定用于分片的列（如果不使用主键，可以在此处配置自定义列）。
    private final Map<ObjectPath, String> chunkKeyColumns;
    // 是否跳过快照阶段的回填过程。开启可提速，但可能导致数据一致性风险。
    private final boolean skipSnapshotBackfill;
    // 是否解析在线 Schema 变更（如通过 Gh-ost 或 PT-OSC 工具进行的变更）。
    private final boolean parseOnLineSchemaChanges;
    public static boolean useLegacyJsonFormat = true;
    // 是否优先分配未绑定的分片
    private final boolean assignUnboundedChunkFirst;

    // --------------------------------------------------------------------------------------------
    // Debezium Configurations
    // --------------------------------------------------------------------------------------------
    // 原始的 Debezium 配置项。
    private final Properties dbzProperties;
    // 转换后的 Debezium 配置对象。
    private final Configuration dbzConfiguration;
    // Debezium 专用的 MySQL 连接器配置类。
    private final MySqlConnectorConfig dbzMySqlConfig;
    // 是否将 MySQL 的 TINYINT(1) 映射为 Java 的 Boolean。
    private final boolean treatTinyInt1AsBoolean;

    MySqlSourceConfig(
            String hostname,
            int port,
            String username,
            String password,
            List<String> databaseList,
            List<String> tableList,
            @Nullable String excludeTableList,
            @Nullable ServerIdRange serverIdRange,
            StartupOptions startupOptions,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            boolean scanNewlyAddedTableEnabled,
            boolean closeIdleReaders,
            Properties dbzProperties,
            Properties jdbcProperties,
            Map<ObjectPath, String> chunkKeyColumns,
            boolean skipSnapshotBackfill,
            boolean parseOnLineSchemaChanges,
            boolean treatTinyInt1AsBoolean,
            boolean useLegacyJsonFormat,
            boolean assignUnboundedChunkFirst) {
        this.hostname = checkNotNull(hostname);
        this.port = port;
        this.username = checkNotNull(username);
        this.password = password;
        this.databaseList = checkNotNull(databaseList);
        this.tableList = checkNotNull(tableList);
        this.excludeTableList = excludeTableList;
        this.serverIdRange = serverIdRange;
        this.startupOptions = checkNotNull(startupOptions);
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.serverTimeZone = checkNotNull(serverTimeZone);
        this.connectTimeout = checkNotNull(connectTimeout);
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.includeSchemaChanges = includeSchemaChanges;
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
        this.closeIdleReaders = closeIdleReaders;
        this.dbzProperties = checkNotNull(dbzProperties);
        this.dbzConfiguration = Configuration.from(dbzProperties);
        this.dbzMySqlConfig = new MySqlConnectorConfig(dbzConfiguration);
        Selectors excludeTableFilter =
                (excludeTableList == null
                        ? null
                        : new Selectors.SelectorsBuilder().includeTables(excludeTableList).build());
        Tables.TableFilter tableFilter = dbzMySqlConfig.getTableFilters().dataCollectionFilter();
        dbzMySqlConfig
                .getTableFilters()
                .setDataCollectionFilters(
                        (TableId tableId) ->
                                tableFilter.isIncluded(tableId)
                                        && (excludeTableFilter == null
                                                || !excludeTableFilter.isMatch(tableId)));
        this.jdbcProperties = jdbcProperties;
        this.chunkKeyColumns = chunkKeyColumns;
        this.skipSnapshotBackfill = skipSnapshotBackfill;
        this.parseOnLineSchemaChanges = parseOnLineSchemaChanges;
        this.treatTinyInt1AsBoolean = treatTinyInt1AsBoolean;
        this.useLegacyJsonFormat = useLegacyJsonFormat;
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public List<String> getDatabaseList() {
        return databaseList;
    }

    public List<String> getTableList() {
        return tableList;
    }

    @Nullable
    public ServerIdRange getServerIdRange() {
        return serverIdRange;
    }

    public StartupOptions getStartupOptions() {
        return startupOptions;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public int getSplitMetaGroupSize() {
        return splitMetaGroupSize;
    }

    public double getDistributionFactorUpper() {
        return distributionFactorUpper;
    }

    public double getDistributionFactorLower() {
        return distributionFactorLower;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public String getServerTimeZone() {
        return serverTimeZone;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public int getConnectMaxRetries() {
        return connectMaxRetries;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    public boolean isIncludeSchemaChanges() {
        return includeSchemaChanges;
    }

    public boolean isScanNewlyAddedTableEnabled() {
        return scanNewlyAddedTableEnabled;
    }

    public boolean isCloseIdleReaders() {
        return closeIdleReaders;
    }

    public boolean isParseOnLineSchemaChanges() {
        return parseOnLineSchemaChanges;
    }

    public boolean isAssignUnboundedChunkFirst() {
        return assignUnboundedChunkFirst;
    }

    public Properties getDbzProperties() {
        return dbzProperties;
    }

    public Configuration getDbzConfiguration() {
        return dbzConfiguration;
    }

    public MySqlConnectorConfig getMySqlConnectorConfig() {
        return dbzMySqlConfig;
    }

    @Deprecated
    public RelationalTableFilters getTableFilters() {
        return dbzMySqlConfig.getTableFilters();
    }

    public Predicate<String> getDatabaseFilter() {
        RelationalTableFilters tableFilters = dbzMySqlConfig.getTableFilters();
        return (String databaseName) -> tableFilters.databaseFilter().test(databaseName);
    }

    public Predicate<TableId> getTableFilter() {
        RelationalTableFilters tableFilters = dbzMySqlConfig.getTableFilters();
        return tableId -> tableFilters.dataCollectionFilter().isIncluded(tableId);
    }

    public Properties getJdbcProperties() {
        return jdbcProperties;
    }

    public Map<ObjectPath, String> getChunkKeyColumns() {
        return chunkKeyColumns;
    }

    public boolean isSkipSnapshotBackfill() {
        return skipSnapshotBackfill;
    }

    public boolean isTreatTinyInt1AsBoolean() {
        return treatTinyInt1AsBoolean;
    }
}
