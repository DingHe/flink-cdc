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
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlPipelineRecordEmitter;
import org.apache.flink.cdc.connectors.mysql.table.MySqlReadableMetadata;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.relational.RelationalDatabaseConnectorConfig;

import java.util.ArrayList;
import java.util.List;
// 在 Flink CDC 的新架构中，它扮演着“数据源工厂的执行者”角色。其核心作用是将 MySQL 的特定逻辑（如 Binlog 读取、快照切分）封装进统一的 Pipeline 模型中。
// 桥接配置：将 MySqlSourceConfig 与 Flink 的 Source API 连接起来。
// 构建读取算子：负责实例化 MySqlSource，这是真正执行数据捕获的 Flink 算子。
// 定义转换逻辑：指定如何将数据库底层的原始记录（Debezium 格式）解析为 Pipeline 框架通用的 Event（事件）对象。
// 提供元数据支持：通过它，框架可以知道 MySQL 支持哪些元数据列，以及如何访问库表结构。
/** A {@link DataSource} for mysql cdc connector. */
@Internal
public class MySqlDataSource implements DataSource {
    // 配置工厂。
    // 用于生成 MySqlSourceConfig。它保存了构建配置所需的参数（如 URL、账号等），支持延迟创建配置对象。
    private final MySqlSourceConfigFactory configFactory;
    // MySQL 数据源配置实例。
    // 它是从工厂中创建出来的具体配置对象，包含了连接、过滤、分片等所有运行参数。
    private final MySqlSourceConfig sourceConfig;
    // 可读元数据列表。 记录了用户希望从 MySQL 捕获的额外信息（如 op_ts、table_name 等）
    private List<MySqlReadableMetadata> readableMetadataList;

    public MySqlDataSource(MySqlSourceConfigFactory configFactory) {
        this(configFactory, new ArrayList<>());
    }

    public MySqlDataSource(
            MySqlSourceConfigFactory configFactory,
            List<MySqlReadableMetadata> readableMetadataList) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.createConfig(0);
        this.readableMetadataList = readableMetadataList;
    }
    // 负责生产“数据捕获能力”。
    @Override
    public EventSourceProvider getEventSourceProvider() {
        // 从 Debezium 配置中读取是否需要包含表和列的注释信息。
        boolean includeComments =
                sourceConfig
                        .getDbzConfiguration()
                        .getBoolean(
                                RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_COMMENTS.name(),
                                false);
        // 创建反序列化器
        // 负责将 MySQL 的变更记录转化为 Pipeline 架构定义的 Event 对象。它会接收 changelogMode（全变更模式）、元数据列表、注释配置等。
        MySqlEventDeserializer deserializer =
                new MySqlEventDeserializer(
                        DebeziumChangelogMode.ALL,
                        sourceConfig.isIncludeSchemaChanges(),
                        readableMetadataList,
                        includeComments,
                        sourceConfig.isTreatTinyInt1AsBoolean());
        // 实例化 MySqlSource：这是 Flink 的 Source 实现。
        MySqlSource<Event> source =
                new MySqlSource<>(
                        configFactory,
                        deserializer,
                        (sourceReaderMetrics, sourceConfig) ->
                                new MySqlPipelineRecordEmitter(
                                        deserializer, sourceReaderMetrics, sourceConfig));
        // 将构建好的 MySqlSource 包装并返回给 Flink 引擎进行调度。
        return FlinkSourceProvider.of(source);
    }
    // 获取元数据访问器。
    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new MySqlMetadataAccessor(sourceConfig);
    }

    @VisibleForTesting
    public MySqlSourceConfig getSourceConfig() {
        return sourceConfig;
    }
    // 声明此数据源支持哪些内置元数据列。
    // 目前返回 OpTsMetadataColumn，即支持捕获“数据库操作时间戳”。
    @Override
    public SupportedMetadataColumn[] supportedMetadataColumns() {
        return new SupportedMetadataColumn[] {new OpTsMetadataColumn()};
    }

    @Override
    public boolean isParallelMetadataSource() {
        // During incremental stage, MySQL never emits schema change events on different partitions
        // (since it has one Binlog stream only.)
        return false;
    }
}
