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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.MySqlValidator;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.BinlogPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsStateSerializer;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlRecordEmitter;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReaderContext;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConnection;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * The MySQL CDC Source based on FLIP-27 and Watermark Signal Algorithm which supports parallel
 * reading snapshot of table and then continue to capture data change from binlog.
 *
 * <pre>
 *     1. The source supports parallel capturing table change.
 *     2. The source supports checkpoint in split level when read snapshot data.
 *     3. The source doesn't need apply any lock of MySQL.
 * </pre>
 *
 * <pre>{@code
 * MySqlSource
 *     .<String>builder()
 *     .hostname("localhost")
 *     .port(3306)
 *     .databaseList("mydb")
 *     .tableList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .serverId(5400)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>See {@link MySqlSourceBuilder} for more details.
 *
 * @param <T> the output type of the source.
 */
// 核心作用是定义 MySQL 数据的并行捕获逻辑。它不仅仅是一个简单的连接器，其先进性体现在以下几点：
// 分阶段读取：支持先通过多并行度读取历史存量数据（Snapshot 阶段），然后无缝切换到单并行度读取增量日志（Binlog 阶段）。
// 无锁设计：引入了 Watermark Signal 算法，在读取存量数据时不需要对数据库加锁（即便不加全局锁，也能保证数据的一致性）
// 断点续传：支持分片（Split）级别的 Checkpoint，如果任务失败，可以从精确的读取位置恢复，而不需要重新扫描全表。
// 高并行度：允许用户设置多个并行度来并行扫描大表。
@Internal
public class MySqlSource<T>
        implements Source<T, MySqlSplit, PendingSplitsState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;
    // 分片枚举器使用的服务名称，用于标识内部的 MySQL 连接。
    private static final String ENUMERATOR_SERVER_NAME = "mysql_source_split_enumerator";
    // 配置工厂。 负责根据用户的参数（如 host, user）生成具体的配置对象 MySqlSourceConfig。
    private final MySqlSourceConfigFactory configFactory;
    // 反序列化器。
    // 将 Debezium 产生的原始记录转换为用户需要的类型（如 JSON, RowData）。
    private final DebeziumDeserializationSchema<T> deserializationSchema;
    // 数据发送器提供者。
    // 用于生成 RecordEmitter，负责将读取到的数据发送到下游算子。
    private final RecordEmitterSupplier<T> recordEmitterSupplier;

    // Actions to perform during the snapshot phase.
    // This field is introduced for testing purpose, for example testing if changes made in the
    // snapshot phase are correctly backfilled into the snapshot by registering a pre high watermark
    // hook for generating changes.
    // 快照钩子。
    // 主要用于测试，允许在快照阶段（读全量数据时）插入自定义动作（如模拟数据变更）。
    private SnapshotPhaseHooks snapshotHooks = SnapshotPhaseHooks.empty();

    /**
     * Get a MySqlParallelSourceBuilder to build a {@link MySqlSource}.
     *
     * @return a MySql parallel source builder.
     */
    @PublicEvolving
    public static <T> MySqlSourceBuilder<T> builder() {
        return new MySqlSourceBuilder<>();
    }

    MySqlSource(
            MySqlSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema) {
        this(
                configFactory,
                deserializationSchema,
                (sourceReaderMetrics, sourceConfig) ->
                        new MySqlRecordEmitter<>(
                                deserializationSchema,
                                sourceReaderMetrics,
                                sourceConfig.isIncludeSchemaChanges()));
    }

    MySqlSource(
            MySqlSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            RecordEmitterSupplier<T> recordEmitterSupplier) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
        this.recordEmitterSupplier = recordEmitterSupplier;
    }

    public MySqlSourceConfigFactory getConfigFactory() {
        return configFactory;
    }

    @Override
    public Boundedness getBoundedness() {
        MySqlSourceConfig sourceConfig = configFactory.createConfig(0);
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            return Boundedness.BOUNDED;
        } else {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }
    }

    @Override
    public SourceReader<T, MySqlSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        MySqlSourceConfig sourceConfig =
                configFactory.createConfig(readerContext.getIndexOfSubtask());
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        final Method metricGroupMethod = readerContext.getClass().getMethod("metricGroup");
        metricGroupMethod.setAccessible(true);
        final MetricGroup metricGroup = (MetricGroup) metricGroupMethod.invoke(readerContext);

        final MySqlSourceReaderMetrics sourceReaderMetrics =
                new MySqlSourceReaderMetrics(metricGroup);
        sourceReaderMetrics.registerMetrics();
        MySqlSourceReaderContext mySqlSourceReaderContext =
                new MySqlSourceReaderContext(readerContext);
        Supplier<MySqlSplitReader> splitReaderSupplier =
                () ->
                        new MySqlSplitReader(
                                sourceConfig,
                                readerContext.getIndexOfSubtask(),
                                mySqlSourceReaderContext,
                                snapshotHooks);
        return new MySqlSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                recordEmitterSupplier.get(sourceReaderMetrics, sourceConfig),
                readerContext.getConfiguration(),
                mySqlSourceReaderContext,
                sourceConfig);
    }

    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext) {
        MySqlSourceConfig sourceConfig = configFactory.createConfig(0, ENUMERATOR_SERVER_NAME);

        final MySqlValidator validator = new MySqlValidator(sourceConfig);
        validator.validate();

        final MySqlSplitAssigner splitAssigner;
        // In snapshot-only startup option, only split snapshots.
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
                boolean isTableIdCaseSensitive = DebeziumUtils.isTableIdCaseSensitive(jdbc);
                splitAssigner =
                        new MySqlSnapshotSplitAssigner(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                new ArrayList<>(),
                                isTableIdCaseSensitive,
                                enumContext);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else if (!sourceConfig.getStartupOptions().isStreamOnly()) {
            try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
                boolean isTableIdCaseSensitive = DebeziumUtils.isTableIdCaseSensitive(jdbc);
                splitAssigner =
                        new MySqlHybridSplitAssigner(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                new ArrayList<>(),
                                isTableIdCaseSensitive,
                                enumContext);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner = new MySqlBinlogSplitAssigner(sourceConfig);
        }

        return new MySqlSourceEnumerator(
                enumContext, sourceConfig, splitAssigner, getBoundedness());
    }

    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext, PendingSplitsState checkpoint) {

        MySqlSourceConfig sourceConfig = configFactory.createConfig(0, ENUMERATOR_SERVER_NAME);

        final MySqlSplitAssigner splitAssigner;
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new MySqlHybridSplitAssigner(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (HybridPendingSplitsState) checkpoint,
                            enumContext);
        } else if (checkpoint instanceof BinlogPendingSplitsState) {
            splitAssigner =
                    new MySqlBinlogSplitAssigner(
                            sourceConfig, (BinlogPendingSplitsState) checkpoint);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }
        return new MySqlSourceEnumerator(
                enumContext, sourceConfig, splitAssigner, getBoundedness());
    }

    @Override
    public SimpleVersionedSerializer<MySqlSplit> getSplitSerializer() {
        return MySqlSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsStateSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @VisibleForTesting
    public void setSnapshotHooks(SnapshotPhaseHooks snapshotHooks) {
        this.snapshotHooks = snapshotHooks;
    }

    /** Create a {@link RecordEmitter} for {@link MySqlSourceReader}. */
    @Internal
    @FunctionalInterface
    interface RecordEmitterSupplier<T> extends Serializable {

        RecordEmitter<SourceRecords, T, MySqlSplitState> get(
                MySqlSourceReaderMetrics metrics, MySqlSourceConfig sourceConfig);
    }
}
