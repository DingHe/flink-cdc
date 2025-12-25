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

package org.apache.flink.cdc.connectors.mysql.source.assigners;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlSchema;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.connectors.mysql.source.connection.JdbcConnectionPools;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

/**
 * A {@link MySqlSplitAssigner} that splits tables into small chunk splits based on primary key
 * range and chunk size.
 *
 * @see MySqlSourceOptions#SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE
 */
// Flink CDC MySQL 连接器中极其核心的一个类。
// 它运行在 Flink 的 JobManager 端（作为 SplitEnumerator 的一部分），专门负责**全量读取阶段（Snapshot Phase）**的分片逻辑。
// MySqlSnapshotSplitAssigner 的主要职责是实现**增量快照读取算法（Incremental Snapshot Algorithm）**中的分片管理。
// 表发现与切分：它会自动发现数据库中匹配的表，并利用 MySqlChunkSplitter 根据主键（Primary Key）将一张大表切分成多个较小的“分块（Chunks/Splits）”。
// 异步切分：为了不阻塞 JobManager 的主线程，它开启了一个异步线程池来执行耗时的 SQL 查询（如查询主键范围）。
// 分片分配：它管理着哪些分片已经分配给了 TaskManager 上的 Reader，哪些还在排队。
// 一致性保证：通过记录每个分片完成时的 Binlog 位点，为后续从全量无缝切换到增量（Binlog）阶段打下基础。
// 容错处理：它支持 Flink 的 Checkpoint 机制，能够将切分进度和状态保存，确保作业失败重启后不重不漏。

public class MySqlSnapshotSplitAssigner implements MySqlSplitAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSnapshotSplitAssigner.class);
    // 记录已经完成切分并处理完毕的表 ID 列表。
    private final List<TableId> alreadyProcessedTables;
    // 已经切分好、但尚未分配给 TaskManager Reader 的分片
    private final List<MySqlSchemalessSnapshotSplit> remainingSplits;
    // 已经分配给 Reader 正在执行的分片映射表（Key 是分片 ID）
    private final Map<String, MySqlSchemalessSnapshotSplit> assignedSplits;
    // 缓存表的结构信息（TableChange）。分片本身不带 Schema 以减小状态大小，需要时从这里查找。
    private final Map<TableId, TableChanges.TableChange> tableSchemas;
    // 记录每个分片执行完毕后的 Binlog 偏移量。
    // 这是增量快照算法判断“高水位”的关键。
    private final Map<String, BinlogOffset> splitFinishedOffsets;
    // MySQL 数据源的配置信息（如用户名、密码、分片大小 chunkSize 等）
    private final MySqlSourceConfig sourceConfig;
    //作业的并行度
    private final int currentParallelism;
    // 待切分的表队列
    private final List<TableId> remainingTables;
    private final boolean isRemainingTablesCheckpointed;
    // Flink 提供的枚举器上下文，用于与 Flink 运行时交互。
    private final SplitEnumeratorContext<MySqlSplit> enumeratorContext;

    private final MySqlPartition partition;
    // 内部对象锁，用于同步异步切分线程与主分配逻辑之间的交互。
    private final Object lock = new Object();

    private volatile Throwable uncaughtSplitterException;
    //当前分配器的状态（如：正在分配全量、全量已完成、正在分配新增表等）。
    private AssignerStatus assignerStatus;
    // 实际执行 SQL 进入数据库查询主键范围并进行物理分片的工具类。
    private MySqlChunkSplitter chunkSplitter;
    private boolean isTableIdCaseSensitive;
    // 单线程线程池，用于异步执行切分任务。
    private ExecutorService executor;

    @Nullable private Long checkpointIdToFinish;

    public MySqlSnapshotSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this(
                sourceConfig,
                currentParallelism,
                new ArrayList<>(),
                new ArrayList<>(),
                new LinkedHashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                AssignerStatus.INITIAL_ASSIGNING,
                remainingTables,
                isTableIdCaseSensitive,
                true,
                ChunkSplitterState.NO_SPLITTING_TABLE_STATE,
                enumeratorContext);
    }

    public MySqlSnapshotSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            SnapshotPendingSplitsState checkpoint,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this(
                sourceConfig,
                currentParallelism,
                checkpoint.getAlreadyProcessedTables(),
                checkpoint.getRemainingSplits(),
                checkpoint.getAssignedSplits(),
                checkpoint.getTableSchemas(),
                checkpoint.getSplitFinishedOffsets(),
                checkpoint.getSnapshotAssignerStatus(),
                checkpoint.getRemainingTables(),
                checkpoint.isTableIdCaseSensitive(),
                checkpoint.isRemainingTablesCheckpointed(),
                checkpoint.getChunkSplitterState(),
                enumeratorContext);
    }

    private MySqlSnapshotSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> alreadyProcessedTables,
            List<MySqlSchemalessSnapshotSplit> remainingSplits,
            Map<String, MySqlSchemalessSnapshotSplit> assignedSplits,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            Map<String, BinlogOffset> splitFinishedOffsets,
            AssignerStatus assignerStatus,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            boolean isRemainingTablesCheckpointed,
            ChunkSplitterState chunkSplitterState,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this.sourceConfig = sourceConfig;
        this.currentParallelism = currentParallelism;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = new CopyOnWriteArrayList<>(remainingSplits);
        // When job restore from savepoint, sort the existing tables and newly added tables
        // to let enumerator only send newly added tables' BinlogSplitMetaEvent
        this.assignedSplits =
                assignedSplits.entrySet().stream()
                        .sorted(Entry.comparingByKey())
                        .collect(
                                Collectors.toMap(
                                        Entry::getKey,
                                        Entry::getValue,
                                        (o, o2) -> o,
                                        LinkedHashMap::new));
        this.tableSchemas = tableSchemas;
        this.splitFinishedOffsets = splitFinishedOffsets;
        this.assignerStatus = assignerStatus;
        this.remainingTables = new CopyOnWriteArrayList<>(remainingTables);
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.chunkSplitter =
                createChunkSplitter(sourceConfig, isTableIdCaseSensitive, chunkSplitterState);
        this.partition =
                new MySqlPartition(sourceConfig.getMySqlConnectorConfig().getLogicalName());
        this.enumeratorContext = enumeratorContext;
    }

    @Override
    public void open() {
        shouldEnterProcessingBacklog();
        chunkSplitter.open();
        discoveryCaptureTables();
        captureNewlyAddedTables();
        startAsynchronouslySplit();
    }
    // 核心职责是确定当前作业到底需要处理哪些表。
    // 它不仅在作业初次启动时运行，还在从检查点（Checkpoint）或保存点（Savepoint）恢复时起着关键的补偿作用。
    private void discoveryCaptureTables() {
        // discovery the tables lazily
        if (needToDiscoveryTables()) {
            long start = System.currentTimeMillis();
            LOG.debug("The remainingTables is empty, start to discovery tables");
            try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
                // 读取 MySQL 的元数据，并根据用户配置的 table.include.list（包含列表）或 table.exclude.list（排除列表）进行正则匹配，过滤出最终要同步的表。
                final List<TableId> discoverTables =
                        DebeziumUtils.discoverCapturedTables(jdbc, sourceConfig);
                this.remainingTables.addAll(discoverTables);
                this.isTableIdCaseSensitive = DebeziumUtils.isTableIdCaseSensitive(jdbc);
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to discovery tables to capture", e);
            }
            LOG.debug(
                    "Discovery tables success, time cost: {} ms.",
                    System.currentTimeMillis() - start);
        }
        // when restore the job from legacy savepoint, the legacy state may haven't snapshot
        // remaining tables, discovery remaining table here
        // 从旧版本或特定状态恢复的补偿逻辑
        // 如果恢复时发现状态中没有保存 remainingTables 列表（可能是由于版本升级或特定配置），且快照分配还没结束，就需要进入补偿模式。
        //
        else if (!isRemainingTablesCheckpointed
                && !AssignerStatus.isSnapshotAssigningFinished(assignerStatus)) {
            try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
                final List<TableId> discoverTables =
                        DebeziumUtils.discoverCapturedTables(jdbc, sourceConfig);
                // 排除已处理的表
                discoverTables.removeAll(alreadyProcessedTables);
                this.remainingTables.addAll(discoverTables);
                this.isTableIdCaseSensitive = DebeziumUtils.isTableIdCaseSensitive(jdbc);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover remaining tables to capture", e);
            }
        }
    }
    // flink CDC 实现**动态加表（Dynamic Table Discovery）**的核心方法。
    // 它允许作业在不停止的情况下，自动识别数据库中新增的表，并为其启动“增量快照”流程，同时清理掉不再需要同步的表。
    private void captureNewlyAddedTables() {
        // Don't scan newly added table in snapshot mode.
        // 用户是否开启了扫描新表的功能
        if (sourceConfig.isScanNewlyAddedTableEnabled()
                && !sourceConfig.getStartupOptions().isSnapshotOnly()
               // 只有当作业已经完成了初始的全量读取，进入到增量（Binlog）阶段后，才会触发这个检测逻辑。
                && AssignerStatus.isAssigningFinished(assignerStatus)) {
            // check whether we got newly added tables
            try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
                final List<TableId> currentCapturedTables =
                        DebeziumUtils.discoverCapturedTables(jdbc, sourceConfig);
                final Set<TableId> previousCapturedTables = new HashSet<>();
                // 汇总 Flink 状态中记录的所有表，形成“旧表清单”。
                List<TableId> tablesInRemainingSplits =
                        remainingSplits.stream()
                                .map(MySqlSnapshotSplit::getTableId)
                                .collect(Collectors.toList());
                previousCapturedTables.addAll(tablesInRemainingSplits);
                previousCapturedTables.addAll(alreadyProcessedTables);
                previousCapturedTables.addAll(remainingTables);

                // Get the removed tables with the new table filter
                Set<TableId> tablesToRemove = new HashSet<>(previousCapturedTables);
                tablesToRemove.removeAll(currentCapturedTables);

                // Get the newly added tables
                // 数据库里新出现的，且 Flink 状态里还没记录的表
                currentCapturedTables.removeAll(previousCapturedTables);
                List<TableId> newlyAddedTables = currentCapturedTables;

                // case 1: there are old tables to remove from state
                // 如果用户修改了配置，去掉了某些表的同步，这段逻辑会确保 Flink 的内存状态（assignedSplits、tableSchemas 等）被清理干净，避免资源浪费和潜在的报错
                if (!tablesToRemove.isEmpty()) {

                    // remove unassigned tables/splits if it does not satisfy new table filter
                    List<String> splitsToRemove = new LinkedList<>();
                    for (Entry<String, MySqlSchemalessSnapshotSplit> splitEntry :
                            assignedSplits.entrySet()) {
                        if (tablesToRemove.contains(splitEntry.getValue().getTableId())) {
                            splitsToRemove.add(splitEntry.getKey());
                        }
                    }
                    splitsToRemove.forEach(assignedSplits.keySet()::remove);
                    splitsToRemove.forEach(splitFinishedOffsets.keySet()::remove);
                    tableSchemas
                            .entrySet()
                            .removeIf(schema -> tablesToRemove.contains(schema.getKey()));
                    remainingSplits.removeIf(split -> tablesToRemove.contains(split.getTableId()));
                    LOG.info("Enumerator remove tables after restart: {}", tablesToRemove);
                    remainingTables.removeAll(tablesToRemove);
                    alreadyProcessedTables.removeIf(tableId -> tablesToRemove.contains(tableId));
                }

                // case 2: there are new tables to add
                // 状态机的切换。它会告诉 Enumerator，现在有一批新表需要像作业刚启动时那样，先进行一次“快照切分（Snapshot Splitting）”，
                // 读取存量数据，然后再合并到当前的 Binlog 流中。
                if (!newlyAddedTables.isEmpty()) {
                    // if job is still in snapshot reading phase, directly add all newly added
                    // tables
                    LOG.info("Found newly added tables, start capture newly added tables process");

                    // add new tables
                    remainingTables.addAll(newlyAddedTables);
                    if (AssignerStatus.isAssigningFinished(assignerStatus)) {
                        // start the newly added tables process under binlog reading phase
                        LOG.info(
                                "Found newly added tables, start capture newly added tables process under binlog reading phase");
                        this.startAssignNewlyAddedTables();
                    }
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover remaining tables to capture", e);
            }
        }
    }

    private void startAsynchronouslySplit() {
        if (chunkSplitter.hasNextChunk() || !remainingTables.isEmpty()) {
            if (executor == null) {
                ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat("snapshot-splitting").build();
                this.executor = Executors.newSingleThreadExecutor(threadFactory);
            }
            executor.submit(this::splitChunksForRemainingTables);
        }
    }
    // 针对一张指定的 MySQL 表，利用主键将其物理切分为多个分片（Chunks），并把这些分片放入待分配队列中。
    // 由于一张大表可能包含数亿行数据，一次性切分所有分片会产生巨大的内存压力和长事务。因此，chunkSplitter 采用分批切分的模式。
    private void splitTable(TableId nextTable) {
        LOG.info("Start splitting table {} into chunks...", nextTable);
        long start = System.currentTimeMillis();
        // 记录该表总共被切成了多少个分片
        int chunkNum = 0;
        // 标记位。由于同一张表的快照分片结构相同，只需记录一次 Schema 信息即可。
        boolean hasRecordSchema = false;
        // split the given table into chunks (snapshot splits)
        do {
            synchronized (lock) {
                List<MySqlSnapshotSplit> splits;
                try {
                    // 它会根据表的主键范围（PK Range）计算出一组切片。
                    // 例如，它会执行类似 SHOW MASTER STATUS 和主键采样，确定第一批切片的边界（如 ID 从 1 到 1000）。
                    splits = chunkSplitter.splitChunks(partition, nextTable);
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Error when splitting chunks for " + nextTable, e);
                }
                // 从第一个分片中提取表的结构（Schema）信息并存入全局的 tableSchemas 缓存中。
                // Flink CDC 的分片在传输过程中为了减小体积，通常是 Schemaless（无 Schema）的。
                // 但在分配器端必须保存一份完整的 Schema，以便后续分发给 TaskManager 时可以重新拼装。
                if (!hasRecordSchema && !splits.isEmpty()) {
                    hasRecordSchema = true;
                    final Map<TableId, TableChanges.TableChange> tableSchema = new HashMap<>();
                    tableSchema.putAll(splits.iterator().next().getTableSchemas());
                    tableSchemas.putAll(tableSchema);
                }
                // 转换并加入待分配队列
                for (MySqlSnapshotSplit split : splits) {
                    // 将分片对象转换为更轻量级的“无 Schema”版本，准备分发。
                    // isAssignUnboundedChunkFirst: 这是一个优化策略。如果一个分片没有结束边界（通常是最后一个分片），
                    // 根据配置决定是否将其插入队列的最前面（index 0），以便优先被消费
                    MySqlSchemalessSnapshotSplit schemalessSnapshotSplit =
                            split.toSchemalessSnapshotSplit();
                    //  优先分配无界分片
                    if (sourceConfig.isAssignUnboundedChunkFirst() && split.getSplitEnd() == null) {
                        // assign unbounded split first
                        remainingSplits.add(0, schemalessSnapshotSplit);
                    } else {
                        remainingSplits.add(schemalessSnapshotSplit);
                    }
                }

                chunkNum += splits.size();
                // 如果 chunkSplitter 已经遍历完了整张表的主键空间，说明该表切分完毕，从待切分表列表中移除。
                if (!chunkSplitter.hasNextChunk()) {
                    remainingTables.remove(nextTable);
                }
                lock.notify();
            }
        } while (chunkSplitter.hasNextChunk());
        long end = System.currentTimeMillis();
        LOG.info(
                "Split table {} into {} chunks, time cost: {}ms.",
                nextTable,
                chunkNum,
                end - start);
    }

    @Override
    public Optional<MySqlSplit> getNext() {
        waitTableDiscoveryReady();
        synchronized (lock) {
            checkSplitterErrors();
            if (!remainingSplits.isEmpty()) {
                // return remaining splits firstly
                Iterator<MySqlSchemalessSnapshotSplit> iterator = remainingSplits.iterator();
                MySqlSchemalessSnapshotSplit split = iterator.next();
                remainingSplits.remove(split);
                assignedSplits.put(split.splitId(), split);
                addAlreadyProcessedTablesIfNotExists(split.getTableId());
                return Optional.of(
                        split.toMySqlSnapshotSplit(tableSchemas.get(split.getTableId())));
            } else if (!remainingTables.isEmpty()) {
                try {
                    // wait for the asynchronous split to complete
                    lock.wait();
                } catch (InterruptedException e) {
                    throw new FlinkRuntimeException(
                            "InterruptedException while waiting for asynchronously snapshot split");
                }
                return getNext();
            } else {
                closeExecutorService();
                return Optional.empty();
            }
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return !allSnapshotSplitsFinished();
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        if (waitingForFinishedSplits()) {
            LOG.error(
                    "The assigner is not ready to offer finished split information, this should not be called");
            throw new FlinkRuntimeException(
                    "The assigner is not ready to offer finished split information, this should not be called");
        }
        final List<MySqlSchemalessSnapshotSplit> assignedSnapshotSplit =
                new ArrayList<>(assignedSplits.values());
        List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        for (MySqlSchemalessSnapshotSplit split : assignedSnapshotSplit) {
            BinlogOffset binlogOffset = splitFinishedOffsets.get(split.splitId());
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            binlogOffset));
        }
        return finishedSnapshotSplitInfos;
    }

    @Override
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        this.splitFinishedOffsets.putAll(splitFinishedOffsets);
        if (allSnapshotSplitsFinished()) {
            enumeratorContext.setIsProcessingBacklog(false);
            if (AssignerStatus.isAssigningSnapshotSplits(assignerStatus)) {
                // Skip the waiting checkpoint when current parallelism is 1 which means we do not
                // need
                // to care about the global output data order of snapshot splits and binlog split.
                if (currentParallelism == 1) {
                    assignerStatus = assignerStatus.onFinish();
                    LOG.info(
                            "Snapshot split assigner received all splits finished and the job parallelism is 1, snapshot split assigner is turn into finished status.");
                } else {
                    LOG.info(
                            "Snapshot split assigner received all splits finished, waiting for a complete checkpoint to mark the assigner finished.");
                }
            }
        }
    }

    @Override
    public void addSplits(Collection<MySqlSplit> splits) {
        for (MySqlSplit split : splits) {
            tableSchemas.putAll(split.asSnapshotSplit().getTableSchemas());
            remainingSplits.add(split.asSnapshotSplit().toSchemalessSnapshotSplit());
            // we should remove the add-backed splits from the assigned list,
            // because they are failed
            assignedSplits.remove(split.splitId());
            splitFinishedOffsets.remove(split.splitId());
        }
    }

    @Override
    public SnapshotPendingSplitsState snapshotState(long checkpointId) {
        SnapshotPendingSplitsState state =
                new SnapshotPendingSplitsState(
                        alreadyProcessedTables,
                        remainingSplits,
                        assignedSplits,
                        tableSchemas,
                        splitFinishedOffsets,
                        assignerStatus,
                        remainingTables,
                        isTableIdCaseSensitive,
                        true,
                        chunkSplitter.snapshotState(checkpointId));
        // we need a complete checkpoint before mark this assigner to be finished, to wait for
        // all records of snapshot splits are completely processed
        if (checkpointIdToFinish == null
                && AssignerStatus.isAssigningSnapshotSplits(assignerStatus)
                && allSnapshotSplitsFinished()) {
            checkpointIdToFinish = checkpointId;
        }
        return state;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // we have waited for at-least one complete checkpoint after all snapshot-splits are
        // finished, then we can mark snapshot assigner as finished.
        if (checkpointIdToFinish != null
                && AssignerStatus.isAssigningSnapshotSplits(assignerStatus)
                && allSnapshotSplitsFinished()) {
            if (checkpointId >= checkpointIdToFinish) {
                assignerStatus = assignerStatus.onFinish();
            }
            LOG.info("Snapshot split assigner is turn into finished status.");
        }
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return assignerStatus;
    }

    @Override
    public void startAssignNewlyAddedTables() {
        Preconditions.checkState(
                AssignerStatus.isAssigningFinished(assignerStatus),
                "Invalid assigner status %s",
                assignerStatus);
        assignerStatus = assignerStatus.startAssignNewlyTables();
    }

    @Override
    public void onBinlogSplitUpdated() {
        Preconditions.checkState(
                AssignerStatus.isNewlyAddedAssigningSnapshotFinished(assignerStatus),
                "Invalid assigner status %s",
                assignerStatus);
        assignerStatus = assignerStatus.onBinlogSplitUpdated();
    }

    @Override
    public void close() {
        closeExecutorService();
        if (chunkSplitter != null) {
            try {
                chunkSplitter.close();
                // clear jdbc connection pools
                JdbcConnectionPools.getInstance().clear();
            } catch (Exception e) {
                LOG.warn("Fail to close the chunk splitter.");
            }
        }
    }

    private void closeExecutorService() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    private void addAlreadyProcessedTablesIfNotExists(TableId tableId) {
        if (!alreadyProcessedTables.contains(tableId)) {
            alreadyProcessedTables.add(tableId);
        }
    }

    private void waitTableDiscoveryReady() {
        while (needToDiscoveryTables()) {
            LOG.debug("Current assigner is discovering tables, wait tables ready...");
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                // nothing to do
            }
        }
    }

    /** Indicates there is no more splits available in this assigner. */
    public boolean noMoreSplits() {
        return !needToDiscoveryTables() && remainingTables.isEmpty() && remainingSplits.isEmpty();
    }

    /** Indicates current assigner need to discovery tables or not. */
    // 通常在 remainingTables（待处理表）、remainingSplits（待分配分片）和 alreadyProcessedTables（已处理表）都为空时返回 true。这标志着作业刚刚开始。
    public boolean needToDiscoveryTables() {
        return remainingTables.isEmpty()
                && remainingSplits.isEmpty()
                && alreadyProcessedTables.isEmpty();
    }

    public Map<String, MySqlSchemalessSnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<TableId, TableChanges.TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public Map<String, BinlogOffset> getSplitFinishedOffsets() {
        return splitFinishedOffsets;
    }

    // -------------------------------------------------------------------------------------------

    /**
     * Returns whether all splits are finished which means no more splits and all assigned splits
     * are finished.
     */
    private boolean allSnapshotSplitsFinished() {
        return noMoreSplits() && assignedSplits.size() == splitFinishedOffsets.size();
    }
    // 在后台线程池中运行，负责最耗时的任务：扫描 MySQL 表的主键范围，并将大表切分成一个个小的分片（Chunks/Splits）。
    private void splitChunksForRemainingTables() {
        try {
            // restore from a checkpoint and start to split the table from the previous
            // checkpoint
            // 如果作业是从 Checkpoint 恢复的，可能当时某张大表只切分了一半。chunkSplitter.hasNextChunk() 会检查分切器内部是否还保存着上一次未完成的切分状态。
            // 如果是，则立即调用 splitTable 继续处理这张表，确保切分逻辑的连续性。
            if (chunkSplitter.hasNextChunk()) {
                LOG.info(
                        "Start splitting remaining chunks for table {}",
                        chunkSplitter.getCurrentSplittingTableId());
                splitTable(chunkSplitter.getCurrentSplittingTableId());
            }

            // split the remaining tables
            // 依次处理所有排队中的表
            for (TableId nextTable : remainingTables) {
                splitTable(nextTable);
            }
        } catch (Throwable e) {
            synchronized (lock) {
                if (uncaughtSplitterException == null) {
                    uncaughtSplitterException = e;
                } else {
                    uncaughtSplitterException.addSuppressed(e);
                }
                // Release the potential waiting getNext() call
                lock.notify();
            }
        }
    }

    private void checkSplitterErrors() {
        if (uncaughtSplitterException != null) {
            throw new FlinkRuntimeException(
                    "Chunk splitting has encountered exception", uncaughtSplitterException);
        }
    }

    private static MySqlChunkSplitter createChunkSplitter(
            MySqlSourceConfig sourceConfig,
            boolean isTableIdCaseSensitive,
            ChunkSplitterState chunkSplitterState) {
        MySqlSchema mySqlSchema = new MySqlSchema(sourceConfig, isTableIdCaseSensitive);
        if (!ChunkSplitterState.NO_SPLITTING_TABLE_STATE.equals(chunkSplitterState)) {
            TableId tableId = chunkSplitterState.getCurrentSplittingTableId();
            return new MySqlChunkSplitter(
                    mySqlSchema,
                    sourceConfig,
                    tableId != null && sourceConfig.getTableFilter().test(tableId)
                            ? chunkSplitterState
                            : ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        }
        return new MySqlChunkSplitter(mySqlSchema, sourceConfig);
    }

    private void shouldEnterProcessingBacklog() {
        if (assignerStatus == AssignerStatus.INITIAL_ASSIGNING) {
            enumeratorContext.setIsProcessingBacklog(true);
        }
    }
}
