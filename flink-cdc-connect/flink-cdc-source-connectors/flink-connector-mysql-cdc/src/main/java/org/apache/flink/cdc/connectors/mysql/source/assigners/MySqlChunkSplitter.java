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

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlSchema;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlTypeUtils;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.math.BigDecimal.ROUND_CEILING;

/** The {@link ChunkSplitter} implementation for MySQL. */
// MySqlChunkSplitter 是 Flink CDC 中针对 MySQL 的 ChunkSplitter 接口的具体实现。
// 它的核心任务是根据表的主键或指定的列，将 MySQL 表的数据划分为多个分片（Chunks），以便在快照阶段进行高效、无锁的并发读取。
// 均匀分片优化：如果主键是连续增长的数字（如 BIGINT 自增 ID），它会通过数学计算直接得出所有分片范围，效率极高。
// 不均匀分片降级：如果主键分布不均（如存在大量空洞），它会通过 SELECT 查询动态寻找下一个分片的边界。
// 状态恢复：配合 Flink Checkpoint，它可以记录当前切分到哪张表、哪个位置，保证作业重启后不重不漏。
// 资源管理：负责建立与 MySQL 的 JDBC 连接，并分析表结构、获取主键范围和行数估算。
public class MySqlChunkSplitter implements ChunkSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlChunkSplitter.class);
    private final Object lock = new Object();
    // 存储 MySQL 连接信息、分片大小（split-size）、分布因子上限等配置。
    private final MySqlSourceConfig sourceConfig;
    // 用于获取 MySQL 的表结构映射信息
    private final MySqlSchema mySqlSchema;
    // 记录当前正在切分的表。如果为 null，表示当前没有正在切分的表。
    @Nullable private TableId currentSplittingTableId;
    // 下一个分片的起始边界（对应数据库中的主键值）
    @Nullable private ChunkSplitterState.ChunkBound nextChunkStart;
    // 记录当前表切分出的分片序号（从 0 开始自增）
    @Nullable private Integer nextChunkId;
    // 用于执行 SQL 查询（如查询 Min/Max 值、估算行数、探测分片边界）
    private JdbcConnection jdbcConnection;
    // Debezium 格式的表对象，包含列定义
    private Table currentSplittingTable;
    // 实际用于分片的列（通常是主键）
    private Column splitColumn;
    // 分片列的逻辑类型（如 BIGINT）
    private RowType splitType;
    // 存储分片列的当前最小值和最大值（数组长度为 2）
    private Object[] minMaxOfSplitColumn;
    // 通过 SHOW TABLE STATUS 等方式获取的表估算行数，用于计算分布因子。
    private long approximateRowCnt;

    public MySqlChunkSplitter(MySqlSchema mySqlSchema, MySqlSourceConfig sourceConfig) {
        this(mySqlSchema, sourceConfig, null, null, null);
    }

    public MySqlChunkSplitter(
            MySqlSchema mySqlSchema,
            MySqlSourceConfig sourceConfig,
            ChunkSplitterState chunkSplitterState) {
        this(
                mySqlSchema,
                sourceConfig,
                chunkSplitterState.getCurrentSplittingTableId(),
                chunkSplitterState.getNextChunkStart(),
                chunkSplitterState.getNextChunkId());
    }

    private MySqlChunkSplitter(
            MySqlSchema mySqlSchema,
            MySqlSourceConfig sourceConfig,
            @Nullable TableId currentSplittingTableId,
            @Nullable ChunkSplitterState.ChunkBound nextChunkStart,
            @Nullable Integer nextChunkId) {
        this.mySqlSchema = mySqlSchema;
        this.sourceConfig = sourceConfig;
        this.currentSplittingTableId = currentSplittingTableId;
        this.nextChunkStart = nextChunkStart;
        this.nextChunkId = nextChunkId;
    }

    @Override
    public void open() {
        this.jdbcConnection = DebeziumUtils.openJdbcConnection(sourceConfig);
    }
    // MySqlChunkSplitter 的逻辑枢纽，它决定了对一张表是采用 “一次性全量均匀切分” 还是 “逐个动态非均匀切分”。
    @Override
    public List<MySqlSnapshotSplit> splitChunks(MySqlPartition partition, TableId tableId)
            throws Exception {
        // 检查当前是否正在切分某个表。
        if (!hasNextChunk()) {
            // 连接数据库，获取该表的 Min/Max 值、主键列、类型及估算行数（为切分做数据准备）。
            analyzeTable(partition, tableId);
            // 计算主键的分布密度。如果主键是连续的数字（如自增 ID），它会利用数学计算直接生成所有分片。
            Optional<List<MySqlSnapshotSplit>> evenlySplitChunks =
                    trySplitAllEvenlySizedChunks(partition, tableId);
            if (evenlySplitChunks.isPresent()) {
                return evenlySplitChunks.get();
            } else {
             // 如果数据分布不均（有大量空洞），则进入增量动态切分模式
                synchronized (lock) {
                    this.currentSplittingTableId = tableId;
                    this.nextChunkStart = ChunkSplitterState.ChunkBound.START_BOUND;
                    this.nextChunkId = 0;
                    // 通过查询数据库，找到当前起始点之后的第 N 条数据作为边界，仅切出一个分片并返回。
                    return Collections.singletonList(
                            splitOneUnevenlySizedChunk(partition, tableId));
                }
            }
        } else {
            Preconditions.checkState(
                    currentSplittingTableId.equals(tableId),
                    "Can not split a new table before the previous table splitting finish.");
            // 这种情况通常发生在从 Checkpoint 恢复后。此时虽然知道在切哪张表，但内存中的表结构信息丢了，需要重新调用 analyzeTable 加载元数据
            if (currentSplittingTable == null) {
                analyzeTable(partition, currentSplittingTableId);
            }
            synchronized (lock) {
                return Collections.singletonList(splitOneUnevenlySizedChunk(partition, tableId));
            }
        }
    }

    /** Analyze the meta information for given table. */
    // 主要作用是在对一张大表进行物理分片（Chunking）之前，先通过 JDBC 连接到数据库，获取该表的元数据（Metadata）、分片参考列以及数据的分布统计信息。
    private void analyzeTable(MySqlPartition partition, TableId tableId) {
        try {
            // 获取目标表的 Debezium Table 对象
            currentSplittingTable =
                    mySqlSchema.getTableSchema(partition, jdbcConnection, tableId).getTable();
            // 确定哪一列将作为分片的依据（通常是主键）
            // 首先检查用户是否在 sourceConfig 中手动指定了分片列（Chunk Key）
            // 如果没有指定，会自动寻找表的主键（Primary Key）
            // 如果是复合主键，默认选取第一列。这一列的值将决定数据切分的边界。
            splitColumn =
                    ChunkUtils.getChunkKeyColumn(
                            currentSplittingTable, sourceConfig.getChunkKeyColumns());
            // 将数据库的物理类型转换为 Flink 内部的 RowType（逻辑类型）
            splitType =
                    ChunkUtils.getChunkKeyColumnType(
                            splitColumn, sourceConfig.isTreatTinyInt1AsBoolean());
            // 查询分片列的最大最小值 (Min/Max Query)
            minMaxOfSplitColumn =
                    StatementUtils.queryMinMax(jdbcConnection, tableId, splitColumn.name());
            // 获取表的近似总记录数。
            approximateRowCnt = StatementUtils.queryApproximateRowCnt(jdbcConnection, tableId);
        } catch (Exception e) {
            throw new RuntimeException("Fail to analyze table in chunk splitter.", e);
        }
    }

    /** Generates one snapshot split (chunk) for the give table path. */
    // 当系统判断表的主键分布不均匀（存在巨大空洞）时，会调用此方法，通过逐个探测边界的方式来切分数据。
    private MySqlSnapshotSplit splitOneUnevenlySizedChunk(MySqlPartition partition, TableId tableId)
            throws SQLException {
        // 获取配置的分片大小（即每个分片包含多少行数据，默认 8096）
        final int chunkSize = sourceConfig.getSplitSize();
        // 获取当前待切分块的起始边界值。如果是第一次切分，则为 null
        final Object chunkStartVal = nextChunkStart.getValue();
        LOG.info(
                "Use unevenly-sized chunks for table {}, the chunk size is {} from {}",
                tableId,
                chunkSize,
                nextChunkStart == ChunkSplitterState.ChunkBound.START_BOUND
                        ? "null"
                        : chunkStartVal.toString());
        // we start from [null, min + chunk_size) and avoid [null, min)
        // 由于数据分布不均，不能简单用加法计算终点。该方法内部会执行 SQL（通常是 SELECT ... FROM ... WHERE id > current_start ORDER BY id LIMIT 1 OFFSET chunkSize-1），
        // 在数据库中实地寻找第 chunkSize 条记录的主键值。
        Object chunkEnd =
                nextChunkEnd(
                        jdbcConnection,
                        nextChunkStart == ChunkSplitterState.ChunkBound.START_BOUND
                                ? minMaxOfSplitColumn[0]
                                : chunkStartVal,
                        tableId,
                        splitColumn.name(),
                        minMaxOfSplitColumn[1],
                        chunkSize);
        // may sleep a while to avoid DDOS on MySQL server
        maySleep(nextChunkId, tableId);
        if (chunkEnd != null && ObjectUtils.compare(chunkEnd, minMaxOfSplitColumn[1]) <= 0) {
            nextChunkStart = ChunkSplitterState.ChunkBound.middleOf(chunkEnd);
            return createSnapshotSplit(
                    jdbcConnection,
                    partition,
                    tableId,
                    nextChunkId++,
                    splitType,
                    chunkStartVal,
                    chunkEnd);
        } else {
            currentSplittingTableId = null;
            nextChunkStart = ChunkSplitterState.ChunkBound.END_BOUND;
            return createSnapshotSplit(
                    jdbcConnection,
                    partition,
                    tableId,
                    nextChunkId++,
                    splitType,
                    chunkStartVal,
                    null);
        }
    }

    /**
     * Try to split all chunks for evenly-sized table, or else return empty.
     *
     * <p>We can use evenly-sized chunks or unevenly-sized chunks when split table into chunks,
     * using evenly-sized chunks which is much efficient, using unevenly-sized chunks which will
     * request many queries and is not efficient.
     */
    // 目的是判断一张表是否可以通过数学计算（步长推导）来一次性完成全部分片，而不需要频繁地查询数据库。
    private Optional<List<MySqlSnapshotSplit>> trySplitAllEvenlySizedChunks(
            MySqlPartition partition, TableId tableId) {
        LOG.debug("Try evenly splitting table {} into chunks", tableId);
        final Object min = minMaxOfSplitColumn[0];
        final Object max = minMaxOfSplitColumn[1];
        // 如果表里没有数据（min/max 为空），或者只有一行数据（min 等于 max）
        // 不需要复杂的切分逻辑，直接将整张表作为一个“全表扫描分片”（ChunkRange.all()）返回。
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Optional.of(
                    generateSplits(
                            partition, tableId, Collections.singletonList(ChunkRange.all())));
        }

        final int chunkSize = sourceConfig.getSplitSize();
        // 调用 getDynamicChunkSize 来判断数据分布是否足够均匀。
        final int dynamicChunkSize =
                getDynamicChunkSize(tableId, splitColumn, min, max, chunkSize, approximateRowCnt);
        if (dynamicChunkSize != -1) {
            LOG.debug("finish evenly splitting table {} into chunks", tableId);
            // 执行“均匀分片”优化
            List<ChunkRange> chunks =
                    splitEvenlySizedChunks(
                            tableId, min, max, approximateRowCnt, chunkSize, dynamicChunkSize);
            return Optional.of(generateSplits(partition, tableId, chunks));
        } else {
            // 主键分布极不均匀（例如主键是随机 UUID 或有巨大的 ID 空洞）
            LOG.debug("beginning unevenly splitting table {} into chunks", tableId);
            return Optional.empty();
        }
    }

    /** Generates all snapshot splits (chunks) from chunk ranges. */
    private List<MySqlSnapshotSplit> generateSplits(
            MySqlPartition partition, TableId tableId, List<ChunkRange> chunks) {
        // convert chunks into splits
        List<MySqlSnapshotSplit> splits = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            ChunkRange chunk = chunks.get(i);
            MySqlSnapshotSplit split =
                    createSnapshotSplit(
                            jdbcConnection,
                            partition,
                            tableId,
                            i,
                            splitType,
                            chunk.getChunkStart(),
                            chunk.getChunkEnd());
            splits.add(split);
        }
        return splits;
    }

    @Override
    public boolean hasNextChunk() {
        return currentSplittingTableId != null;
    }

    @Override
    public ChunkSplitterState snapshotState(long checkpointId) {
        synchronized (lock) {
            return new ChunkSplitterState(currentSplittingTableId, nextChunkStart, nextChunkId);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }

    /**
     * Split table into evenly sized chunks based on the numeric min and max value of split column,
     * and tumble chunks in step size.
     */
    @VisibleForTesting
    public List<ChunkRange> splitEvenlySizedChunks(
            TableId tableId,
            Object min,
            Object max,
            long approximateRowCnt,
            int chunkSize,
            int dynamicChunkSize) {
        LOG.info(
                "Use evenly-sized chunk optimization for table {}, the approximate row count is {}, the chunk size is {}, the dynamic chunk size is {}",
                tableId,
                approximateRowCnt,
                chunkSize,
                dynamicChunkSize);
        if (approximateRowCnt <= chunkSize) {
            // there is no more than one chunk, return full table as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = ObjectUtils.plus(min, dynamicChunkSize);
        while (ObjectUtils.compare(chunkEnd, max) <= 0) {
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            chunkStart = chunkEnd;
            try {
                chunkEnd = ObjectUtils.plus(chunkEnd, dynamicChunkSize);
            } catch (ArithmeticException e) {
                // Stop chunk split to avoid dead loop when number overflows.
                break;
            }
        }

        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    private Object nextChunkEnd(
            JdbcConnection jdbc,
            Object previousChunkEnd,
            TableId tableId,
            String splitColumnName,
            Object max,
            int chunkSize)
            throws SQLException {
        // chunk end might be null when max values are removed
        Object chunkEnd =
                StatementUtils.queryNextChunkMax(
                        jdbc, tableId, splitColumnName, chunkSize, previousChunkEnd);
        if (Objects.equals(previousChunkEnd, chunkEnd)) {
            // we don't allow equal chunk start and end,
            // should query the next one larger than chunkEnd
            chunkEnd = StatementUtils.queryMin(jdbc, tableId, splitColumnName, chunkEnd);

            // queryMin will return null when the chunkEnd is the max value,
            // this will happen when the mysql table ignores the capitalization.
            // see more detail at the test MySqlConnectorITCase#testReadingWithMultiMaxValue.
            // In the test, the max value of order_id will return 'e' and when we get the chunkEnd =
            // 'E',
            // this method will return 'E' and will not return null.
            // When this method is invoked next time, queryMin will return null here.
            // So we need return null when we reach the max value here.
            if (chunkEnd == null) {
                return null;
            }
        }
        if (ObjectUtils.compare(chunkEnd, max) >= 0) {
            return null;
        } else {
            return chunkEnd;
        }
    }

    private MySqlSnapshotSplit createSnapshotSplit(
            JdbcConnection jdbc,
            MySqlPartition partition,
            TableId tableId,
            int chunkId,
            RowType splitKeyType,
            Object chunkStart,
            Object chunkEnd) {
        // currently, we only support single split column
        Object[] splitStart = chunkStart == null ? null : new Object[] {chunkStart};
        Object[] splitEnd = chunkEnd == null ? null : new Object[] {chunkEnd};
        Map<TableId, TableChange> schema = new HashMap<>();
        schema.put(tableId, mySqlSchema.getTableSchema(partition, jdbc, tableId));
        return new MySqlSnapshotSplit(
                tableId, chunkId, splitKeyType, splitStart, splitEnd, null, schema);
    }

    // ------------------------------------------------------------------------------------------

    /**
     * Checks whether split column is evenly distributed across its range and return the
     * dynamicChunkSize. If the split column is not evenly distributed, return -1.
     */
    private int getDynamicChunkSize(
            TableId tableId,
            Column splitColumn,
            Object min,
            Object max,
            int chunkSize,
            long approximateRowCnt) {
        if (!isEvenlySplitColumn(splitColumn, sourceConfig.isTreatTinyInt1AsBoolean())) {
            return -1;
        }
        final double distributionFactorUpper = sourceConfig.getDistributionFactorUpper();
        final double distributionFactorLower = sourceConfig.getDistributionFactorLower();
        double distributionFactor =
                calculateDistributionFactor(tableId, min, max, approximateRowCnt);
        boolean dataIsEvenlyDistributed =
                ObjectUtils.doubleCompare(distributionFactor, distributionFactorLower) >= 0
                        && ObjectUtils.doubleCompare(distributionFactor, distributionFactorUpper)
                                <= 0;
        LOG.info(
                "The actual distribution factor for table {} is {}, the lower bound of evenly distribution factor is {}, the upper bound of evenly distribution factor is {}",
                tableId,
                distributionFactor,
                distributionFactorLower,
                distributionFactorUpper);
        if (dataIsEvenlyDistributed) {
            // the minimum dynamic chunk size is at least 1
            return Math.max((int) (distributionFactor * chunkSize), 1);
        }
        return -1;
    }

    /** Checks whether split column is evenly distributed across its range. */
    private static boolean isEvenlySplitColumn(Column splitColumn, boolean tinyInt1isBit) {
        DataType flinkType = MySqlTypeUtils.fromDbzColumn(splitColumn, tinyInt1isBit);
        LogicalTypeRoot typeRoot = flinkType.getLogicalType().getTypeRoot();

        // currently, we only support the optimization that split column with type BIGINT, INT,
        // DECIMAL
        return typeRoot == LogicalTypeRoot.BIGINT
                || typeRoot == LogicalTypeRoot.INTEGER
                || typeRoot == LogicalTypeRoot.DECIMAL;
    }

    /** Returns the distribution factor of the given table. */
    private double calculateDistributionFactor(
            TableId tableId, Object min, Object max, long approximateRowCnt) {

        if (!min.getClass().equals(max.getClass())) {
            throw new IllegalStateException(
                    String.format(
                            "Unsupported operation type, the MIN value type %s is different with MAX value type %s.",
                            min.getClass().getSimpleName(), max.getClass().getSimpleName()));
        }
        if (approximateRowCnt == 0) {
            return Double.MAX_VALUE;
        }
        BigDecimal difference = ObjectUtils.minus(max, min);
        // factor = (max - min + 1) / rowCount
        final BigDecimal subRowCnt = difference.add(BigDecimal.valueOf(1));
        double distributionFactor =
                subRowCnt.divide(new BigDecimal(approximateRowCnt), 4, ROUND_CEILING).doubleValue();
        LOG.info(
                "The distribution factor of table {} is {} according to the min split key {}, max split key {} and approximate row count {}",
                tableId,
                distributionFactor,
                min,
                max,
                approximateRowCnt);
        return distributionFactor;
    }

    private static void maySleep(int count, TableId tableId) {
        // every 10 queries to sleep 0.1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
            LOG.info("ChunkSplitter has split {} chunks for table {}", count, tableId);
        }
    }

    public TableId getCurrentSplittingTableId() {
        return currentSplittingTableId;
    }

    public Integer getNextChunkId() {
        return nextChunkId;
    }

    @Override
    public void close() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        mySqlSchema.close();
    }
}
