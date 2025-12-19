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

package org.apache.flink.cdc.connectors.mysql.source.split;

import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** The split to describe the binlog of MySql table(s). */
// MySqlBinlogSplit 是 Flink CDC MySQL 连接器中负责增量日志（Binlog）读取阶段的核心分片类。
// 在 Flink CDC 的“并行增量快照算法”中，作业会先通过多个 MySqlSnapshotSplit 读取存量数据，最后通过一个 MySqlBinlogSplit 来承接所有的增量变更。
// 这个类的主要作用是描述如何读取以及从哪里开始读取 MySQL Binlog，它是实现全量与增量无缝衔接的关键：
// 定义起止位点：记录从 Binlog 的哪个位置（Offset）开始读，读到哪里结束。
// 维护一致性信息：它持有了所有已完成的快照分片信息（finishedSnapshotSplitInfos）。这对于算法判断哪些 Binlog 记录需要被过滤、哪些需要被应用至关重要。
// 支持动态加表：通过 isSuspended（挂起）状态和水位线推进逻辑，支持在作业运行过程中动态添加新的表而不丢失数据。
// 承载表结构：保存读取 Binlog 时所需的最新表结构（Schema）。

public class MySqlBinlogSplit extends MySqlSplit {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplit.class);
    private static final int TABLES_LENGTH_FOR_LOG = 3;
    // 读取起始位点。
    // 如果是从全量切换过来的，这通常是所有快照分片中最小的高水位位点。
    private final BinlogOffset startingOffset;
    // 读取结束位点。
    // 对于流式作业，通常为 null（表示永不停止）；但在某些有限流处理中会指定。
    private final BinlogOffset endingOffset;
    // 已完成快照信息列表。
    // 记录了每个快照分片的范围及其对应的高水位。用于 Binlog 与快照数据的去重和对齐。
    private final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos;
    // 表结构映射。
    // 存储 TableId 到 TableChange 的映射，用于解析二进制 Binlog 数据。
    private final Map<TableId, TableChange> tableSchemas;
    // 预期完成的分片总数。
    // 用于验证是否所有快照分片都已汇报完毕。
    private final int totalFinishedSplitSize;
    // 挂起状态标识。
    // 为 true 时，表示当前 Binlog 读取暂时停止（通常发生在动态增加表时，需要等待新表的快照完成）。
    private final boolean isSuspended;
    // 日志辅助属性。
    // 预格式化好的表名字符串，用于在日志打印时只显示前几个表名，防止表太多撑爆日志。
    private final String tablesForLog;
    // 序列化缓存。
    // 提高分片在节点间传输时的性能。
    @Nullable transient byte[] serializedFormCache;

    public MySqlBinlogSplit(
            String splitId,
            BinlogOffset startingOffset,
            BinlogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.isSuspended = isSuspended;
        this.tablesForLog = getTablesForLog();
    }

    public MySqlBinlogSplit(
            String splitId,
            BinlogOffset startingOffset,
            BinlogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.isSuspended = false;
        this.tablesForLog = getTablesForLog();
    }

    public BinlogOffset getStartingOffset() {
        return startingOffset;
    }

    public BinlogOffset getEndingOffset() {
        return endingOffset;
    }

    public List<FinishedSnapshotSplitInfo> getFinishedSnapshotSplitInfos() {
        return finishedSnapshotSplitInfos;
    }

    @Override
    public Map<TableId, TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public int getTotalFinishedSplitSize() {
        return totalFinishedSplitSize;
    }

    public boolean isSuspended() {
        return isSuspended;
    }

    public boolean isCompletedSplit() {
        return totalFinishedSplitSize == finishedSnapshotSplitInfos.size();
    }

    private String getTablesForLog() {
        List<TableId> tablesForLog = new ArrayList<>();
        if (tableSchemas != null) {
            List<TableId> tableIds = new ArrayList<>(new TreeSet(tableSchemas.keySet()));
            // Truncate tables length to avoid printing too much log
            tablesForLog = tableIds.subList(0, Math.min(tableIds.size(), TABLES_LENGTH_FOR_LOG));
        }
        return tablesForLog.toString();
    }

    public String getTables() {
        return tablesForLog;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlBinlogSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MySqlBinlogSplit that = (MySqlBinlogSplit) o;
        return totalFinishedSplitSize == that.totalFinishedSplitSize
                && isSuspended == that.isSuspended
                && Objects.equals(startingOffset, that.startingOffset)
                && Objects.equals(endingOffset, that.endingOffset)
                && Objects.equals(finishedSnapshotSplitInfos, that.finishedSnapshotSplitInfos)
                && Objects.equals(tableSchemas, that.tableSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                totalFinishedSplitSize,
                isSuspended);
    }

    @Override
    public String toString() {
        return "MySqlBinlogSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", tables="
                + tablesForLog
                + ", offset="
                + startingOffset
                + ", endOffset="
                + endingOffset
                + ", isSuspended="
                + isSuspended
                + '}';
    }

    // -------------------------------------------------------------------
    // factory utils to build new MySqlBinlogSplit instance
    // -------------------------------------------------------------------
    // 向 Binlog 分片中追加新完成的快照信息。
    public static MySqlBinlogSplit appendFinishedSplitInfos(
            MySqlBinlogSplit binlogSplit, List<FinishedSnapshotSplitInfo> splitInfos) {
        // re-calculate the starting binlog offset after the new table added
        BinlogOffset startingOffset = binlogSplit.getStartingOffset();
        for (FinishedSnapshotSplitInfo splitInfo : splitInfos) {
            if (splitInfo.getHighWatermark().isBefore(startingOffset)) {
                startingOffset = splitInfo.getHighWatermark();
            }
        }
        splitInfos.addAll(binlogSplit.getFinishedSnapshotSplitInfos());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                startingOffset,
                binlogSplit.getEndingOffset(),
                splitInfos,
                binlogSplit.getTableSchemas(),
                binlogSplit.getTotalFinishedSplitSize(),
                binlogSplit.isSuspended());
    }

    /**
     * Filter out the outdated finished splits in {@link MySqlBinlogSplit}.
     *
     * <p>When restore from a checkpoint, the finished split infos may contain some splits from the
     * deleted tables. We need to remove these splits from the total finished split infos and update
     * the size, while also removing the outdated tables from the table schemas of binlog split.
     */
    // 过滤掉已删除表的陈旧信息。
    // 当用户修改配置删除了某些表并从 Checkpoint 重启时，此方法负责清理分片内不再需要的表结构和快照信息。
    public static MySqlBinlogSplit filterOutdatedSplitInfos(
            MySqlBinlogSplit binlogSplit, Tables.TableFilter currentTableFilter) {
        Map<TableId, TableChange> filteredTableSchemas =
                binlogSplit.getTableSchemas().entrySet().stream()
                        .filter(entry -> currentTableFilter.isIncluded(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Set<TableId> tablesToRemoveInFinishedSnapshotSplitInfos =
                binlogSplit.getFinishedSnapshotSplitInfos().stream()
                        .filter(i -> !currentTableFilter.isIncluded(i.getTableId()))
                        .map(split -> split.getTableId())
                        .collect(Collectors.toSet());
        if (tablesToRemoveInFinishedSnapshotSplitInfos.isEmpty()) {
            return new MySqlBinlogSplit(
                    binlogSplit.splitId,
                    binlogSplit.getStartingOffset(),
                    binlogSplit.getEndingOffset(),
                    binlogSplit.getFinishedSnapshotSplitInfos(),
                    filteredTableSchemas,
                    binlogSplit.totalFinishedSplitSize,
                    binlogSplit.isSuspended());
        }

        LOG.info(
                "Reader remove tables after restart: {}",
                tablesToRemoveInFinishedSnapshotSplitInfos);
        List<FinishedSnapshotSplitInfo> allFinishedSnapshotSplitInfos =
                binlogSplit.getFinishedSnapshotSplitInfos().stream()
                        .filter(
                                i ->
                                        !tablesToRemoveInFinishedSnapshotSplitInfos.contains(
                                                i.getTableId()))
                        .collect(Collectors.toList());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                binlogSplit.getStartingOffset(),
                binlogSplit.getEndingOffset(),
                allFinishedSnapshotSplitInfos,
                filteredTableSchemas,
                binlogSplit.getTotalFinishedSplitSize()
                        - (binlogSplit.getFinishedSnapshotSplitInfos().size()
                                - allFinishedSnapshotSplitInfos.size()),
                binlogSplit.isSuspended());
    }
    // 填充或更新表结构信息。
    public static MySqlBinlogSplit fillTableSchemas(
            MySqlBinlogSplit binlogSplit, Map<TableId, TableChange> tableSchemas) {
        tableSchemas.putAll(binlogSplit.getTableSchemas());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                binlogSplit.getStartingOffset(),
                binlogSplit.getEndingOffset(),
                binlogSplit.getFinishedSnapshotSplitInfos(),
                tableSchemas,
                binlogSplit.getTotalFinishedSplitSize(),
                binlogSplit.isSuspended());
    }
    // 实现 Binlog 读取状态的挂起与恢复。
    public static MySqlBinlogSplit toNormalBinlogSplit(
            MySqlBinlogSplit suspendedBinlogSplit, int totalFinishedSplitSize) {
        return new MySqlBinlogSplit(
                suspendedBinlogSplit.splitId,
                suspendedBinlogSplit.getStartingOffset(),
                suspendedBinlogSplit.getEndingOffset(),
                suspendedBinlogSplit.getFinishedSnapshotSplitInfos(),
                suspendedBinlogSplit.getTableSchemas(),
                totalFinishedSplitSize,
                false);
    }

    public static MySqlBinlogSplit toSuspendedBinlogSplit(MySqlBinlogSplit normalBinlogSplit) {
        return new MySqlBinlogSplit(
                normalBinlogSplit.splitId,
                normalBinlogSplit.getStartingOffset(),
                normalBinlogSplit.getEndingOffset(),
                forwardHighWatermarkToStartingOffset(
                        normalBinlogSplit.getFinishedSnapshotSplitInfos(),
                        normalBinlogSplit.getStartingOffset()),
                normalBinlogSplit.getTableSchemas(),
                normalBinlogSplit.getTotalFinishedSplitSize(),
                true);
    }

    /**
     * Forwards {@link FinishedSnapshotSplitInfo#getHighWatermark()} to current binlog reading
     * offset for these snapshot-splits have started the binlog reading, this is pretty useful for
     * newly added table process that we can continue to consume binlog for these splits from the
     * updated high watermark.
     *
     * @param existedSplitInfos
     * @param currentBinlogReadingOffset
     */
    // 推进水位线。
    // 在动态加表场景下，将已有的快照分片的高水位推进到当前的 Binlog 读取位置。这样可以防止作业在恢复后，重复去读那些已经被处理过的陈旧 Binlog。
    private static List<FinishedSnapshotSplitInfo> forwardHighWatermarkToStartingOffset(
            List<FinishedSnapshotSplitInfo> existedSplitInfos,
            BinlogOffset currentBinlogReadingOffset) {
        List<FinishedSnapshotSplitInfo> updatedSnapshotSplitInfos = new ArrayList<>();
        for (FinishedSnapshotSplitInfo existedSplitInfo : existedSplitInfos) {
            // for split has started read binlog, forward its high watermark to current binlog
            // reading offset
            if (existedSplitInfo.getHighWatermark().isBefore(currentBinlogReadingOffset)) {
                FinishedSnapshotSplitInfo forwardHighWatermarkSnapshotSplitInfo =
                        new FinishedSnapshotSplitInfo(
                                existedSplitInfo.getTableId(),
                                existedSplitInfo.getSplitId(),
                                existedSplitInfo.getSplitStart(),
                                existedSplitInfo.getSplitEnd(),
                                currentBinlogReadingOffset);
                updatedSnapshotSplitInfos.add(forwardHighWatermarkSnapshotSplitInfo);
            } else {
                updatedSnapshotSplitInfos.add(existedSplitInfo);
            }
        }
        return updatedSnapshotSplitInfos;
    }
}
