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
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A {@link MySqlSplitAssigner} that splits tables into small chunk splits based on primary key
 * range and chunk size and also continue with a binlog split.
 */

// Flink CDC 实现“全量 + 增量”无缝衔接的核心组件。它作为一个“指挥官”，负责协调 快照阶段（Snapshot Phase） 和 增量阶段（Binlog Phase） 的平滑切换。
// 该类的主要作用是混合任务分配。在 Flink CDC 任务启动时，它首先利用 MySqlSnapshotSplitAssigner 将表切分成多个 Chunk 并行读取历史存量数据；
// 当所有存量分片读取完毕后，它会自动创建一个 Binlog 分片，引导任务进入增量数据监听阶段。
// 顺序保证：确保在所有快照分片读完并上报“高水位线”后，才开始分配 Binlog 分片，防止同一主键的数据乱序。
// 状态切换：管理从“只读快照”到“只读 Binlog”的状态流转。
// 动态表发现支持：支持在运行过程中增加新的表，并为其分配快照任务。
public class MySqlHybridSplitAssigner implements MySqlSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlHybridSplitAssigner.class);
    // 一个常量字符串（"binlog-split"），作为生成的唯一 Binlog 分片的标识符。
    private static final String BINLOG_SPLIT_ID = "binlog-split";
    // 用于控制快照元数据分组的大小。当快照分片非常多时，元数据过大会导致 RPC 传输困难，此参数用于分组优化。
    private final int splitMetaGroupSize;
    private final MySqlSourceConfig sourceConfig;
    // 关键状态位。标记 Binlog 分片是否已经分发出去。
    // 由于 Binlog 分片通常全局只有一个，必须防止重复分发。
    private boolean isBinlogSplitAssigned;
    // 内部持有的快照分配器实例，所有的快照分片逻辑实际上都委托给它处理。
    private final MySqlSnapshotSplitAssigner snapshotSplitAssigner;

    public MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this(
                sourceConfig,
                new MySqlSnapshotSplitAssigner(
                        sourceConfig,
                        currentParallelism,
                        remainingTables,
                        isTableIdCaseSensitive,
                        enumeratorContext),
                false,
                sourceConfig.getSplitMetaGroupSize());
    }

    public MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState checkpoint,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this(
                sourceConfig,
                new MySqlSnapshotSplitAssigner(
                        sourceConfig,
                        currentParallelism,
                        checkpoint.getSnapshotPendingSplits(),
                        enumeratorContext),
                checkpoint.isBinlogSplitAssigned(),
                sourceConfig.getSplitMetaGroupSize());
    }

    private MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            MySqlSnapshotSplitAssigner snapshotSplitAssigner,
            boolean isBinlogSplitAssigned,
            int splitMetaGroupSize) {
        this.sourceConfig = sourceConfig;
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
        this.splitMetaGroupSize = splitMetaGroupSize;
    }

    @Override
    public void open() {
        snapshotSplitAssigner.open();
    }
    // 精确控制着 快照分片（Snapshot Split） 向 增量分片（Binlog Split） 切换的时机。
    @Override
    public Optional<MySqlSplit> getNext() {
        // 检查是否处于“新表快照分配已完成但尚未完全转场”的中间状态。
        // 如果用户在作业运行期间动态添加了表，分配器需要确保新表的快照分片完全处理完毕。在某些特定子状态下，为了保证一致性，会暂时停止发放新的分片。
        if (AssignerStatus.isNewlyAddedAssigningSnapshotFinished(getAssignerStatus())) {
            // do not assign split until the adding table process finished
            return Optional.empty();
        }
        // 判断所有的**历史存量数据（快照分片）**是否已经全部分配完毕。
        // 如果 noMoreSplits() 为 true，意味着静态的表数据已经切分并分发完了，接下来的逻辑将尝试进入 Binlog（增量）阶段。
        if (snapshotSplitAssigner.noMoreSplits()) {
            // binlog split assigning
            // 检查 Binlog 分片是否已经分配出去了
            if (isBinlogSplitAssigned) {
                // no more splits for the assigner
                return Optional.empty();
            // 处理作业第一次启动时，从全量快照到增量 Binlog 的切换
            } else if (AssignerStatus.isInitialAssigningFinished(
                    snapshotSplitAssigner.getAssignerStatus())) {
                // we need to wait snapshot-assigner to be finished before
                // assigning the binlog split. Otherwise, records emitted from binlog split
                // might be out-of-order in terms of same primary key with snapshot splits.
                isBinlogSplitAssigned = true;
                return Optional.of(createBinlogSplit());
            // 运行中动态加表的处理
            // 此时 Binlog 读取器（Binlog Reader）已经在运行了，不需要再次创建一个新的 Binlog 分片，只需要标记分配完成，并触发相关机制让 Binlog Reader 开始监听新增加的表。
            } else if (AssignerStatus.isNewlyAddedAssigningFinished(
                    snapshotSplitAssigner.getAssignerStatus())) {
                // do not need to create binlog, but send event to wake up the binlog reader
                isBinlogSplitAssigned = true;
                return Optional.empty();
            } else {
                // binlog split is not ready by now
                return Optional.empty();
            }
        } else {
            // snapshot assigner still have remaining splits, assign split from it
            return snapshotSplitAssigner.getNext();
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return snapshotSplitAssigner.waitingForFinishedSplits();
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        return snapshotSplitAssigner.getFinishedSplitInfos();
    }

    @Override
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        snapshotSplitAssigner.onFinishedSplits(splitFinishedOffsets);
    }

    @Override
    public void addSplits(Collection<MySqlSplit> splits) {
        List<MySqlSplit> snapshotSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                // we don't store the split, but will re-create binlog split later
                isBinlogSplitAssigned = false;
            }
        }
        snapshotSplitAssigner.addSplits(snapshotSplits);
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new HybridPendingSplitsState(
                snapshotSplitAssigner.snapshotState(checkpointId), isBinlogSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return snapshotSplitAssigner.getAssignerStatus();
    }

    @Override
    public boolean noMoreSplits() {
        return snapshotSplitAssigner.noMoreSplits() && isBinlogSplitAssigned;
    }

    @Override
    public void startAssignNewlyAddedTables() {
        snapshotSplitAssigner.startAssignNewlyAddedTables();
    }

    @Override
    public void onBinlogSplitUpdated() {
        snapshotSplitAssigner.onBinlogSplitUpdated();
    }

    @Override
    public void close() {
        snapshotSplitAssigner.close();
    }

    // --------------------------------------------------------------------------------------------
    // Flink CDC 实现 “无锁增量快照算法” 的终点，也是 增量阶段（Binlog） 的起点。
    // 它的核心任务是将所有已经完成的快照分片（Snapshot Splits）的 高水位线（High Watermark） 进行汇总，
    // 从而构建出一个能够覆盖所有数据变动、且不丢不重的 Binlog 读取任务。
    private MySqlBinlogSplit createBinlogSplit() {
        // 从快照分配器中获取所有已经分发出去的快照分片，并按 splitId 进行排序
        // 确保处理顺序的一致性。这些分片代表了全量阶段处理过的所有数据范围。
        final List<MySqlSchemalessSnapshotSplit> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(MySqlSplit::splitId))
                        .collect(Collectors.toList());
        // 获取完成偏移量映射
        Map<String, BinlogOffset> splitFinishedOffsets =
                snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        // 所有快照分片中最早开始上报水位线的位置
        BinlogOffset minBinlogOffset = null;
        // 所有快照分片中最后完成读取的位置。这是 增量读取的逻辑起点
        BinlogOffset maxBinlogOffset = null;
        // 寻找全量阶段的 Binlog 边界
        for (MySqlSchemalessSnapshotSplit split : assignedSnapshotSplit) {
            // find the min and max binlog offset
            // // 更新最小偏移量
            BinlogOffset binlogOffset = splitFinishedOffsets.get(split.splitId());
            if (minBinlogOffset == null || binlogOffset.isBefore(minBinlogOffset)) {
                minBinlogOffset = binlogOffset;
            }
            // 更新最大偏移量
            if (maxBinlogOffset == null || binlogOffset.isAfter(maxBinlogOffset)) {
                maxBinlogOffset = binlogOffset;
            }
            // 封装已完成分片的详细信息
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            binlogOffset));
        }

        // If the source is running in snapshot mode, we use the highest watermark among
        // snapshot splits as the ending offset to provide a consistent snapshot view at the moment
        // of high watermark.
        // 决定 Binlog 读取器什么时候“下班”。
        BinlogOffset stoppingOffset = BinlogOffset.ofNonStopping();
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            stoppingOffset = maxBinlogOffset;
        }

        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them
        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
        // 创建并返回 Binlog 分片
        // 这个 Split 被 Assignor 发送给 Source Reader 之后，MySQL 连接器就从“快照模式”正式切换到了“流式模式”。
        return new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                minBinlogOffset == null ? BinlogOffset.ofEarliest() : minBinlogOffset, // 起始位
                stoppingOffset, // 结束位
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size());
    }
}
