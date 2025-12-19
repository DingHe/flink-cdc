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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** The split to describe a split of a MySql table snapshot. */
// MySqlSnapshotSplit 是 Flink CDC MySQL 连接器中极其关键的一个类。
// 它是“并行增量快照算法”（Incremental Snapshot Algorithm）得以实现的核心载体。
// 在 Flink CDC 读取 MySQL 全量（快照）数据时，为了实现并行化和无锁化，它会将一张大表按照主键（或其他拆分键）切分成多个“块”（Chunks）。
// MySqlSnapshotSplit 的作用就是：
// 定义范围：它代表了一张表中的“一段数据记录”。比如：id 从 1 到 1000 的所有行。
// 保存元数据：它记录了该分片的起始位置、结束位置、对应的表结构以及当前分片读取完成后的 高水位线（High Watermark） 位点。
// 任务分配：JobManager 将这些 Split 发送给不同的 TaskManager，从而让多个节点并行读取数据，大幅提升存量数据的同步效率。

public class MySqlSnapshotSplit extends MySqlSplit {
    // 标识该分片所属的数据库表（包含库名和表名）。
    private final TableId tableId;
    // 拆分键（通常是主键）的数据类型（如 INT, BIGINT, STRING）。
    private final RowType splitKeyType;
    // 保存该表的结构信息。
    // 读取二进制数据并将其还原为对象时需要用到。
    private final Map<TableId, TableChange> tableSchemas;
    // 分片范围起点。
    // 包含主键的具体值。如果为 null，表示从表的开头开始。
    @Nullable private final Object[] splitStart;
    // 分片范围终点。
    // 如果为 null，表示一直读取到表的末尾。
    @Nullable private final Object[] splitEnd;
    /** The high watermark is not null when the split read finished. */
    // 高水位线。这是增量快照算法的核心。
    // 记录的是该分片读取完那一刻的 Binlog 位置。用于后续将快照期间发生的 Binlog 变更与快照数据进行合并。
    @Nullable private final BinlogOffset highWatermark;
    // 缓存属性（transient）。
    // 用于缓存序列化后的结果，优化性能，避免重复序列化。
    @Nullable transient byte[] serializedFormCache;

    /**
     * Create a SnapshotSplit with generating splitId with the given tableId and chunkId.
     *
     * @see #generateSplitId(TableId, int)
     */
    public MySqlSnapshotSplit(
            TableId tableId,
            int chunkId,
            RowType splitKeyType,
            Object[] splitStart,
            Object[] splitEnd,
            BinlogOffset highWatermark,
            Map<TableId, TableChange> tableSchemas) {
        super(generateSplitId(tableId, chunkId));
        this.tableId = tableId;
        this.splitKeyType = splitKeyType;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.highWatermark = highWatermark;
        this.tableSchemas = tableSchemas;
    }

    /**
     * This constructor should not be used directly. Please use the other constructor. If this
     * constructor must be invoked, please use the same format for the splitId as {@link
     * #generateSplitId(TableId, int)}. Or else the parsing method will fail. See more in {@link
     * #extractTableId(String)} and {@link #extractChunkId(String)}.
     */
    @Internal
    public MySqlSnapshotSplit(
            TableId tableId,
            String splitId,
            RowType splitKeyType,
            Object[] splitStart,
            Object[] splitEnd,
            BinlogOffset highWatermark,
            Map<TableId, TableChange> tableSchemas) {
        super(splitId);
        this.tableId = tableId;
        this.splitKeyType = splitKeyType;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.highWatermark = highWatermark;
        this.tableSchemas = tableSchemas;
    }

    public TableId getTableId() {
        return tableId;
    }

    @Nullable
    public Object[] getSplitStart() {
        return splitStart;
    }

    @Nullable
    public Object[] getSplitEnd() {
        return splitEnd;
    }

    @Nullable
    public BinlogOffset getHighWatermark() {
        return highWatermark;
    }

    public boolean isSnapshotReadFinished() {
        return highWatermark != null;
    }

    @Override
    public Map<TableId, TableChange> getTableSchemas() {
        return tableSchemas;
    }

    /** Casts this split into a {@link MySqlSchemalessSnapshotSplit}. */
    public final MySqlSchemalessSnapshotSplit toSchemalessSnapshotSplit() {
        return new MySqlSchemalessSnapshotSplit(
                tableId, splitId, splitKeyType, splitStart, splitEnd, highWatermark);
    }

    public static String generateSplitId(TableId tableId, int chunkId) {
        return tableId.toString() + ":" + chunkId;
    }

    public static TableId extractTableId(String splitId) {
        return TableId.parse(splitId.substring(0, splitId.lastIndexOf(":")));
    }

    public static int extractChunkId(String splitId) {
        return Integer.parseInt(splitId.substring(splitId.lastIndexOf(":") + 1));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MySqlSnapshotSplit that = (MySqlSnapshotSplit) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(splitKeyType, that.splitKeyType)
                && Arrays.equals(splitStart, that.splitStart)
                && Arrays.equals(splitEnd, that.splitEnd)
                && Objects.equals(highWatermark, that.highWatermark);
    }

    public RowType getSplitKeyType() {
        return splitKeyType;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), tableId, splitKeyType, highWatermark);
        result = 31 * result + Arrays.hashCode(splitStart);
        result = 31 * result + Arrays.hashCode(splitEnd);
        result = 31 * result + Arrays.hashCode(serializedFormCache);
        return result;
    }

    @Override
    public String toString() {
        String splitKeyTypeSummary =
                splitKeyType.getFields().stream()
                        .map(RowType.RowField::asSummaryString)
                        .collect(Collectors.joining(",", "[", "]"));
        return "MySqlSnapshotSplit{"
                + "tableId="
                + tableId
                + ", splitId='"
                + splitId
                + '\''
                + ", splitKeyType="
                + splitKeyTypeSummary
                + ", splitStart="
                + Arrays.toString(splitStart)
                + ", splitEnd="
                + Arrays.toString(splitEnd)
                + ", highWatermark="
                + highWatermark
                + '}';
    }
}
