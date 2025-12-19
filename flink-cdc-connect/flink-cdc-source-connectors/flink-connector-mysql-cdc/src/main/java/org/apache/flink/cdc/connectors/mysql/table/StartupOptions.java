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

package org.apache.flink.cdc.connectors.mysql.table;

import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Debezium startup options. */
// StartupOptions 的主要作用是定义 Flink MySQL CDC 连接器在第一次启动时，应该从哪个位置开始读取数据。
// 在 CDC（数据变更捕获）场景中，通常有两种数据读取阶段：
// 快照阶段 (Snapshot Phase)：读取数据库中已有的存量数据。
// 增量阶段 (Binlog/Stream Phase)：读取实时产生的变更数据（Binlog）。
// StartupOptions 通过组合不同的 StartupMode 和 BinlogOffset，让用户能够灵活选择是“先读存量再读增量”、“只读存量”还是“直接从某个特定时间点/位置读增量”。
public final class StartupOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    // 启动模式。
    // 这是一个枚举值（如 INITIAL, SNAPSHOT, TIMESTAMP 等），决定了连接器启动时的逻辑框架。
    public final StartupMode startupMode;
    // Binlog 偏移量。 具体的起始位置信息。
    // 如果模式需要指定位置（如特定文件或时间戳），该属性会存储具体的数据；如果是全量初始化模式，则可能为 null。
    @Nullable public final BinlogOffset binlogOffset;

    /**
     * Performs an initial snapshot on the monitored database tables upon first startup, and
     * continue to read the latest binlog.
     */
    // 全量初始化（先扫描表，再读 Binlog）。
    // 场景：最常用的模式。第一次启动时同步整张表，然后实时追踪后续更新。
    public static StartupOptions initial() {
        return new StartupOptions(StartupMode.INITIAL, null);
    }

    /**
     * Performs an initial snapshot on the monitored database tables upon first startup, and not
     * read the binlog anymore .
     */
    // 只读快照
    // 场景：只读取当前时刻的存量数据，读取完成后任务结束，不读取后续的增量更新。
    public static StartupOptions snapshot() {
        return new StartupOptions(StartupMode.SNAPSHOT, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the beginning of the binlog. This should be used with care, as it is only valid when the
     * binlog is guaranteed to contain the entire history of the database.
     */
    // 从 Binlog 的最开始位置读取。
    // 场景：不进行快照。适用于 Binlog 包含了数据库完整历史记录的情况（较少见）。
    public static StartupOptions earliest() {
        return new StartupOptions(StartupMode.EARLIEST_OFFSET, BinlogOffset.ofEarliest());
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the end of the binlog which means only have the changes since the connector was started.
     */
    // 从最新位置读取（只读未来）。
    // 场景：不关心存量数据，只从连接器启动的那一刻开始捕获新的变更。
    public static StartupOptions latest() {
        return new StartupOptions(StartupMode.LATEST_OFFSET, BinlogOffset.ofLatest());
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read binlog from the specified offset.
     */
    // 从指定的位点开始
    // 重载：支持 (文件名, 位置)、GTID 集合 或直接传入 BinlogOffset 对象。
    // 场景：精准控制。例如从另一个作业停止时的位点恢复。
    public static StartupOptions specificOffset(String specificOffsetFile, long specificOffsetPos) {
        return new StartupOptions(
                StartupMode.SPECIFIC_OFFSETS,
                BinlogOffset.ofBinlogFilePosition(specificOffsetFile, specificOffsetPos));
    }

    public static StartupOptions specificOffset(String gtidSet) {
        return new StartupOptions(StartupMode.SPECIFIC_OFFSETS, BinlogOffset.ofGtidSet(gtidSet));
    }

    public static StartupOptions specificOffset(BinlogOffset binlogOffset) {
        return new StartupOptions(StartupMode.SPECIFIC_OFFSETS, binlogOffset);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read binlog from the specified timestamp.
     *
     * <p>The consumer will traverse the binlog from the beginning and ignore change events whose
     * timestamp is smaller than the specified timestamp.
     *
     * @param startupTimestampMillis timestamp for the startup offsets, as milliseconds from epoch.
     */
    // 行为：从指定的时间戳开始读取 Binlog。
    // 逻辑：连接器会遍历 Binlog 并过滤掉早于该时间戳的事件。
    public static StartupOptions timestamp(long startupTimestampMillis) {
        return new StartupOptions(
                StartupMode.TIMESTAMP, BinlogOffset.ofTimestampSec(startupTimestampMillis / 1000));
    }

    private StartupOptions(StartupMode startupMode, BinlogOffset binlogOffset) {
        this.startupMode = startupMode;
        this.binlogOffset = binlogOffset;
        if (isStreamOnly()) {
            checkNotNull(
                    binlogOffset, "Binlog offset is required if startup mode is %s", startupMode);
        }
    }
    // 作用：判断是否为“纯流读”模式。
    // 逻辑：如果是 EARLIEST, LATEST, SPECIFIC_OFFSETS, 或 TIMESTAMP 模式，返回 true。这些模式跳过快照步骤。
    public boolean isStreamOnly() {
        return startupMode == StartupMode.EARLIEST_OFFSET
                || startupMode == StartupMode.LATEST_OFFSET
                || startupMode == StartupMode.SPECIFIC_OFFSETS
                || startupMode == StartupMode.TIMESTAMP;
    }

    // 作用：判断是否为“仅快照”模式。
    public boolean isSnapshotOnly() {
        return startupMode == StartupMode.SNAPSHOT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartupOptions that = (StartupOptions) o;
        return startupMode == that.startupMode && Objects.equals(binlogOffset, that.binlogOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startupMode, binlogOffset);
    }
}
