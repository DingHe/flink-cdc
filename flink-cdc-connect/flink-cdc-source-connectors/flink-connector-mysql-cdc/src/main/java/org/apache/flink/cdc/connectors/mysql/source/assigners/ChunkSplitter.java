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

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;

import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.relational.TableId;

import java.util.List;

/**
 * The {@code ChunkSplitter}'s task is to split table into a set of chunks or called splits (i.e.
 * {@link MySqlSnapshotSplit}).
 */
// ChunkSplitter 是实现**增量快照读取（Incremental Snapshot Reading）**的核心组件接口。它决定了如何将一张海量数据的物理表“切”成一段段可并行处理的分片。
// 在处理大表时，一次性读取整张表会造成数据库压力过大且无法并行。ChunkSplitter 通过分析表的主键（Primary Key）分布，将表划分为多个 MySqlSnapshotSplit（即 Chunk）
// 并行化基础：它产生的分片可以分发给不同的并行子任务（Source Reader）同时读取。
// 增量式切分：它不需要一次性计算出所有分片，可以多次调用，逐步完成整表的切分，避免一次性生成过多分片导致内存溢出。
// 状态化：它支持 Flink 的 Checkpoint 机制，确保在切分过程中如果作业失败，可以从上次切分的位置恢复。
public interface ChunkSplitter {
    /**
     * Called to open the chunk splitter to acquire any resources, like threads or jdbc connections.
     */
    // 在切分工作开始前被调用。实现类通常在此方法中建立 JDBC 数据库连接、获取数据库元数据（如主键列名、类型等）或初始化内部使用的线程池。
    void open();

    /**
     * Called to split chunks for a table, the assigner could invoke this method multiple times to
     * receive all the splits.
     */
    // 执行实际的切分动作。
    // partition 标识 MySQL 的分区信息；tableId 标识具体的表名（库名.表名）。
    List<MySqlSnapshotSplit> splitChunks(MySqlPartition partition, TableId tableId)
            throws Exception;

    /** Get whether the splitter has more chunks for current table. */
    // 标识当前正在切分的表是否还有剩余的数据段没切完。
    boolean hasNextChunk();

    /**
     * Creates a snapshot of the state of this chunk splitter, to be stored in a checkpoint.
     *
     * <p>This method takes the ID of the checkpoint for which the state is snapshotted. Most
     * implementations should be able to ignore this parameter, because for the contents of the
     * snapshot, it doesn't matter for which checkpoint it gets created. This parameter can be
     * interesting for source connectors with external systems where those systems are themselves
     * aware of checkpoints; for example in cases where the enumerator notifies that system about a
     * specific checkpoint being triggered.
     *
     * @param checkpointId The ID of the checkpoint for which the snapshot is created.
     * @return an object containing the state of the split enumerator.
     */
    // 持久化切分进度。
    // 它将当前切分到的主键位置（例如：已经切到了 id=50000）封装进 ChunkSplitterState 对象中。
    // 保证了“精确一次”的切分语义，防止重启后重复切分或漏切。
    ChunkSplitterState snapshotState(long checkpointId);

    /**
     * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
     * was committed.
     *
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    // 当 Flink 确认某个 Checkpoint 已经成功保存到外部存储后调用。切分器可以利用此信号清理一些不再需要的旧状态或临时资源。
    void notifyCheckpointComplete(long checkpointId);

    /**
     * Called to close the splitter, in case it holds on to any resources, like threads or jdbc
     * connections.
     */
    void close() throws Exception;
}
