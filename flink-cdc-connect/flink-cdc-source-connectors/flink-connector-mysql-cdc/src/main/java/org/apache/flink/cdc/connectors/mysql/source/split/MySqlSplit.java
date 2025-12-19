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

import org.apache.flink.api.connector.source.SourceSplit;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.Map;
import java.util.Objects;

/** The split of table comes from a Table that splits by primary key. */
// 在 Flink CDC 的并行读取架构中，数据被划分为多个“分片（Splits）”。MySqlSplit 的主要作用是：
// 作为基础抽象：它定义了 MySQL 数据源中所有分片的通用行为，统一了“存量快照分片（Snapshot Split）”和“增量日志分片（Binlog Split）”的身份标识。
// 实现并行读取的基础：通过将一张大表按照主键范围切分成多个 MySqlSplit，Flink 才能让多个并行任务（TaskManager）同时读取数据库。
// 状态恢复的凭据：分片包含了读取数据所需的所有位置信息。当任务失败重启时，Flink 会通过保存的 Split 信息知道该从哪个位置继续读取

public abstract class MySqlSplit implements SourceSplit {
    // 分片的唯一标识符。
    // Flink 框架内部，每个分片必须有一个唯一的字符串 ID。对于快照分片，ID 通常包含表名和主键范围；
    // 对于 Binlog 分片，ID 通常是固定的（如 "binlog-split"），因为 Binlog 通常作为单线程处理。
    protected final String splitId;

    public MySqlSplit(String splitId) {
        this.splitId = splitId;
    }

    /** Checks whether this split is a snapshot split. */
    // 判断当前分片是否属于“快照阶段”。
    public final boolean isSnapshotSplit() {
        return getClass() == MySqlSnapshotSplit.class
                || getClass() == MySqlSchemalessSnapshotSplit.class;
    }

    /** Checks whether this split is a binlog split. */
    // 判断当前分片是否属于“增量阶段”。
    public final boolean isBinlogSplit() {
        return getClass() == MySqlBinlogSplit.class;
    }

    /** Casts this split into a {@link MySqlSnapshotSplit}. */
    // 强制类型转换。
    public final MySqlSnapshotSplit asSnapshotSplit() {
        return (MySqlSnapshotSplit) this;
    }

    /** Casts this split into a {@link MySqlBinlogSplit}. */
    public final MySqlBinlogSplit asBinlogSplit() {
        return (MySqlBinlogSplit) this;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public abstract Map<TableId, TableChanges.TableChange> getTableSchemas();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MySqlSplit that = (MySqlSplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
}
