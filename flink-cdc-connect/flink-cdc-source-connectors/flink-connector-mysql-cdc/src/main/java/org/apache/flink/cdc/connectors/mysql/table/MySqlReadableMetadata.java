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

import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataMetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** Defines the supported metadata columns for {@link MySqlTableSource}. */
// 主要作用是 定义并实现 MySQL CDC 源表支持的“元数据列”（Metadata Columns）。
// 在 Flink SQL 中，当你使用 MySQL CDC 作为数据源时，除了同步业务表本身的字段（如 id, name），你往往还需要获取一些 关于数据本身的额外信息（即元数据），例如：
// 这条数据是从哪个数据库、哪张表同步过来的？
// 这条数据在 MySQL 中发生变更的精确时间戳是多少？
// 这条数据的操作类型是什么（插入、更新还是删除）？
// MySqlReadableMetadata 通过枚举的方式，将这些信息映射为 Flink SQL 能够识别的虚拟列。用户可以通过 METADATA 关键字在 DDL 中声明这些列。
public enum MySqlReadableMetadata {
    /** Name of the table that contain the row. */
    // 获取产生该行数据的 MySQL 原始表名。
    // 实现逻辑：从 Debezium 的 SourceRecord 值的 source 结构体中提取 table 字段。
    TABLE_NAME(
            "table_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(AbstractSourceInfo.TABLE_NAME_KEY));
                }
            }),

    /** Name of the database that contain the row. */
    // 获取产生该行数据的 MySQL 数据库名。
    DATABASE_NAME(
            "database_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(AbstractSourceInfo.DATABASE_NAME_KEY));
                }
            }),

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the binlog, the value is always 0.
     */
    // 获取该变更在 MySQL 数据库中发生的精确时间。
    // 如果是全量同步（Snapshot）阶段，该值通常为 0；只有在增量同步（Binlog）阶段才有真实值。
    OP_TS(
            "op_ts",
            DataTypes.TIMESTAMP_LTZ(3).notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return TimestampData.fromEpochMillis(
                            (Long) sourceStruct.get(AbstractSourceInfo.TIMESTAMP_KEY));
                }
            }),

    /**
     * It indicates the row kind of the changelog. '+I' means INSERT message, '-D' means DELETE
     * message, '-U' means UPDATE_BEFORE message and '+U' means UPDATE_AFTER message
     */
    // 获取该行数据的 变更类型标识（RowKind）。
    // 标识符：+I (插入), -D (删除), -U (更新前镜像), +U (更新后镜像)。
    ROW_KIND(
            "row_kind",
            DataTypes.STRING().notNull(),
            new RowDataMetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(RowData rowData) {
                    return StringData.fromString(rowData.getRowKind().shortString());
                }

                @Override
                public Object read(SourceRecord record) {
                    throw new UnsupportedOperationException(
                            "Please call read(RowData rowData) method instead.");
                }
            });

    private final String key;

    private final DataType dataType;

    private final MetadataConverter converter;

    MySqlReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public MetadataConverter getConverter() {
        return converter;
    }
}
