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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PostTransformChangeInfo caches pre-transformed / pre-transformed schema, schema field getters,
 * and binary record data generator for post-transform schema.
 */

// PostTransformChangeInfo 的主要作用是 “桥接转换前后的结构” 并提供 “高性能访问接口”
// Schema 状态快照：同时保存了进入该算子时的 Schema（Pre-transformed）和离开该算子时的 Schema（Post-transformed）
// 访问加速：将列名与索引（Index）的映射关系预先存入 Map，避免在处理每行记录时都去遍历 Schema。
// 读写工具缓存：预先创建好从二进制行数据中读取字段的 FieldGetter，以及将计算结果序列化回二进制格式的 BinaryRecordDataGenerator。

public class PostTransformChangeInfo {
    // 标识该元数据属于哪张表（包含数据库、表名等）。
    private final TableId tableId;
    // 进入后置转换算子前的表结构。
    private final Schema preTransformedSchema;
    // 经过用户规则（投影、计算列、UDF）处理后的最终表结构。
    private final Schema postTransformedSchema;
    // 加速用：记录原始列名到其在数组中位置的映射，
    // 方便表达式引擎快速通过列名查找数据。
    private final Map<String, Integer> preTransformedSchemaFieldNameToIndexMap;
    // 核心读工具：针对原始 Schema 生成的字段获取器。
    // 可以直接从 BinaryRecordData 中高效提取特定位置的 Java 对象。
    private final RecordData.FieldGetter[] preTransformedFieldGetters;
    // 针对最终 Schema 生成的字段获取器（通常用于处理后的校验或二次读取）。
    private final RecordData.FieldGetter[] postTransformedFieldGetters;
    // 核心写工具：根据最终 Schema 生成。
    // 负责将转换计算后的 Java 对象数组（Object[]）重新打包成 Flink 内部的高性能二进制格式。
    private final BinaryRecordDataGenerator postTransformedRecordDataGenerator;
    // 加速用：记录最终输出列名到其索引位置的映射。
    private final Map<String, Integer> postTransformedSchemaFieldNameToIndexMap;

    public static PostTransformChangeInfo of(
            TableId tableId, Schema preTransformedSchema, Schema postTransformedSchema) {

        List<RecordData.FieldGetter> preTransformedFieldGetters =
                SchemaUtils.createFieldGetters(preTransformedSchema.getColumns());

        List<RecordData.FieldGetter> postTransformedFieldGetters =
                SchemaUtils.createFieldGetters(postTransformedSchema.getColumns());

        BinaryRecordDataGenerator postTransformedRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        DataTypeConverter.toRowType(postTransformedSchema.getColumns()));

        return new PostTransformChangeInfo(
                tableId,
                preTransformedSchema,
                preTransformedFieldGetters.toArray(new RecordData.FieldGetter[0]),
                postTransformedSchema,
                postTransformedFieldGetters.toArray(new RecordData.FieldGetter[0]),
                postTransformedRecordDataGenerator);
    }

    private PostTransformChangeInfo(
            TableId tableId,
            Schema preTransformedSchema,
            RecordData.FieldGetter[] preTransformedFieldGetters,
            Schema postTransformedSchema,
            RecordData.FieldGetter[] postTransformedFieldGetters,
            BinaryRecordDataGenerator postTransformedRecordDataGenerator) {

        this.tableId = tableId;

        this.preTransformedSchema = preTransformedSchema;
        this.preTransformedFieldGetters = preTransformedFieldGetters;
        this.preTransformedSchemaFieldNameToIndexMap = new HashMap<>();
        for (int i = 0; i < preTransformedSchema.getColumns().size(); i++) {
            preTransformedSchemaFieldNameToIndexMap.put(
                    preTransformedSchema.getColumns().get(i).getName(), i);
        }

        this.postTransformedSchema = postTransformedSchema;
        this.postTransformedFieldGetters = postTransformedFieldGetters;
        this.postTransformedRecordDataGenerator = postTransformedRecordDataGenerator;
        this.postTransformedSchemaFieldNameToIndexMap = new HashMap<>();

        for (int i = 0; i < postTransformedSchema.getColumns().size(); i++) {
            postTransformedSchemaFieldNameToIndexMap.put(
                    postTransformedSchema.getColumns().get(i).getName(), i);
        }
    }

    public String getName() {
        return tableId.identifier();
    }

    public String getTableName() {
        return tableId.getTableName();
    }

    public String getSchemaName() {
        return tableId.getSchemaName();
    }

    public String getNamespace() {
        return tableId.getNamespace();
    }

    public TableId getTableId() {
        return tableId;
    }

    public Schema getPreTransformedSchema() {
        return preTransformedSchema;
    }

    public Schema getPostTransformedSchema() {
        return postTransformedSchema;
    }

    public @Nullable Integer getPreTransformedSchemaFieldIndex(String fieldName) {
        return preTransformedSchemaFieldNameToIndexMap.get(fieldName);
    }

    public @Nullable Integer getPostTransformedSchemaFieldIndex(String fieldName) {
        return postTransformedSchemaFieldNameToIndexMap.get(fieldName);
    }

    public RecordData.FieldGetter[] getPreTransformedFieldGetters() {
        return preTransformedFieldGetters;
    }

    public BinaryRecordDataGenerator getPostTransformedRecordDataGenerator() {
        return postTransformedRecordDataGenerator;
    }
}
