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

import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.runtime.parser.metadata.MetadataColumns;

import javax.annotation.Nullable;

import java.util.Map;

/** Contextual information during Post-transform phase. */
// 保存运行时上下文：它记录了当前正在处理的事件是“增、删、改”中的哪一种（opType）、数据发生的系统时间（epochTime）以及数据源携带的原始元数据（meta）。
// 这些信息是执行计算表达式（如 CURRENT_TIMESTAMP 或引用源表名）的基础。
// 保存运行时上下文：它记录了当前正在处理的事件是“增、删、改”中的哪一种（opType）、数据发生的系统时间（epochTime）以及数据源携带的原始元数据（meta）。
// 这些信息是执行计算表达式（如 CURRENT_TIMESTAMP 或引用源表名）的基础。
// 定义字段查找协议：通过静态方法 lookupObjectByName，它统一了表达式引擎访问各种数据的路径。无论用户在 SQL 表达式中写的是列名、元数据名还是计算后的别名，都由该类负责“寻址”并获取真实数值。
public class TransformContext {
    // 处理时间戳：记录当前事件被算子处理时的系统毫秒时间。
    // 它被注入到 Janino 编译后的方法参数中，用于支持时间相关的内置函数（如获取当前处理时间）。
    public long epochTime;
    // 操作类型：记录当前 DataChangeEvent 的操作类型，通常为 INSERT、UPDATE、DELETE 等字符串。
    // 这允许用户在 Filter 或 Projection 中基于操作类型做逻辑判断（例如：filter: op_type = 'INSERT'）
    public String opType;
    // 原始元数据映射：存储从数据源（Source）传递过来的原始元数据信息（如 MySQL 的 binlog.file、binlog.pos 等）。
    // 它是 SupportedMetadataColumn 读取元数据时的原始素材。
    public Map<String, String> meta;

    /**
     * Retrieve a corresponding object based on identifier name. The lookup order would be: <br>
     * 1. Built-in metadata column names; <br>
     * 2. Source-provided metadata column names; <br>
     * 3. Calculated column names (which may shade original columns); <br>
     * 4. Existing upstream column names. <br>
     * If given name is nowhere to be found, an exception will be thrown.
     */
    // 这个方法是表达式引擎（ProjectionColumnProcessor 或 FilterProcessor）在准备计算参数时的**“首席导航员”**。
    // 它规定了当表达式中出现一个标识符（如字段名）时，应该按什么顺序去哪里找。
    public static Object lookupObjectByName(
            String name, // 要查找的标识符名称（如 "id" 或 "__table_name__"）
            PostTransformChangeInfo tableInfo, // 转换信息快照，包含前后 Schema 的映射。
            Map<String, SupportedMetadataColumn> supportedMetadataColumns, // 当前数据源支持的元数据字典。
            Object[] preRow, // 原始输入数据行（Java 对象数组）
            @Nullable Object[] postRow, // （可选）计算后的数据行，用于某些后置处理场景。
            TransformContext context) { // 当前的 TransformContext 实例，提供运行时环境。
        // 检查 name 是否是内置常量（如库名、表名、Schema 名或事件类型）。
        switch (name) {
            case MetadataColumns.DEFAULT_NAMESPACE_NAME:
                return tableInfo.getNamespace();
            case MetadataColumns.DEFAULT_SCHEMA_NAME:
                return tableInfo.getSchemaName();
            case MetadataColumns.DEFAULT_TABLE_NAME:
                return tableInfo.getTableName();
            case MetadataColumns.DEFAULT_DATA_EVENT_TYPE:
                return context.opType;
        }

        // or source-provided metadata column
        // 检查该名称是否存在于 supportedMetadataColumns 映射中
        if (supportedMetadataColumns.containsKey(name)) {
            return supportedMetadataColumns.get(name).read(context.meta);
        }
        // 如果提供了 postRow，则尝试在转换后的 Schema 中查找。
        // 逻辑：如果用户定义了计算列 a + b AS c，此时查找 c 就能从 postRow 的相应位置拿到结果。这允许“列名遮蔽”（Shade），即计算列可以覆盖原始列。
        if (postRow != null) {
            // or a column that is presented in post-transform schema
            @Nullable
            Integer indexInPostTransformedSchema =
                    tableInfo.getPostTransformedSchemaFieldIndex(name);
            if (indexInPostTransformedSchema != null) {
                return postRow[indexInPostTransformedSchema];
            }
        }

        // or a pre-transformed column that has been projected out
        // 最后在进入算子前的原始 Schema 中查找。
        // 逻辑：通过 preRow 和物理索引获取最原始的数据值。
        @Nullable
        Integer indexInPreTransformedSchema = tableInfo.getPreTransformedSchemaFieldIndex(name);
        if (indexInPreTransformedSchema != null) {
            return preRow[indexInPreTransformedSchema];
        }

        throw new RuntimeException("Failed to lookup column name: " + name);
    }
}
