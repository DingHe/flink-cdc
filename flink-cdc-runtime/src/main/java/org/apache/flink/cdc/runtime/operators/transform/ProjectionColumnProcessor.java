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

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.runtime.parser.JaninoCompiler;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.codehaus.janino.ExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.runtime.operators.transform.TransformContext.lookupObjectByName;
import static org.apache.flink.cdc.runtime.parser.metadata.MetadataColumns.METADATA_COLUMNS;

/**
 * The processor of the projection column. It processes the data column and the user-defined
 * computed columns.
 */
// ProjectionColumnProcessor 是 “最小的执行单元”。
// 如果把整个转换算子比作一个工厂，那么 TransformProjectionProcessor 是一条生产线，而 ProjectionColumnProcessor 就是生产线上专门负责处理单个字段的精密仪器。
// 无论用户定义的列是简单的字段引用（id），还是复杂的算术运算（price * 0.8），亦或是函数调用（CONCAT(name, '_suffix')），该类都会通过 Janino 编译器 将这些表达式编译为字节码，并在运行时高效地计算出结果。


public class ProjectionColumnProcessor {
    // 缓存的表结构信息，
    // 用于在计算时通过列名快速查找原始行数据中的物理偏移量。
    private final PostTransformChangeInfo tableInfo;
    // 该处理器的“任务书”，
    // 包含了列名、数据类型、脚本表达式（Script Expression）以及依赖的原始列名单。
    private final ProjectionColumn projectionColumn;
    // 时区信息。
    // 某些时间函数（如 TO_TIMESTAMP）在计算时需要参考此属性。
    private final String timezone;
    // 缓存键。包含了表达式文本、参数名、参数类型等，用于确保相同逻辑的表达式只被编译一次。
    private final TransformExpressionKey transformExpressionKey;
    // 当前支持的元数据列字典，允许表达式引用如数据源名称、Schema 名等信息。
    private final Map<String, SupportedMetadataColumn> supportedMetadataColumns;
    // 已实例化的 UDF 对象列表。计算时会将这些实例作为参数传入表达式。
    private final List<Object> udfFunctionInstances;
    // 核心执行引擎。
    // 由 Janino 生成的表达式求值器，负责执行最终的字节码指令。
    private final ExpressionEvaluator expressionEvaluator;

    public ProjectionColumnProcessor(
            PostTransformChangeInfo tableInfo,
            ProjectionColumn projectionColumn,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            final List<Object> udfFunctionInstances,
            Map<String, SupportedMetadataColumn> supportedMetadataColumns) {
        this.tableInfo = tableInfo;
        this.projectionColumn = projectionColumn;
        this.timezone = timezone;
        this.supportedMetadataColumns = supportedMetadataColumns;
        this.transformExpressionKey = generateTransformExpressionKey();
        this.expressionEvaluator =
                TransformExpressionCompiler.compileExpression(
                        transformExpressionKey, udfDescriptors);
        this.udfFunctionInstances = udfFunctionInstances;
    }

    public static ProjectionColumnProcessor of(
            PostTransformChangeInfo tableInfo,
            ProjectionColumn projectionColumn,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances,
            Map<String, SupportedMetadataColumn> supportedMetadataColumns) {
        return new ProjectionColumnProcessor(
                tableInfo,
                projectionColumn,
                timezone,
                udfDescriptors,
                udfFunctionInstances,
                supportedMetadataColumns);
    }

    public Object evaluate(Object[] rowData, TransformContext context) {
        try {
            // 调用 generateParams，根据当前这一行数据（rowData）提取出表达式执行所需的全部变量。
            Object[] params = generateParams(rowData, context);
            return expressionEvaluator.evaluate(params);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to evaluate projection expression `%s` for column `%s` in table `%s`.\n"
                                    + "\tColumn name map: {%s}",
                            projectionColumn.getScriptExpression(),
                            projectionColumn.getColumnName(),
                            tableInfo.getName(),
                            projectionColumn.getColumnNameMapAsString()),
                    e);
        }
    }

    // 加载原始列：遍历表达式依赖的所有原始字段，从 rowData 中提取值并存入参数列表。
    private Object[] generateParams(Object[] rowData, TransformContext context) {
        List<Object> params = new ArrayList<>();

        // 1 - Add referenced columns
        LinkedHashSet<String> originalColumnNames =
                new LinkedHashSet<>(projectionColumn.getOriginalColumnNames());
        for (String columnName : originalColumnNames) {
            params.add(
                    lookupObjectByName(
                            columnName,
                            tableInfo,
                            supportedMetadataColumns,
                            rowData,
                            null,
                            context));
        }

        // 2 - Add time-sensitive function arguments
        params.add(timezone);
        params.add(context.epochTime);

        // 3 - Add UDF function instances
        params.addAll(udfFunctionInstances);
        return params.toArray();
    }
    // 分析用户写的 SQL 表达式脚本，提取出该表达式依赖的所有变量（参数名）和数据类型（参数类型），从而为后续的 Janino 字节码编译生成一份精确的“方法签名”。
    private TransformExpressionKey generateTransformExpressionKey() {
        // 存储编译后的 Java 方法所需的参数名列表
        List<String> argumentNames = new ArrayList<>();
        // 存储对应参数的 Java 类型（Class 对象）。这两个列表必须一一对应
        List<Class<?>> paramTypes = new ArrayList<>();
        // 获取原始（转换前）表的列定义
        List<Column> columns = tableInfo.getPreTransformedSchema().getColumns();
        // scriptExpression: 用户定义的 SQL 表达式字符串（例如 id + 1）。
        String scriptExpression = projectionColumn.getScriptExpression();
        // columnNameMap: 一个映射表，将 SQL 中的原始列名映射为合法的 Java 变量名（例如处理带空格或特殊字符的列名）。
        Map<String, String> columnNameMap = projectionColumn.getColumnNameMap();
        // originalColumnNames: 该表达式脚本中实际引用到的原始列名集合。使用 LinkedHashSet 是为了去重并保持解析顺序的一致性。
        LinkedHashSet<String> originalColumnNames =
                new LinkedHashSet<>(projectionColumn.getOriginalColumnNames());
        for (String originalColumnName : originalColumnNames) {
            // 如果引用的名称是原始表中的一个字段，则：
            // 从映射表中获取其对应的 Java 变量名加入 argumentNames。
            // 利用 DataTypeConverter 将 Flink 的逻辑数据类型（如 VARCHAR）转换为 Java 的物理类（如 String.class）加入 paramTypes。
            for (Column column : columns) {
                if (column.getName().equals(originalColumnName)) {
                    argumentNames.add(columnNameMap.get(originalColumnName));
                    paramTypes.add(DataTypeConverter.convertOriginalClass(column.getType()));
                    break;
                }
            }
            // 如果引用的是内置元数据（如 __table_name__），则从全局静态列表 METADATA_COLUMNS 中查找其定义的 Java 类型并添加。
            METADATA_COLUMNS.stream()
                    .filter(col -> col.f0.equals(originalColumnName))
                    .findFirst()
                    .ifPresent(
                            col -> {
                                argumentNames.add(columnNameMap.get(col.f0));
                                paramTypes.add(col.f2);
                            });
            // 如果引用的是特定数据源提供的元数据（如 MySQL 的 binlog 文件名），则从 supportedMetadataColumns 中获取。
            supportedMetadataColumns.entrySet().stream()
                    .filter(col -> col.getValue().getName().equals(originalColumnName))
                    .findFirst()
                    .ifPresent(
                            col -> {
                                argumentNames.add(columnNameMap.get(col.getValue().getName()));
                                paramTypes.add(col.getValue().getJavaClass());
                            });
        }

        argumentNames.add(JaninoCompiler.DEFAULT_TIME_ZONE);
        paramTypes.add(String.class);

        argumentNames.add(JaninoCompiler.DEFAULT_EPOCH_TIME);
        paramTypes.add(Long.class);

        return TransformExpressionKey.of(
                JaninoCompiler.loadSystemFunction(scriptExpression),
                argumentNames,
                paramTypes,
                DataTypeConverter.convertOriginalClass(projectionColumn.getDataType()),
                columnNameMap);
    }
}
