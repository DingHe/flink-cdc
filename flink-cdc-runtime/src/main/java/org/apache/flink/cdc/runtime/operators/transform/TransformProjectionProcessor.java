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
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.runtime.parser.TransformParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The processor of transform projection applies to process a row of filtering tables.
 *
 * <p>A transform projection processor contains:
 *
 * <ul>
 *   <li>CreateTableEvent: add the user-defined computed columns into Schema.
 *   <li>SchemaChangeEvent: update the columns of TransformProjection.
 *   <li>DataChangeEvent: Fill data field to row in PreTransformOperator. Process the data column
 *       and the user-defined expression computed columns.
 * </ul>
 */
// TransformProjectionProcessor 是处理 “投影（Projection）” 逻辑的物理执行引擎。如果说 PostTransformer 存储的是逻辑意图（做什么），那么这个类就是具体的执行者（怎么做）。
// TransformProjectionProcessor 的主要作用是将原始数据行（Row）转换成目标投影后的数据行。
// 物理字段映射：将原始表中的列搬运到结果集的指定位置。
// 表达式计算：实时计算用户定义的列（如 price * count）
// 常量填充：向结果集中填充固定值或元数据（如数据库名、当前系统时间）。
// UDF 调用：在计算过程中调度用户自定义函数。

public class TransformProjectionProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TransformProjectionProcessor.class);
    // 包含转换前后的 Schema 对比信息及字段索引映射。它是查找字段物理位置的“指南”。
    private final PostTransformChangeInfo changeInfo;
    // 用户定义的 SQL 风格投影表达式（例如 id, UPPER(name), price * 1.1 AS new_price）。
    private final String projectionExpression;
    // 处理时间相关计算（如 CURRENT_TIMESTAMP）时使用的时区。
    private final String timezone;
    // UDF 的描述符集合，定义了函数的名称和类路径。
    private final List<UserDefinedFunctionDescriptor> udfDescriptors;
    // 已实例化的 UDF 对象，用于在计算时直接调用。
    private final List<Object> udfFunctionInstances;
    // 最核心的属性。
    // 这是一个执行器列表，列表中的每个元素对应输出表的一列。每一列都有自己独立的处理器（可能是引用原始列，也可能是计算表达式）。
    private final List<ProjectionColumnProcessor> columnProcessors;
    // 当前系统支持的元数据列定义列表。
    private final SupportedMetadataColumn[] supportedMetadataColumns;
    // 元数据列的快速查找表（Map 结构），用于在解析表达式时快速匹配元数据字段名。
    private final Map<String, SupportedMetadataColumn> supportedMetadataColumnsMap;

    public TransformProjectionProcessor(
            PostTransformChangeInfo changeInfo,
            String projectionExpression,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        this.changeInfo = changeInfo;
        this.projectionExpression = projectionExpression;
        this.timezone = timezone;
        this.udfDescriptors = udfDescriptors;
        this.udfFunctionInstances = udfFunctionInstances;
        this.supportedMetadataColumns = supportedMetadataColumns;

        // Construct a mapping table ad-hoc to accelerate looking-up
        Map<String, SupportedMetadataColumn> supportedMetadataColumnsMap = new HashMap<>();
        for (SupportedMetadataColumn supportedMetadataColumn : supportedMetadataColumns) {
            supportedMetadataColumnsMap.put(
                    supportedMetadataColumn.getName(), supportedMetadataColumn);
        }
        this.supportedMetadataColumnsMap = supportedMetadataColumnsMap;
        this.columnProcessors = createProjectionColumnProcessors();
    }

    public Object[] project(Object[] rowData, TransformContext context) {
        return columnProcessors.stream()
                .map(processor -> processor.evaluate(rowData, context))
                .toArray();
    }
    // 职责是将用户编写的 SQL 投影字符串 转化为一组 可立即执行的 Java 对象（Processors）
    //
    private List<ProjectionColumnProcessor> createProjectionColumnProcessors() {
        // changeInfo 包含了转换前后的 Schema 信息。
        // 如果没有这些元数据，处理器就无法知道原始数据的结构，也无法完成物理索引的映射。
        // 如果为空，程序会直接抛出异常，防止后续逻辑出错。
        Preconditions.checkNotNull(
                changeInfo,
                "Projection column processors could only be created if changeInfo is available.");

        List<ProjectionColumn> projectionColumns =
                TransformParser.generateProjectionColumns(
                        projectionExpression,
                        changeInfo.getPreTransformedSchema().getColumns(),
                        udfDescriptors,
                        supportedMetadataColumns);

        List<ProjectionColumnProcessor> columnProcessors =
                projectionColumns.stream()
                        .map(
                                column ->
                                        ProjectionColumnProcessor.of(
                                                changeInfo,
                                                column,
                                                timezone,
                                                udfDescriptors,
                                                udfFunctionInstances,
                                                supportedMetadataColumnsMap))
                        .collect(Collectors.toList());

        LOG.info("Successfully created projection column processors cache.");
        LOG.info("Cached results: {}", columnProcessors);
        return columnProcessors;
    }
}
