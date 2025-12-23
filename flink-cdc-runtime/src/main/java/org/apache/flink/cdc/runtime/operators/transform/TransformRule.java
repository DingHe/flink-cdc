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
import org.apache.flink.cdc.common.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;

/** A rule defining pre-transformations where filtered rows and irrelevant columns are removed. */
// TransformRule 是 Flink CDC Pipeline 框架中一个非常重要的配置载体类。
// 它主要用于存储和传递用户定义的转换逻辑，是连接“用户配置”与“算子执行”之间的桥梁。
// TransformRule 的核心作用是定义一套转换规则的“蓝图”。
// 在 Flink CDC 的 YAML 配置文件中，用户可以定义 transforms 块，指定哪些表需要进行投影（Projection）、过滤（Filter）或者修改主键。TransformRule 就是这些配置在内存中的具体表现形式。
// 它主要服务于 PreTransformOperator 和 PostTransformOperator：
// 在 Pre-Transform 阶段：算子读取 TransformRule 来决定哪些列需要被保留（列剪枝），以及是否需要修改表结构的元数据（如主键）。
// 在 Post-Transform 阶段：算子根据 TransformRule 中的表达式进行实际的行过滤和计算操作。
public class TransformRule implements Serializable {
    private static final long serialVersionUID = 1L;
    // 指定该规则适用于哪些表。
    // 通常支持通配符或正则表达式（例如 db.table_.*）。
    private final String tableInclusions;
    // 定义投影逻辑，即“选出哪些列”以及“如何计算新列”。
    private final @Nullable String projection;
    // 定义行过滤逻辑，即“保留哪些数据行”。
    private final @Nullable String filter;
    // 重新定义目标表的主键。
    // 在某些场景下，上游表可能没有主键，或者下游需要以不同的维度作为主键，可以通过此属性强制指定。
    private final String primaryKey;
    // 定义目标表的分区键。
    // 主要用于写入类似 Iceberg、Paimon 等支持分区的下游存储系统。
    private final String partitionKey;
    // 传递表级别的额外配置选项。
    private final String tableOption;
    // 指定后置转换器。
    // 这通常是一个内部使用的类路径或标识符，用于在常规投影计算完成后，对数据进行最后的格式转换或处理
    private final @Nullable String postTransformConverter;
    // 记录源端支持的元数据列（如数据库名、表名、操作时间等）。
    // 这确保了在执行转换表达式时，算子知道哪些元数据是可以被引用和计算的。
    private final SupportedMetadataColumn[] supportedMetadataColumns;

    public TransformRule(
            String tableInclusions,
            @Nullable String projection,
            @Nullable String filter,
            String primaryKey,
            String partitionKey,
            String tableOption,
            @Nullable String postTransformConverter,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        this.tableInclusions = tableInclusions;
        this.projection = StringUtils.isNullOrWhitespaceOnly(projection) ? "*" : projection;
        this.filter = filter;
        this.primaryKey = primaryKey;
        this.partitionKey = partitionKey;
        this.tableOption = tableOption;
        this.postTransformConverter = postTransformConverter;
        this.supportedMetadataColumns = supportedMetadataColumns;
    }

    public String getTableInclusions() {
        return tableInclusions;
    }

    @Nullable
    public String getProjection() {
        return projection;
    }

    @Nullable
    public String getFilter() {
        return filter;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public String getTableOption() {
        return tableOption;
    }

    @Nullable
    public String getPostTransformConverter() {
        return postTransformConverter;
    }

    public SupportedMetadataColumn[] getSupportedMetadataColumns() {
        return supportedMetadataColumns;
    }
}
