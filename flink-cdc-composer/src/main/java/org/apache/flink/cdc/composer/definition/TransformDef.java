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

package org.apache.flink.cdc.composer.definition;

import org.apache.flink.cdc.common.utils.StringUtils;

import java.util.Objects;

/**
 * Definition of a transformation.
 *
 * <p>A transformation definition contains:
 *
 * <ul>
 *   <li>sourceTable: a regex pattern for matching input table IDs. Required for the definition.
 *   <li>projection: a string for projecting the row of matched table as output. Optional for the
 *       definition.
 *   <li>filter: a string for filtering the row of matched table as output. Optional for the
 *       definition.
 *   <li>primaryKeys: a string for primary key columns for matching input table IDs, seperated by
 *       `,`. Optional for the definition.
 *   <li>partitionKeys: a string for partition key columns for matching input table IDs, seperated
 *       by `,`. Optional for the definition.
 *   <li>tableOptions: a string for table options for matching input table IDs, options are
 *       seperated by `,`, key and value are seperated by `=`. Optional for the definition.
 *   <li>description: description for the transformation. Optional for the definition.
 * </ul>
 */
// TransformDef 类的核心作用是以结构化的方式定义一个或多个表上的数据处理逻辑。
// 在 Flink CDC 流水线中，转换（Transform）是可选的步骤，它允许用户在不编写 Flink 代码的情况下，
// 通过类 SQL 的语法对变更数据记录 (Change Data Records) 进行操作。
// 定义了对哪些表执行哪些数据转换操作：
// 表匹配 (SourceTable): 使用正则表达式匹配需要应用转换规则的输入表 ID。
// 字段操作 (Projection/Filter): 定义对行数据进行列选择 (projection) 和行过滤 (filter) 的逻辑。
// 元数据修改 (Keys/Options): 定义对表的元数据（如主键、分区键、表选项）进行覆盖和修改的规则。
public class TransformDef {
    // 源表匹配模式。
    // 这是一个正则表达式 (regex pattern)，用于匹配需要应用此转换规则的输入表 ID。这是定义转换规则的必需部分。
    private final String sourceTable;
    // 字段投影表达式。
    // 可选。定义对行中列进行选择和重命名的逻辑（类似于 SQL 的 SELECT 子句），通常是逗号分隔的列名或表达式。如果为 null 或空白，则表示不进行投影操作。
    private final String projection;
    // 行过滤表达式。
    // 可选。定义对行数据进行过滤的条件（类似于 SQL 的 WHERE 子句），只有满足条件的行才会被传递到下游。如果为 null 或空白，则表示不进行过滤。
    private final String filter;
    // 转换规则的描述。
    // 可选。用于对该转换规则提供说明或备注。
    private final String description;
    // 主键列定义。
    // 可选。逗号分隔的列名字符串，用于覆盖或设置匹配表的主键定义。
    private final String primaryKeys;
    // 分区键定义。
    // 可选。逗号分隔的列名字符串，用于覆盖或设置匹配表的分区键定义。
    private final String partitionKeys;
    // 表选项定义。
    // 可选。一个逗号分隔的键值对字符串（例如 key1=value1,key2=value2），用于覆盖或设置匹配表的额外表选项。
    private final String tableOptions;
    // 后置转换器。
    // 这是一个额外的、可选的转换器类路径，用于在执行完核心转换（投影/过滤）后，对数据进行进一步的自定义处理。
    private final String postTransformConverter;

    public TransformDef(
            String sourceTable,
            String projection,
            String filter,
            String primaryKeys,
            String partitionKeys,
            String tableOptions,
            String description,
            String postTransformConverter) {
        this.sourceTable = sourceTable;
        this.projection = projection;
        this.filter = filter;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.tableOptions = tableOptions;
        this.description = description;
        this.postTransformConverter = postTransformConverter;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getProjection() {
        return projection;
    }

    public boolean isValidProjection() {
        return !StringUtils.isNullOrWhitespaceOnly(projection);
    }

    public String getFilter() {
        return filter;
    }

    public boolean isValidFilter() {
        return !StringUtils.isNullOrWhitespaceOnly(filter);
    }

    public String getDescription() {
        return description;
    }

    public String getPrimaryKeys() {
        return primaryKeys;
    }

    public String getPartitionKeys() {
        return partitionKeys;
    }

    public String getTableOptions() {
        return tableOptions;
    }

    public String getPostTransformConverter() {
        return postTransformConverter;
    }

    @Override
    public String toString() {
        return "TransformDef{"
                + "sourceTable='"
                + sourceTable
                + '\''
                + ", projection='"
                + projection
                + '\''
                + ", filter='"
                + filter
                + '\''
                + ", description='"
                + description
                + '\''
                + ", postTransformConverter='"
                + postTransformConverter
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransformDef that = (TransformDef) o;
        return Objects.equals(sourceTable, that.sourceTable)
                && Objects.equals(projection, that.projection)
                && Objects.equals(filter, that.filter)
                && Objects.equals(description, that.description)
                && Objects.equals(primaryKeys, that.primaryKeys)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(tableOptions, that.tableOptions)
                && Objects.equals(postTransformConverter, that.postTransformConverter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sourceTable,
                projection,
                filter,
                description,
                primaryKeys,
                partitionKeys,
                tableOptions,
                postTransformConverter);
    }
}
