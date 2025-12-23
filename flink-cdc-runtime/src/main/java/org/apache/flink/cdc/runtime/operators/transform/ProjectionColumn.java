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
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The ProjectionColumn applies to describe the information of the transformation column. If it only
 * has column info, it describes the data column. If it has column info and expression info, it
 * describes the user-defined computed columns.
 *
 * <p>A projection column contains:
 *
 * <ul>
 *   <li>column: column information parsed from projection.
 *   <li>expression: a string for column expression split from the user-defined projection.
 *   <li>scriptExpression: a string for column script expression compiled from the column
 *       expression.
 *   <li>originalColumnNames: a list for recording the name of all columns used by the column
 *       expression.
 * </ul>
 */
// Flink CDC 转换（Transform）框架中用于描述目标表单个列转换逻辑的核心实体类。它承载了从源表字段到目标表字段的映射关系、计算逻辑以及元数据信息。
// 在 Flink CDC 的 projection（投影）处理过程中，用户定义的每一个输出列都会对应一个 ProjectionColumn 实例。它的主要作用包括：
// 定义目标列元数据：规定了转换后该列的名称、数据类型（DataType）以及是否为主键等信息。
// 区分转换类型：它能识别并处理三种不同类型的转换：
// 直接透传：原封不动地引用源表字段。
// 别名转换：仅仅改变字段名称，不改变值逻辑（AS 语法）。
// 计算列转换：通过表达式、函数或数学运算生成新值。
// 连接计算引擎：它存储了经过解析和翻译后的 Janino 脚本表达式（Java 代码片段），供算子在运行时动态编译并执行计算。

public class ProjectionColumn implements Serializable {
    private static final long serialVersionUID = 1L;
    // 存储目标列的物理结构信息。
    // 包含：列名、数据类型、是否允许为空、长度等。它是构建下游目标表 Schema 的直接依据。
    private final Column column;
    // 存储用户在投影配置中定义的原始表达式。
    // 在 age + 1 AS new_age 中，expression 可能是 age + 1
    private final String expression;
    // 存储转换后的 Java/Janino 脚本代码。
    // 算子不会直接运行 SQL 表达式，而是运行这里的 Java 代码片段。例如 age + 1 可能会被翻译成 arg0 + 1（arg0 是变量映射）
    private final String scriptExpression;
    // 记录该列计算时依赖的所有原始源表列名。
    private final List<String> originalColumnNames;
    // 维护原始列名与脚本中变量名的映射关系。
    // 确保在生成的 Java 脚本中，变量能准确引用到对应的字段值（防止因字段名特殊字符导致的语法错误）。
    private final Map<String, String> columnNameMap;

    public ProjectionColumn(
            Column column,
            String expression,
            String scriptExpression,
            List<String> originalColumnNames,
            Map<String, String> columnNameMap) {
        this.column = column;
        this.expression = expression;
        this.scriptExpression = scriptExpression;
        this.originalColumnNames = originalColumnNames;
        this.columnNameMap = columnNameMap;
    }

    public ProjectionColumn copy() {
        return new ProjectionColumn(
                column.copy(column.getName()),
                expression,
                scriptExpression,
                new ArrayList<>(originalColumnNames),
                new HashMap<>(columnNameMap));
    }

    public Column getColumn() {
        return column;
    }

    public String getColumnName() {
        return column.getName();
    }

    public DataType getDataType() {
        return column.getType();
    }

    public String getScriptExpression() {
        return scriptExpression;
    }

    public List<String> getOriginalColumnNames() {
        return originalColumnNames;
    }

    public Map<String, String> getColumnNameMap() {
        return columnNameMap;
    }

    public String getColumnNameMapAsString() {
        return TransformException.prettyPrintColumnNameMap(getColumnNameMap());
    }

    // 判断这是否是一个需要执行逻辑转换的列。
    // 如果 scriptExpression 不为空，说明需要经过计算引擎处理。
    public boolean isValidTransformedProjectionColumn() {
        return !StringUtils.isNullOrWhitespaceOnly(scriptExpression);
    }

    /**
     * This projection is created with a plain column name. <br>
     * Just like column {@code id} in {@code id, name AS new_name, age + 1 AS new_age}. <br>
     * Comments and default expressions will be intact.
     */
    // 直接透传。
    // 例如 SELECT id
    // 会保留原始列的所有属性（如注释、默认值）
    public static ProjectionColumn ofForwarded(Column column, String mappedColumnName) {
        String name = column.getName();
        Map<String, String> columnNameMap = Collections.singletonMap(name, mappedColumnName);
        return new ProjectionColumn(
                column, name, mappedColumnName, Collections.singletonList(name), columnNameMap);
    }

    /**
     * This projection is created with a simple $id$ AS $new_id$ expression. <br>
     * Just like column {@code new_name} in {@code id, name AS new_name, age + 1 AS new_age}. <br>
     * Comments and default expressions will be intact.
     */
    // 简单的别名映射。
    // 例如 SELECT name AS nick_name
    public static ProjectionColumn ofAliased(
            Column column, String newName, String mappedColumnName) {
        String originalName = column.getName();
        Map<String, String> columnNameMap =
                Collections.singletonMap(originalName, mappedColumnName);
        return new ProjectionColumn(
                column.copy(newName),
                originalName,
                mappedColumnName,
                Collections.singletonList(originalName),
                columnNameMap);
    }

    /**
     * This projection is created with a complex calculation expression. <br>
     * Just like column {@code new_age} in {@code id, name AS new_name, age + 1 AS new_age}. <br>
     * No comments nor default expressions will be kept.
     */
    // 复杂的计算列。
    // 例如 SELECT UPPER(name), age * 2
    public static ProjectionColumn ofCalculated(
            String columnName,
            DataType dataType,
            String expression,
            String scriptExpression,
            List<String> originalColumnNames,
            Map<String, String> columnNameMap) {
        return new ProjectionColumn(
                Column.physicalColumn(columnName, dataType),
                expression,
                scriptExpression,
                originalColumnNames,
                columnNameMap);
    }

    @Override
    public String toString() {
        return "ProjectionColumn{"
                + "column="
                + column
                + ", expression='"
                + expression
                + '\''
                + ", scriptExpression='"
                + scriptExpression
                + '\''
                + ", originalColumnNames="
                + originalColumnNames
                + ", columnNameMap="
                + columnNameMap
                + '}';
    }
}
