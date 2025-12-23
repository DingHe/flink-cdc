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

import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.cdc.runtime.parser.TransformParser;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The TransformFilter applies to describe the information of the filter row.
 *
 * <p>A filter row contains:
 *
 * <ul>
 *   <li>expression: a string for filter expression split from the user-defined filter.
 *   <li>scriptExpression: a string for filter script expression compiled from the column
 *       expression.
 *   <li>columnNames: a list for recording the name of all columns used by the filter expression.
 * </ul>
 */
// TransformFilter 是 Flink CDC 转换算子（Transform Operator）中专门用于处理 行级过滤（Row Filtering） 逻辑的描述类。它负责存储和管理用户定义的过滤条件，并将其从“人类可读的表达式”转换为“机器可执行的代码脚本”。
// 在 Flink CDC 的数据流中，TransformFilter 扮演着“安检员”的角色：
// 承载过滤定义：存储用户在 YAML 配置文件中编写的过滤表达式（例如 age > 18 AND status = 'active'）。
// 表达式转换：将类 SQL 的过滤表达式转换为 Janino（一种高性能 Java 编译器）能够运行的脚本表达式。
// 依赖追踪：记录过滤条件中引用了哪些原始列（ColumnNames），以便算子在运行时准确提取数据进行计算。
// 提供运行元数据：为转换算子提供在运行时判定一行数据是否该被保留（Keep）或丢弃（Drop）的所有必要信息。
public class TransformFilter implements Serializable {
    private static final long serialVersionUID = 1L;
    // 存储用户输入的原始过滤表达式
    private final String expression;
    // 存储转换后的 Java/Janino 脚本表达式。
    // 由于原始表达式不能直接运行，解析器会将其中的列名替换为变量，并将运算符转换为 Java 语法。
    private final String scriptExpression;
    // 过滤条件中涉及到的所有列名列表。
    // 算子需要根据这个列表从二进制数据中提前提取出对应的字段值。
    private final List<String> columnNames;
    // 存储原始列名与脚本中使用的变量名之间的映射关系。
    // 防止列名中包含特殊字符（如空格、连字符）导致脚本解析错误，通常会将列名映射为安全的内部变量（如 arg0, arg1）。
    private final Map<String, String> columnNameMap;

    public TransformFilter(
            String expression,
            String scriptExpression,
            List<String> columnNames,
            Map<String, String> columnNameMap) {
        this.expression = expression;
        this.scriptExpression = scriptExpression;
        this.columnNames = columnNames;
        this.columnNameMap = columnNameMap;
    }

    public String getExpression() {
        return expression;
    }

    public String getScriptExpression() {
        return scriptExpression;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public Map<String, String> getColumnNameMap() {
        return columnNameMap;
    }

    public String getColumnNameMapAsString() {
        return TransformException.prettyPrintColumnNameMap(getColumnNameMap());
    }

    public static Optional<TransformFilter> of(
            String filterExpression, List<UserDefinedFunctionDescriptor> udfDescriptors) {
        if (StringUtils.isNullOrWhitespaceOnly(filterExpression)) {
            return Optional.empty();
        }
        List<String> columnNames = TransformParser.parseFilterColumnNameList(filterExpression);
        Map<String, String> columnNameMap = TransformParser.generateColumnNameMap(columnNames);
        String scriptExpression =
                TransformParser.translateFilterExpressionToJaninoExpression(
                        filterExpression, udfDescriptors, columnNameMap);
        return Optional.of(
                new TransformFilter(
                        filterExpression, scriptExpression, columnNames, columnNameMap));
    }

    public boolean isValid() {
        return !columnNames.isEmpty();
    }

    @Override
    public String toString() {
        return "TransformFilter{"
                + "expression='"
                + expression
                + '\''
                + ", scriptExpression='"
                + scriptExpression
                + '\''
                + ", columnNames="
                + columnNames
                + ", columnNameMap="
                + columnNameMap
                + '}';
    }
}
