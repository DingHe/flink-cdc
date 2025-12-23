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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The key applies to describe the information of the transformation expression.
 *
 * <p>A transform expression key contains:
 *
 * <ul>
 *   <li>expression: a string for the transformation expression.
 *   <li>argumentNames: a list for the argument names in expression.
 *   <li>argumentClasses: a list for the argument classes in expression.
 *   <li>returnClass: a class for the return class in expression
 *   <li>columnNameMap: a map whose key is the original column name and value is the mapped column
 *       name
 * </ul>
 */
// TransformExpressionKey 是一个至关重要的 元数据描述符 和 缓存键（Cache Key）。
// 它决定了转换表达式（Transform Expression）如何被唯一标识以及如何被编译成可执行代码。
// 定义“方法签名”：在将 SQL 字符串编译为 Java 字节码之前，必须明确知道这个“函数”长什么样。它封装了表达式的输入参数（名与类型）、计算逻辑（脚本）以及期望的返回类型。
// 高性能缓存标识：Janino 编译字节码是一个相对昂贵的操作。Flink CDC 使用这个类作为缓存的 Key。当多张表或多个列拥有完全相同的 TransformExpressionKey（即逻辑、参数、返回类型完全一致）时，系统可以直接复用已经编译好的 ExpressionEvaluator，而无需重新编译，极大提升了初始化速度。
public class TransformExpressionKey implements Serializable {
    private static final long serialVersionUID = 1L;
    // 计算逻辑文本：经过系统处理后的计算表达式（例如 id + 1 或 UPPER(name)）。它是编译后方法的方法体核心。
    private final String expression;
    // 参数名列表：定义了表达式中引用的变量名（如 ["id", "name"]）。这决定了编译生成的 Java 方法中的形参名称。
    private final List<String> argumentNames;
    // 参数类型列表：与参数名一一对应，定义了每个输入变量的 Java 类型（如 [Integer.class, String.class]）。这是确保字节码类型安全的关键。
    private final List<Class<?>> argumentClasses;
    // 返回类型：定义了表达式计算结果的 Java 类型。例如，如果目标列是 BIGINT，则此属性为 Long.class。
    private final Class<?> returnClass;
    // 列名映射表：记录原始 SQL 列名与内部变量名之间的映射。这对于处理包含特殊字符或保留字的列名非常重要，确保在代码生成时不会出现语法错误。
    private final Map<String, String> columnNameMap;

    private TransformExpressionKey(
            String expression,
            List<String> argumentNames,
            List<Class<?>> argumentClasses,
            Class<?> returnClass,
            Map<String, String> columnNameMap) {
        this.expression = expression;
        this.argumentNames = argumentNames;
        this.argumentClasses = argumentClasses;
        this.returnClass = returnClass;
        this.columnNameMap = columnNameMap;
    }

    public String getExpression() {
        return expression;
    }

    public List<String> getArgumentNames() {
        return argumentNames;
    }

    public List<Class<?>> getArgumentClasses() {
        return argumentClasses;
    }

    public Class<?> getReturnClass() {
        return returnClass;
    }

    public Map<String, String> getColumnNameMap() {
        return columnNameMap;
    }

    public static TransformExpressionKey of(
            String expression,
            List<String> argumentNames,
            List<Class<?>> argumentClasses,
            Class<?> returnClass,
            Map<String, String> columnNameMap) {
        return new TransformExpressionKey(
                expression, argumentNames, argumentClasses, returnClass, columnNameMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransformExpressionKey that = (TransformExpressionKey) o;
        return expression.equals(that.expression)
                && argumentNames.equals(that.argumentNames)
                && argumentClasses.equals(that.argumentClasses)
                && returnClass.equals(that.returnClass)
                && columnNameMap.equals(that.columnNameMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, argumentNames, argumentClasses, returnClass, columnNameMap);
    }

    @Override
    public String toString() {
        return "TransformExpressionKey{"
                + "expression='"
                + expression
                + '\''
                + ", argumentNames="
                + argumentNames
                + ", argumentClasses="
                + argumentClasses
                + ", returnClass="
                + returnClass
                + ", columnNameMap="
                + columnNameMap
                + '}';
    }
}
