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

package org.apache.flink.cdc.runtime.parser;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptor;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Use Janino compiler to compiler the statement of flink cdc pipeline transform into the executable
 * code of Janino. For example, compiler 'string1 || string2' into 'concat(string1, string2)'. The
 * core logic is to traverse SqlNode tree and transform to Atom tree. Janino documents:
 * https://www.janino.net/index.html#properties
 */

// 主要任务是：将 Calcite 解析出来的 SQL 语法树（SqlNode）“翻译”并“编译”为高效的 Java 字节码。
// 用户在配置中写的 SQL 表达式（如 price * 1.1）在运行时会被转换成真正的 Java 方法调用，从而实现极高性能的数据转换。
// 代码翻译 (Transpilation)：将 SQL 语义的对象（如 SqlNode）转换为 Janino 能够理解的 Java 抽象语法树（Java.Rvalue）。例如，把 SQL 的 || 翻译成 Java 的 concat() 方法。
// 动态编译 (Just-In-Time Compilation)：利用 Janino 框架，在任务启动阶段将生成的 Java 表达式字符串编译成内存中的类。
// 桥接系统函数与 UDF：它负责将 SQL 中的内置函数（如 TO_DATE）和用户自定义函数（UDF）映射到 Flink 运行时的工具类方法上。

public class JaninoCompiler {
    // 定义了在处理 SQL 字面量时需要特殊忽略类型检查的类型（如 SYMBOL）
    private static final List<SqlTypeName> SQL_TYPE_NAME_IGNORE = Arrays.asList(SqlTypeName.SYMBOL);
    // 不依赖时区的时间函数（如 NOW, CURRENT_TIMESTAMP），翻译时只需传入当前时间戳。
    private static final List<String> TIMEZONE_FREE_TEMPORAL_FUNCTIONS =
            Arrays.asList("CURRENT_TIMESTAMP", "NOW");
    // 依赖时区的时间函数（如 CURRENT_DATE, UNIX_TIMESTAMP），翻译时会自动追加时区参数。
    private static final List<String> TIMEZONE_REQUIRED_TEMPORAL_FUNCTIONS =
            Arrays.asList(
                    "LOCALTIME",
                    "LOCALTIMESTAMP",
                    "CURRENT_TIME",
                    "CURRENT_DATE",
                    "UNIX_TIMESTAMP");
    private static final List<String> TIMEZONE_FREE_TEMPORAL_CONVERSION_FUNCTIONS =
            Collections.emptyList();

    // 需要时区的转换函数（如 TO_DATE, DATE_FORMAT），确保时间格式化符合指定时区。
    private static final List<String> TIMEZONE_REQUIRED_TEMPORAL_CONVERSION_FUNCTIONS =
            Arrays.asList(
                    "TO_DATE",
                    "TO_TIMESTAMP",
                    "FROM_UNIXTIME",
                    "TIMESTAMPADD",
                    "TIMESTAMPDIFF",
                    "TIMESTAMP_DIFF",
                    "DATE_FORMAT");
    // 内部变量占位符，代表系统处理时的“当前毫秒数”。
    public static final String DEFAULT_EPOCH_TIME = "__epoch_time__";
    // 内部变量占位符，代表系统配置的“时区ID”。
    public static final String DEFAULT_TIME_ZONE = "__time_zone__";
    // 在生成的表达式头部注入静态导入语句。
    // 它导入了 SystemFunctionUtils，使得 SQL 函数可以直接映射为 Java 的静态方法。
    public static String loadSystemFunction(String expression) {
        return "import static org.apache.flink.cdc.runtime.functions.SystemFunctionUtils.*;"
                + expression;
    }
    // 将 Java 源代码字符串正式编译为 ExpressionEvaluator
    // Flink CDC Transform 引擎的核心“发动机”。它的作用是利用 Janino 库，将已经翻译好的 Java 表达式字符串编译成内存中的字节码，从而让这些动态逻辑能以接近原生 Java 的速度运行。
    public static ExpressionEvaluator compileExpression(
            String expression, //  要编译的 Java 表达式字符串 (如 "arg0 + 1")
            List<String> argumentNames, // 参数名列表 (如 ["arg0"])
            List<Class<?>> argumentClasses, // 参数类型列表 (如 [Integer.class])
            Class<?> returnClass) { // 预期的返回类型 (如 Integer.class)
        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
        // toArray(new String[0]) 是 Java 的标准写法，将 List 转换为数组以符合接口要求。
        expressionEvaluator.setParameters(
                argumentNames.toArray(new String[0]), argumentClasses.toArray(new Class[0]));
        // 指定表达式计算结果的数据类型。
        expressionEvaluator.setExpressionType(returnClass);
        try {
            // 正式执行编译。
            // 将传入的 expression 字符串进行词法分析、语法分析，并最终直接生成 Java 字节码。
            expressionEvaluator.cook(expression);
            return expressionEvaluator;
        } catch (CompileException e) {
            throw new InvalidProgramException(
                    "Expression cannot be compiled. This is a bug. Please file an issue.\nExpression: "
                            + expression,
                    e);
        }
    }
    // 将 Calcite 解析出来的 SQL 抽象语法树节点（SqlNode） 转换成最终可以被编译的 Java 表达式字符串。
    public static String translateSqlNodeToJaninoExpression(
            SqlNode transform, // 输入：Calcite 的 SQL 语法树节点
            List<UserDefinedFunctionDescriptor> udfDescriptors, // 输入：用户自定义函数（UDF）的描述符列表
            Map<String, String> columnNameMap) { // 输入：列名映射表（如 "id" -> "arg0"）
        // Java.Rvalue: 这是 Janino 库中的类，代表一个“右值表达式”（即可以放在等号右边的代码片段）。
        Java.Rvalue rvalue =
                translateSqlNodeToJaninoRvalue(transform, udfDescriptors, columnNameMap);
        if (rvalue != null) {
            return rvalue.toString();
        }
        return "";
    }
    // 是 JaninoCompiler 中的核心路由（Dispatcher）。它利用多态和类型检查，将 Calcite 的 SqlNode 语法树节点精确地分流到对应的转换逻辑中。
    // 作用是 “类型分发”。它并不直接执行复杂的转换逻辑，而是识别当前 SQL 节点的具体类型（是列名？是常量？还是函数调用？），然后将其“委托”给专门的私有方法处理，并返回 Janino 能够理解的 Java.Rvalue 对象。
    public static Java.Rvalue translateSqlNodeToJaninoRvalue(
            SqlNode transform,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            Map<String, String> columnNameMap) {
        // 处理标识符 (SqlIdentifier)
        if (transform instanceof SqlIdentifier) {
            return translateSqlIdentifier((SqlIdentifier) transform, columnNameMap);
        // 处理基础调用 (SqlBasicCall)
        } else if (transform instanceof SqlBasicCall) {
            return translateSqlBasicCall((SqlBasicCall) transform, udfDescriptors, columnNameMap);
        // 处理条件分支 (SqlCase)
        } else if (transform instanceof SqlCase) {
            return translateSqlCase((SqlCase) transform, udfDescriptors, columnNameMap);
        // 处理字面量 (SqlLiteral)
        } else if (transform instanceof SqlLiteral) {
            return translateSqlSqlLiteral((SqlLiteral) transform);
        }
        return null;
    }
    // 根据标识符的名称，决定是将其映射为一个 Java 变量，还是转换成一个特定的时间函数调用。
    private static Java.Rvalue translateSqlIdentifier(
            SqlIdentifier sqlIdentifier, Map<String, String> columnNameMap) {
        // 获取标识符的最终名称
        String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
        // 时间函数分支处理
        // 无时区相关时间函数
        if (TIMEZONE_FREE_TEMPORAL_FUNCTIONS.contains(columnName.toUpperCase())) {
            return generateTimezoneFreeTemporalFunctionOperation(columnName);
        // 需时区相关时间函数 (TIMEZONE_REQUIRED_TEMPORAL_FUNCTIONS)
        } else if (TIMEZONE_REQUIRED_TEMPORAL_FUNCTIONS.contains(columnName.toUpperCase())) {
            return generateTimezoneRequiredTemporalFunctionOperation(columnName);
        // 时间转换函数 (带有 CONVERSION 后缀的列表)
        } else if (TIMEZONE_FREE_TEMPORAL_CONVERSION_FUNCTIONS.contains(columnName.toUpperCase())) {
            return generateTimezoneFreeTemporalConversionFunctionOperation(columnName);
        } else if (TIMEZONE_REQUIRED_TEMPORAL_CONVERSION_FUNCTIONS.contains(
                columnName.toUpperCase())) {
            return generateTimezoneRequiredTemporalConversionFunctionOperation(columnName);
        } else {
            // 处理真正的业务字段（如 id, name）
            // Java.AmbiguousName：这是 Janino 的类，代表一个“不确定的名称”。
            // 在编译阶段，Janino 会根据上下文将其解析为变量名、字段名或类名。对于 Flink CDC 来说，这里通常会被解析为预先定义好的参数名（如 argN）
            return new Java.AmbiguousName(
                    Location.NOWHERE,
                    new String[] {columnNameMap.getOrDefault(columnName, columnName)});
        }
    }

    private static Java.Rvalue translateSqlSqlLiteral(SqlLiteral sqlLiteral) {
        if (sqlLiteral.getValue() == null) {
            return new Java.NullLiteral(Location.NOWHERE);
        }
        String value = sqlLiteral.getValue().toString();
        if (sqlLiteral instanceof SqlCharStringLiteral) {
            // Double quotation marks represent strings in Janino.
            value = "\"" + value.substring(1, value.length() - 1) + "\"";
        } else if (sqlLiteral instanceof SqlNumericLiteral) {
            if (((SqlNumericLiteral) sqlLiteral).isInteger()) {
                long longValue = sqlLiteral.longValue(true);
                if (longValue > Integer.MAX_VALUE || longValue < Integer.MIN_VALUE) {
                    value += "L";
                }
            }
        }
        if (SQL_TYPE_NAME_IGNORE.contains(sqlLiteral.getTypeName())) {
            value = "\"" + value + "\"";
        }
        return new Java.AmbiguousName(Location.NOWHERE, new String[] {value});
    }
    // 负责处理 SQL 中的函数调用和运算符（例如 +, -, CONCAT, ABS 等），并将其转换为 Janino 能够识别的 Java 表达式对象。
    // 它的核心逻辑可以概括为：收集显式参数 -> 自动注入隐式系统参数（时间/时区） -> 映射为 Java 代码。
    private static Java.Rvalue translateSqlBasicCall(
            SqlBasicCall sqlBasicCall,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            Map<String, String> columnNameMap) {
        // 从当前的 SQL 调用节点中提取出所有的操作数（即参数）
        // 例子：如果 SQL 是 CONCAT(name, '!')，那么 operandList 就包含两个节点：name（字段标识符）和 '!'（字符串字面量）
        List<SqlNode> operandList = sqlBasicCall.getOperandList();
        List<Java.Rvalue> atoms = new ArrayList<>();
        // 这里的 "atoms" 指的是构成最终 Java 方法调用的各个参数
        // 遍历 SQL 参数，将它们递归地转换成 Java 表达式并存入 atoms 列表

        for (SqlNode sqlNode : operandList) {
            translateSqlNodeToAtoms(sqlNode, atoms, udfDescriptors, columnNameMap);
        }
        // 如果检测到是 NOW、CURRENT_TIMESTAMP 等函数，则自动向参数列表末尾添加一个隐藏参数：__epoch_time__
        if (TIMEZONE_FREE_TEMPORAL_FUNCTIONS.contains(
                sqlBasicCall.getOperator().getName().toUpperCase())) {
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME}));
        // 如果函数是 CURRENT_DATE 或 LOCALTIME 等，则自动注入两个参数：__epoch_time__ 和 __time_zone__
        } else if (TIMEZONE_REQUIRED_TEMPORAL_FUNCTIONS.contains(
                sqlBasicCall.getOperator().getName().toUpperCase())) {
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME}));
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE}));
        // 如果是 TO_DATE、FROM_UNIXTIME 等转换函数，则只注入 __time_zone__
        } else if (TIMEZONE_REQUIRED_TEMPORAL_CONVERSION_FUNCTIONS.contains(
                sqlBasicCall.getOperator().getName().toUpperCase())) {
            atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE}));
        }
        return sqlBasicCallToJaninoRvalue(
                sqlBasicCall, atoms.toArray(new Java.Rvalue[0]), udfDescriptors);
    }
    // 负责将 SQL 中的 CASE WHEN ... THEN ... ELSE ... END 语句转换为 Java 的嵌套三元运算符（Ternary Operator，即 condition ? trueValue : falseValue）
    private static Java.Rvalue translateSqlCase(
            SqlCase sqlCase,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            Map<String, String> columnNameMap) {
        SqlNodeList whenOperands = sqlCase.getWhenOperands(); // WHEN 条件列表
        SqlNodeList thenOperands = sqlCase.getThenOperands(); // THEN 结果列表
        SqlNode elseOperand = sqlCase.getElseOperand(); // ELSE 结果
        // 将所有的 WHEN 条件和 THEN 结果递归翻译成 Java 表达式对象并存入列表
        List<Java.Rvalue> whenAtoms = new ArrayList<>();
        for (SqlNode sqlNode : whenOperands) {
            translateSqlNodeToAtoms(sqlNode, whenAtoms, udfDescriptors, columnNameMap);
        }
        List<Java.Rvalue> thenAtoms = new ArrayList<>();
        for (SqlNode sqlNode : thenOperands) {
            translateSqlNodeToAtoms(sqlNode, thenAtoms, udfDescriptors, columnNameMap);
        }
        // 倒序构建嵌套三元运算符（核心算法）
        // 从最后一个 WHEN 开始，逐层向上包裹。
        Java.Rvalue elseAtoms =
                translateSqlNodeToJaninoRvalue(elseOperand, udfDescriptors, columnNameMap);
        Java.Rvalue sqlCaseRvalueTemp = elseAtoms;
        for (int i = whenAtoms.size() - 1; i >= 0; i--) {
            sqlCaseRvalueTemp =
                    new Java.ConditionalExpression(
                            Location.NOWHERE,
                            whenAtoms.get(i),
                            thenAtoms.get(i),
                            sqlCaseRvalueTemp);
        }
        // 给最终生成的整个嵌套表达式加上括号。
        return new Java.ParenthesizedExpression(Location.NOWHERE, sqlCaseRvalueTemp);
    }
    // 主要任务是将复杂的 SQL 节点展开，并将其转换后的 Java 表达式（Rvalue）填充到 atoms 列表中
    // 之所以叫 "Atoms"（原子），是因为它处理的是构成一个函数调用或运算的最基本单位。
    private static void translateSqlNodeToAtoms(
            SqlNode sqlNode,
            List<Java.Rvalue> atoms,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            Map<String, String> columnNameMap) {
        // 处理字段名或系统内置变量
        if (sqlNode instanceof SqlIdentifier) {
            atoms.add(translateSqlIdentifier((SqlIdentifier) sqlNode, columnNameMap));
        // 将 SQL 中的常量（如 '2023-01-01', 100, NULL）转换为 Java 对应的常量形式
        } else if (sqlNode instanceof SqlLiteral) {
            atoms.add(translateSqlSqlLiteral((SqlLiteral) sqlNode));
        // 处理函数嵌套或复合运算
        } else if (sqlNode instanceof SqlBasicCall) {
            atoms.add(translateSqlBasicCall((SqlBasicCall) sqlNode, udfDescriptors, columnNameMap));
        // 在某些 SQL 语法中（如 IN (1, 2, 3)），参数是以 SqlNodeList 形式存在的。
            // 这段代码循环遍历列表，将其中每一个元素都转换成 Java 表达式并添加到 atoms 列表中。
        } else if (sqlNode instanceof SqlNodeList) {
            for (SqlNode node : (SqlNodeList) sqlNode) {
                translateSqlNodeToAtoms(node, atoms, udfDescriptors, columnNameMap);
            }
        } else if (sqlNode instanceof SqlCase) {
            atoms.add(translateSqlCase((SqlCase) sqlNode, udfDescriptors, columnNameMap));
        }
    }

    // JaninoCompiler 中的终极映射表（Dispatcher）。
    // 它的职责是根据 Calcite 定义的 SQL 算子类型（SqlKind），将之前收集到的参数（atoms）组装成真正的 Java 语法结构。
    private static Java.Rvalue sqlBasicCallToJaninoRvalue(
            SqlBasicCall sqlBasicCall,
            Java.Rvalue[] atoms,
            List<UserDefinedFunctionDescriptor> udfDescriptors) {
        switch (sqlBasicCall.getKind()) {
            case AND:
                return generateBinaryOperation(sqlBasicCall, atoms, "&&");
            case OR:
                return generateBinaryOperation(sqlBasicCall, atoms, "||");
            case NOT:
                return generateUnaryOperation("!", atoms[0]);
            case EQUALS:
                return generateEqualsOperation(sqlBasicCall, atoms);
            case NOT_EQUALS:
                return generateUnaryOperation("!", generateEqualsOperation(sqlBasicCall, atoms));
            case IS_NULL:
                return generateUnaryOperation("null == ", atoms[0]);
            case IS_NOT_NULL:
                return generateUnaryOperation("null != ", atoms[0]);
            case IS_FALSE:
            case IS_NOT_TRUE:
                return generateUnaryOperation("false == ", atoms[0]);
            case IS_TRUE:
            case IS_NOT_FALSE:
                return generateUnaryOperation("true == ", atoms[0]);
            case BETWEEN:
            case IN:
            case NOT_IN:
            case LIKE:
            case CEIL:
            case FLOOR:
            case TRIM:
            case OTHER_FUNCTION:
                return generateOtherFunctionOperation(sqlBasicCall, atoms, udfDescriptors);
            case PLUS:
                return generateBinaryOperation(sqlBasicCall, atoms, "+");
            case MINUS:
                return generateBinaryOperation(sqlBasicCall, atoms, "-");
            case TIMES:
                return generateBinaryOperation(sqlBasicCall, atoms, "*");
            case DIVIDE:
                return generateBinaryOperation(sqlBasicCall, atoms, "/");
            case MOD:
                return generateBinaryOperation(sqlBasicCall, atoms, "%");
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN_OR_EQUAL:
                return generateCompareOperation(sqlBasicCall, atoms);
            case CAST:
                return generateCastOperation(sqlBasicCall, atoms);
            case TIMESTAMP_DIFF:
                return generateTimestampDiffOperation(sqlBasicCall, atoms);
            case TIMESTAMP_ADD:
                return generateTimestampAddOperation(sqlBasicCall, atoms);
            case OTHER:
                return generateOtherOperation(sqlBasicCall, atoms);
            default:
                throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
    }

    private static Java.Rvalue generateUnaryOperation(String operator, Java.Rvalue atom) {
        // 1. 源代码位置
        // 2. 运算符字符串
        // // 3. 唯一的操作数
        return new Java.UnaryOperation(Location.NOWHERE, operator, atom);
    }

    // 门负责将 SQL 中的二元运算符（如 +, -, *, /, &&, ||）转换为 Java 语言中的二元运算表达式。
    private static Java.Rvalue generateBinaryOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms, String operator) {
        if (atoms.length != 2) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        return new Java.BinaryOperation(Location.NOWHERE, atoms[0], operator, atoms[1]);
    }
    // JaninoCompiler 中处理 SQL 等值比较（=） 的核心函数。
    // 虽然在 Java 中我们习惯用 ==，但在处理 SQL 逻辑时，直接映射为 == 是非常危险的（因为 Java 的 == 对字符串比较的是地址而非内容）。
    // 因此，Flink CDC 采用了一种更稳妥的策略：将所有等值判断转化为一个专用的工具方法调用。
    private static Java.Rvalue generateEqualsOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 2) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        return new Java.MethodInvocation(
                Location.NOWHERE, null, StringUtils.convertToCamelCase("VALUE_EQUALS"), atoms);
    }
    // 是从 SQL 语法树中提取目标类型，并决定如何将原始数据转换为目标 Java 类型。
    private static Java.Rvalue generateCastOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        // 虽然 CAST 语法在 SQL 中看起来有两个部分（值和类型），但在 Janino 编译器的 atoms 收集阶段（即之前的 translateSqlNodeToAtoms），
        // 只有待转换的值（即 source_col）被当作表达式原子存入了 atoms。目标类型信息则保留在 sqlBasicCall 的操作数列表中。
        // 因此这里校验 atoms 长度必须为 1。
        if (atoms.length != 1) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        List<SqlNode> operandList = sqlBasicCall.getOperandList();
        SqlDataTypeSpec sqlDataTypeSpec = (SqlDataTypeSpec) operandList.get(1);
        return generateTypeConvertMethod(sqlDataTypeSpec, atoms);
    }
    // 专门负责处理 SQL 中的比较运算符（如 <、>、<=、>=）。
    // 与处理等值比较（=）类似，Flink CDC 并没有直接使用 Java 的原生符号（如 arg0 > arg1），而是将它们全部转化为对工具方法 compare 系列函数的调用。
    private static Java.Rvalue generateCompareOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 2) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        String compareMethodName;
        switch (sqlBasicCall.getKind()) {
            case LESS_THAN:
                compareMethodName = "LESS_THAN";
                break;
            case GREATER_THAN:
                compareMethodName = "GREATER_THAN";
                break;
            case LESS_THAN_OR_EQUAL:
                compareMethodName = "LESS_THAN_OR_EQUAL";
                break;
            case GREATER_THAN_OR_EQUAL:
                compareMethodName = "GREATER_THAN_OR_EQUAL";
                break;
            default:
                throw new ParseException(
                        "Unsupported binary relation operator: "
                                + sqlBasicCall.getKind().toString());
        }
        return new Java.MethodInvocation(
                Location.NOWHERE, null, StringUtils.convertToCamelCase(compareMethodName), atoms);
    }

    private static Java.Rvalue generateTimestampDiffOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 4) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        String timeIntervalUnit = atoms[0].toString().toUpperCase();
        switch (timeIntervalUnit) {
            case "\"SECOND\"":
            case "\"MINUTE\"":
            case "\"HOUR\"":
            case "\"DAY\"":
            case "\"MONTH\"":
            case "\"YEAR\"":
                break;
            default:
                throw new ParseException(
                        "Unsupported time interval unit in timestamp diff function: "
                                + timeIntervalUnit);
        }
        List<Java.Rvalue> timestampDiffFunctionParam = new ArrayList<>();
        timestampDiffFunctionParam.add(
                new Java.AmbiguousName(Location.NOWHERE, new String[] {timeIntervalUnit}));
        timestampDiffFunctionParam.add(atoms[1]);
        timestampDiffFunctionParam.add(atoms[2]);
        timestampDiffFunctionParam.add(atoms[3]);
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(sqlBasicCall.getOperator().getName()),
                timestampDiffFunctionParam.toArray(new Java.Rvalue[0]));
    }

    private static Java.Rvalue generateTimestampAddOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (atoms.length != 4) {
            throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
        }
        String timeIntervalUnit = atoms[0].toString().toUpperCase();
        switch (timeIntervalUnit) {
            case "\"SECOND\"":
            case "\"MINUTE\"":
            case "\"HOUR\"":
            case "\"DAY\"":
            case "\"MONTH\"":
            case "\"YEAR\"":
                break;
            default:
                throw new ParseException(
                        "Unsupported time interval unit in timestamp add function: "
                                + timeIntervalUnit);
        }
        List<Java.Rvalue> timestampDiffFunctionParam = new ArrayList<>();
        timestampDiffFunctionParam.add(
                new Java.AmbiguousName(Location.NOWHERE, new String[] {timeIntervalUnit}));
        timestampDiffFunctionParam.add(atoms[1]);
        timestampDiffFunctionParam.add(atoms[2]);
        timestampDiffFunctionParam.add(atoms[3]);
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(sqlBasicCall.getOperator().getName()),
                timestampDiffFunctionParam.toArray(new Java.Rvalue[0]));
    }

    private static Java.Rvalue generateCharLengthOperation(Java.Rvalue[] atoms) {
        return new Java.MethodInvocation(
                Location.NOWHERE, null, StringUtils.convertToCamelCase("CHAR_LENGTH"), atoms);
    }

    private static Java.Rvalue generateOtherOperation(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        if (sqlBasicCall.getOperator().getName().equals("||")) {
            return new Java.MethodInvocation(
                    Location.NOWHERE, null, StringUtils.convertToCamelCase("CONCAT"), atoms);
        }
        throw new ParseException("Unrecognized expression: " + sqlBasicCall.toString());
    }
    // 负责处理 SQL 中除了基础运算符（如 +, -）之外的所有函数调用，包括 SQL 内置函数、逻辑控制函数 以及 用户自定义函数 (UDF)。
    private static Java.Rvalue generateOtherFunctionOperation(
            SqlBasicCall sqlBasicCall,
            Java.Rvalue[] atoms,
            List<UserDefinedFunctionDescriptor> udfDescriptors) {
        // 获取 SQL 函数名（如 IF、CONCAT、MY_UDF）并统一转为大写，以便进行不区分大小写的匹配。
        String operationName = sqlBasicCall.getOperator().getName().toUpperCase();
        // 处理特殊的 IF 函数
        // 将 SQL 的 IF(cond, a, b) 转换为 Java 的三元运算符 cond ? a : b
        if (operationName.equals("IF")) {
            if (atoms.length == 3) {
                return new Java.ConditionalExpression(
                        Location.NOWHERE, atoms[0], atoms[1], atoms[2]);
            } else {
                throw new ParseException("Unrecognized expression: " + sqlBasicCall);
            }
        } else {
            // 处理自定义函数 (UDF)
            // 在用户提供的 UDF 列表中查找当前函数
            Optional<UserDefinedFunctionDescriptor> udfFunctionOptional =
                    udfDescriptors.stream()
                            .filter(e -> e.getName().equalsIgnoreCase(operationName))
                            .findFirst();
            return udfFunctionOptional
                    .map(
                            udfFunction ->
                                    new Java.MethodInvocation(
                                            Location.NOWHERE,
                                            null,
                                            // 通过 generateInvokeExpression 获取该 UDF 在 Java 里的调用路径，并将参数 atoms 传进去。
                                            generateInvokeExpression(udfFunction),
                                            atoms))
                    .orElseGet(
                            () ->
                                    new Java.MethodInvocation(
                                            Location.NOWHERE,
                                            null,
                                            // 如果既不是 IF 也不是 UDF，则视为标准的 SQL 内置函数
                                            StringUtils.convertToCamelCase(
                                                    sqlBasicCall.getOperator().getName()),
                                            atoms));
        }
    }

    // 专门用于处理不依赖时区的时间函数（如 NOW(), CURRENT_TIMESTAMP）的底层转换逻辑。
    // 核心作用是将一个 SQL 时间函数翻译成一个 Java 静态方法调用，并自动注入系统当前的毫秒级时间戳作为参数。
    private static Java.Rvalue generateTimezoneFreeTemporalFunctionOperation(String operationName) {
        return new Java.MethodInvocation(
                Location.NOWHERE, // 1. 位置信息，告知 Janino 编译器这段代码没有对应的源文件行列信息。
                null, // 表示该方法调用的目标对象。在 Java 中，如果调用目标为 null，通常意味着这是一个静态方法调用（依赖于静态导入）
                StringUtils.convertToCamelCase(operationName), // 将 SQL 风格的函数名转换为 Java 风格的驼峰命名。
                // SQL 中的 NOW() 是不带参数的，但在底层执行时，为了保证同一批次数据处理的时间一致性，Flink 会传入一个预定义的变量 __epoch_time__（由 DEFAULT_EPOCH_TIME 常量定义）。
                new Java.Rvalue[] {
                    new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME})
                });
    }
    // 处理依赖时区的时间函数（如 LOCALTIME, CURRENT_DATE, LOCALTIMESTAMP 等）的关键转换逻辑。
    // 与之前“不带时区”的方法相比，它的核心区别在于：它不仅注入了系统当前时间（__epoch_time__），还额外注入了时区信息（__time_zone__），
    // 以确保时间计算在不同的时区配置下都能得出正确的结果。
    private static Java.Rvalue generateTimezoneRequiredTemporalFunctionOperation(
            String operationName) {
        List<Java.Rvalue> timestampFunctionParam = new ArrayList<>();
        timestampFunctionParam.add(
                new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_EPOCH_TIME}));
        timestampFunctionParam.add(
                new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE}));
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(operationName),
                timestampFunctionParam.toArray(new Java.Rvalue[0]));
    }

    private static Java.Rvalue generateTimezoneFreeTemporalConversionFunctionOperation(
            String operationName) {
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(operationName),
                new Java.Rvalue[0]);
    }
    // 门为需要时区的“时间转换型”函数设计的转换逻辑
    // 与前面处理“获取当前时间”的函数（如 NOW()）不同，这类函数通常用于将已有的数据（如字符串、长整型）转换为时间格式，或者对时间进行格式化。
    private static Java.Rvalue generateTimezoneRequiredTemporalConversionFunctionOperation(
            String operationName) {
        return new Java.MethodInvocation(
                Location.NOWHERE,
                null,
                StringUtils.convertToCamelCase(operationName),
                new Java.Rvalue[] {
                    new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE})
                });
    }
    // 是 JaninoCompiler 中处理 数据类型转换（CAST） 的底层落地实现。
    // 它根据 SQL 的目标类型，将其映射为 SystemFunctionUtils 工具类中的具体转换方法。
    private static Java.Rvalue generateTypeConvertMethod(
            SqlDataTypeSpec sqlDataTypeSpec, Java.Rvalue[] atoms) {
        switch (sqlDataTypeSpec.getTypeName().getSimple().toUpperCase()) {
            case "BOOLEAN":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToBoolean", atoms);
            case "TINYINT":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToByte", atoms);
            case "SMALLINT":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToShort", atoms);
            case "INTEGER":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToInteger", atoms);
            case "BIGINT":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToLong", atoms);
            case "FLOAT":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToFloat", atoms);
            case "DOUBLE":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToDouble", atoms);
            case "DECIMAL":
                int precision = 10;
                int scale = 0;
                if (sqlDataTypeSpec.getTypeNameSpec() instanceof SqlBasicTypeNameSpec) {
                    SqlBasicTypeNameSpec typeNameSpec =
                            (SqlBasicTypeNameSpec) sqlDataTypeSpec.getTypeNameSpec();
                    if (typeNameSpec.getPrecision() > -1) {
                        precision = typeNameSpec.getPrecision();
                    }
                    if (typeNameSpec.getScale() > -1) {
                        scale = typeNameSpec.getScale();
                    }
                }
                List<Java.Rvalue> newAtoms = new ArrayList<>(Arrays.asList(atoms));
                newAtoms.add(
                        new Java.AmbiguousName(
                                Location.NOWHERE, new String[] {String.valueOf(precision)}));
                newAtoms.add(
                        new Java.AmbiguousName(
                                Location.NOWHERE, new String[] {String.valueOf(scale)}));
                return new Java.MethodInvocation(
                        Location.NOWHERE,
                        null,
                        "castToDecimalData",
                        newAtoms.toArray(new Java.Rvalue[0]));
            case "CHAR":
            case "VARCHAR":
            case "STRING":
                return new Java.MethodInvocation(Location.NOWHERE, null, "castToString", atoms);
            case "TIMESTAMP":
                List<Java.Rvalue> timestampAtoms = new ArrayList<>(Arrays.asList(atoms));
                timestampAtoms.add(
                        new Java.AmbiguousName(Location.NOWHERE, new String[] {DEFAULT_TIME_ZONE}));
                return new Java.MethodInvocation(
                        Location.NOWHERE,
                        null,
                        "castToTimestamp",
                        timestampAtoms.toArray(new Java.Rvalue[0]));
            default:
                throw new ParseException(
                        "Unsupported data type cast: " + sqlDataTypeSpec.toString());
        }
    }
    // 处理 用户自定义函数 (UDF) 的核心逻辑。它的作用是构造出一个能够正确调用 UDF 实例的 Java 表达式字符串。
    // __instanceOf%s：这是 Flink CDC 内部约定的变量命名规则。如果你定义了一个类名叫 MyFunction 的 UDF，对应的实例变量名就是 __instanceOfMyFunction。
    private static String generateInvokeExpression(UserDefinedFunctionDescriptor udfFunction) {
        // 判断用户在注册 UDF 时是否显式提供了返回类型的暗示
        if (udfFunction.getReturnTypeHint() != null) {
            return String.format(
                    "(%s) __instanceOf%s.eval",
                    DataTypeConverter.convertOriginalClass(udfFunction.getReturnTypeHint())
                            .getCanonicalName(),
                    udfFunction.getClassName());
        } else {
            return String.format("__instanceOf%s.eval", udfFunction.getClassName());
        }
    }
}
