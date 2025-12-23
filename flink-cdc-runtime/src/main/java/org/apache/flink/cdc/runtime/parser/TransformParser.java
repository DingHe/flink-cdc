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

import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.runtime.operators.transform.ProjectionColumn;
import org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptor;
import org.apache.flink.cdc.runtime.parser.metadata.TransformSchemaFactory;
import org.apache.flink.cdc.runtime.parser.metadata.TransformSqlOperatorTable;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.common.utils.StringUtils.isNullOrWhitespaceOnly;
import static org.apache.flink.cdc.runtime.parser.metadata.MetadataColumns.METADATA_COLUMNS;
import static org.apache.flink.cdc.runtime.typeutils.DataTypeConverter.convertCalciteType;

/** Use Flink's calcite parser to parse the statement of flink cdc pipeline transform. */
// TransformParser 是 Flink CDC 转换（Transform）框架中的“大脑”。
// 它利用 Apache Calcite 作为底层的 SQL 解析引擎，将用户编写的类似 SQL 的字符串（投影、过滤规则）解析、验证并转化为 Flink CDC 内部的可执行模型
// 在 Flink CDC Pipeline 中，用户可以使用 SQL 语法定义数据转换逻辑。TransformParser 的核心任务是：
// 语法解析：将原始字符串转化为抽象语法树（AST）。
// 语义验证：检查用户引用的列名、函数名是否合法，是否存在类型错误。
// 类型推导：确定每一个计算表达式（如 a + b）产生的最终数据类型（INT, STRING 等）。
// 模型构建：将 SQL 片段映射为 ProjectionColumn 等对象，并生成用于运行时执行的 Janino 脚本。


public class TransformParser {
    private static final Logger LOG = LoggerFactory.getLogger(TransformParser.class);
    // 虚拟的数据库名（"default_schema"）。
    // Calcite 验证 SQL 需要完整的上下文。
    private static final String DEFAULT_SCHEMA = "default_schema";
    // 虚拟的表名（"TB"）。将源表字段模拟为此表的字段，以便进行 SQL 校验。
    private static final String DEFAULT_TABLE = "TB";
    // 内部变量前缀 $。
    // 用于 Janino 编译时，将列名替换为 $0, $1 以避开非法字符。
    private static final String MAPPED_COLUMN_NAME_PREFIX = "$";
    // 常量 $0。通常用于处理单列引用的场景。
    private static final String MAPPED_SINGLE_COLUMN_NAME = MAPPED_COLUMN_NAME_PREFIX + "0";
    // 配置并返回一个 Calcite 解析器。
    // 设定了 MySQL 5 的语法兼容模式，并开启大小写敏感及 Java 词法解析（Lex.JAVA）。
    // 解析器的配置目标是：“像 Java 处理代码一样精确，同时兼容 MySQL 的语法习惯”
    private static SqlParser getCalciteParser(String sql) {
        return SqlParser.create(
                sql,
                SqlParser.Config.DEFAULT
                         // Flink CDC 的用户很多来自于 MySQL 生态。
                         // 选择此模式可以让解析器支持 MySQL 特有的语法习惯，例如使用反引号（`）来引用字段名，以及特定的函数调用规则。这增强了语法的灵活性。
                        .withConformance(SqlConformanceEnum.MYSQL_5)
                         // 设置为 true 意味着解析器在处理标识符（如表名、列名）时会区分大小写。
                        .withCaseSensitive(true)
                         // 设置 词法分析策略 (Lexical Policy)
                         // Lex.JAVA 规定了如何拆分字符串中的 Token（符号）
                        .withLex(Lex.JAVA));
    }
    // 用是将初步解析得到的 SQL 语法树（SqlNode）转换为关系表达式（RelNode）
    private static RelNode sqlToRel(
            List<Column> columns,
            SqlNode sqlNode,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        // 1. 将源表的物理列和 Flink CDC 支持的元数据列（如 __table_name__）合并
        List<Column> columnsWithMetadata =
                copyFillMetadataColumn(columns, supportedMetadataColumns);
        // // 2. 创建 Calcite 的根 Schema 环境
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus schema = rootSchema.plus();
        // 3. 配置虚拟表参数：表名（默认 TB）和合并后的列信息
        Map<String, Object> operand = new HashMap<>();
        operand.put("tableName", DEFAULT_TABLE);
        operand.put("columns", columnsWithMetadata);
        // 4. 利用工厂类在 rootSchema 中创建一个名为 "default_schema" 的空间，并注册这张表
        rootSchema.add(
                DEFAULT_SCHEMA,
                TransformSchemaFactory.INSTANCE.create(schema, DEFAULT_SCHEMA, operand));
        List<SqlFunction> udfFunctions = new ArrayList<>();
        // 动态注册用户自定义函数 (UDF)
        for (UserDefinedFunctionDescriptor udf : udfDescriptors) {
            try {
                // 1. 反射加载 UDF 类
                Class<?> clazz = Class.forName(udf.getClasspath());
                SqlReturnTypeInference returnTypeInference;
                // 2. 识别 UDF 中的 eval 方法（Flink UDF 标准入口）
                ScalarFunction function = ScalarFunctionImpl.create(clazz, "eval");
                Preconditions.checkNotNull(
                        function, "UDF function must provide at least one `eval` method.");
                // 3. 确定返回值类型
                if (udf.getReturnTypeHint() != null) {
                    // This UDF has return type hint annotation

                    // 如果用户明确指定了返回类型（Hint），则使用指定的类型
                    returnTypeInference =
                            o -> {
                                RelDataTypeFactory typeFactory = o.getTypeFactory();
                                DataType returnTypeHint = udf.getReturnTypeHint();
                                return convertCalciteType(typeFactory, returnTypeHint);
                            };
                } else {
                    // 否则通过反射 eval 方法的 Java 返回值类型来自动推断
                    // Infer it from eval method return type
                    returnTypeInference = o -> function.getReturnType(o.getTypeFactory());
                }
                // 4. 将函数添加到 Schema 中，供 SQL 验证器查找
                schema.add(udf.getName(), function);
                // 5. 封装成 Calcite 的 SqlFunction 对象（标记为多参数变参函数）
                udfFunctions.add(
                        new SqlFunction(
                                udf.getName(),
                                SqlKind.OTHER_FUNCTION,
                                returnTypeInference,
                                InferTypes.RETURN_TYPE,
                                OperandTypes.VARIADIC,
                                SqlFunctionCategory.USER_DEFINED_FUNCTION));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to resolve UDF: " + udf, e);
            }
        }
        // 1. 创建类型工厂（处理 SQL 到 Java 类型的映射）
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        // 2. 创建目录读取器，用于在验证期间查找表和函数
        CalciteCatalogReader calciteCatalogReader =
                new CalciteCatalogReader(
                        rootSchema,
                        rootSchema.path(DEFAULT_SCHEMA),
                        factory,
                        new CalciteConnectionConfigImpl(new Properties()));
        // 3. 加载内置算子表（如 +, -, UPPER 等）和刚才注册的 UDF 算子表
        TransformSqlOperatorTable transformSqlOperatorTable = TransformSqlOperatorTable.instance();
        SqlOperatorTable udfOperatorTable = SqlOperatorTables.of(udfFunctions);
        // 1. 创建验证器，设置 MySQL 5 兼容性
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        SqlOperatorTables.chain(transformSqlOperatorTable, udfOperatorTable),
                        calciteCatalogReader,
                        factory,
                        SqlValidator.Config.DEFAULT
                                // 允许标识符展开
                                .withIdentifierExpansion(true)
                                .withConformance(SqlConformanceEnum.MYSQL_5));
        // 2. 执行校验：检查列名对不对、函数参数对不对、类型能不能隐式转换
        SqlNode validateSqlNode = validator.validate(sqlNode);
        // 1. 创建转换器，配置优化器集群 (RelOptCluster)
        SqlToRelConverter sqlToRelConverter =
                new SqlToRelConverter(
                        null,
                        validator,
                        calciteCatalogReader,
                        RelOptCluster.create(
                                new HepPlanner(new HepProgramBuilder().build()),// 使用简单的启发式优化器
                                new RexBuilder(factory)),
                        StandardConvertletTable.INSTANCE,
                        SqlToRelConverter.config().withTrimUnusedFields(true));// 自动剪裁未使用的字段
        // 2. 将校验后的 SqlNode 转换为 RelRoot
        RelRoot relRoot = sqlToRelConverter.convertQuery(validateSqlNode, false, false);
        // 3. 返回最终的关系表达式树
        return relRoot.rel;
    }
    // 核心作用是将一个纯文本形式的 SQL 语句转化为 Calcite 的 抽象语法树（AST），并确保该语句是一个 SELECT 查询。
    public static SqlSelect parseSelect(String statement) {
        SqlNode sqlNode;
        try {
            sqlNode = getCalciteParser(statement).parseQuery();
        } catch (SqlParseException e) {
            LOG.error("Statements can not be parsed. {} \n {}", statement, e);
            throw new ParseException("Statements can not be parsed.", e);
        }
        if (sqlNode instanceof SqlSelect) {
            return (SqlSelect) sqlNode;
        } else {
            throw new ParseException("Only select statements can be parsed.");
        }
    }

    // Returns referenced columns (directly and indirectly) by projection and filter expression.
    // For example, given projection expression "a, c, upper(x) as d, y as e", filter expression "z
    // > 0", and columns array [a, b, c, x, y, z], returns referenced column array [a, c, x, y, z].
    // 找出在数据转换过程中，哪些源列是真正被“引用”到的。这对于优化性能（例如只读取必要的字段）至关重要。
    // 该方法通过解析“投影表达式”（SELECT 部分）和“过滤表达式”（WHERE 部分），提取出所有出现在这些表达式中的原始列名。
    public static List<Column> generateReferencedColumns(
            String projectionExpression, @Nullable String filterExpression, List<Column> columns) {
        // 如果没有任何投影定义，说明不需要处理任何列，直接返回空列表。
        if (isNullOrWhitespaceOnly(projectionExpression)) {
            return new ArrayList<>();
        }

        Set<String> referencedColumnNames = new HashSet<>();

        SqlSelect sqlProject = parseProjectionExpression(projectionExpression);
        if (!sqlProject.getSelectList().isEmpty()) {
            for (SqlNode sqlNode : sqlProject.getSelectList()) {
                // 1. 处理带别名的计算列 (如: upper(x) AS d)
                if (sqlNode instanceof SqlBasicCall) {
                    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                    // 只处理 AS 算子
                    if (SqlKind.AS.equals(sqlBasicCall.getOperator().kind)) {
                        // getOperandList().get(0) 是 AS 左边的表达式 (upper(x))
                        // 从表达式中提取出所有涉及的列名 (x)
                        referencedColumnNames.addAll(
                                parseColumnNameList(sqlBasicCall.getOperandList().get(0)));
                    } else {
                        throw new ParseException(
                                "Unrecognized projection expression: "
                                        + sqlBasicCall
                                        + ". Should be <EXPR> AS <IDENTIFIER>");
                    }
                // 2. 处理直接引用的列名 (如: a)
                } else if (sqlNode instanceof SqlIdentifier) {
                    SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                    // 如果是 SELECT *，说明引用了所有列，直接返回传入的 columns 集合
                    if (sqlIdentifier.isStar()) {
                        // wildcard star character matches all columns
                        return columns;
                    }
                    referencedColumnNames.add(
                            // 否则提取标识符的最后一部分作为列名
                            sqlIdentifier.names.get(sqlIdentifier.names.size() - 1));
                }
            }
        }

        if (!isNullOrWhitespaceOnly(projectionExpression)) {
            SqlSelect sqlFilter = parseFilterExpression(filterExpression);
            // 从 WHERE 子句中提取所有涉及的列名 (如: z > 0 中的 z)
            referencedColumnNames.addAll(parseColumnNameList(sqlFilter.getWhere()));
        }
        // 遍历原始的 Column 对象列表，只保留那些名字存在于 referencedColumnNames 集合中的列。
        return columns.stream()
                .filter(e -> referencedColumnNames.contains(e.getName()))
                .collect(Collectors.toList());
    }

    // Expands wildcard character * to full column list.
    // For example, given projection expression "a AS new_a, *, c as new_c"
    // and schema [a, b, c], expand it to [a as new_a, a, b, c, c as new_c].
    // This step is necessary since passing wildcard to sqlToRel will capture
    // unexpected metadata columns.
    // 核心作用是手动处理 SQL 中的通配符（星号 *）。
    // 确保 * 仅展开为用户定义的物理列，而不会包含系统内部为了校验而添加的隐藏元数据列。
    private static void expandWildcard(SqlSelect sqlSelect, List<Column> columns) {
        List<SqlNode> expandedNodes = new ArrayList<>();
        for (SqlNode sqlNode : sqlSelect.getSelectList().getList()) {
            // 检查当前的节点是否是一个标识符（SqlIdentifier），并且它是否代表星号（isStar()）
            if (sqlNode instanceof SqlIdentifier && ((SqlIdentifier) sqlNode).isStar()) {
                expandedNodes.addAll(
                        // 获取当前表定义的所有物理列（
                        // SqlParserPos.QUOTED_ZERO：这是一个特殊的标记，表示这个节点是生成的，而不是从原始 SQL 文本中直接解析出来的位置。
                        columns.stream()
                                .map(c -> new SqlIdentifier(c.getName(), SqlParserPos.QUOTED_ZERO))
                                .collect(Collectors.toList()));
            } else {
                expandedNodes.add(sqlNode);
            }
        }
        sqlSelect.setSelectList(new SqlNodeList(expandedNodes, SqlParserPos.ZERO));
    }

    // Returns projected columns based on given projection expression.
    // For example, given projection expression "a, b, c, upper(a) as d, b as e" and columns array
    // [a, b, c, x, y, z], returns projection column array [a, b, c, d, e].
    // 解析用户定义的投影表达式字符串，并将其转换为 Flink CDC 内部可执行的投影列模型（ProjectionColumn 列表）
    // 不仅完成了语法的解析，还负责了类型推断、星号展开以及将 SQL 表达式翻译为 Janino Java 表达式。
    public static List<ProjectionColumn> generateProjectionColumns(
            String projectionExpression,
            List<Column> columns,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        // 如果表达式为空，直接返回空列表。
        if (isNullOrWhitespaceOnly(projectionExpression)) {
            return new ArrayList<>();
        }
        // 调用 parseProjectionExpression 将字符串（如 id, age + 1 AS new_age）包装并解析为 Calcite 的 SqlSelect 语法树。
        SqlSelect sqlSelect = parseProjectionExpression(projectionExpression);
        if (sqlSelect.getSelectList().isEmpty()) {
            return new ArrayList<>();
        }
        // 如果用户写了 SELECT *，将其替换为源表中真实的物理列列表。
        expandWildcard(sqlSelect, columns);
        // 通过将 SQL 转换为关系表达式（RelNode），利用 Calcite 的推断引擎确定每个表达式的返回类型。
        RelNode relNode = sqlToRel(columns, sqlSelect, udfDescriptors, supportedMetadataColumns);
        // 每一列的数据类型
        RelDataType[] relDataTypes =
                relNode.getRowType().getFieldList().stream()
                        .map(RelDataTypeField::getType)
                        .toArray(RelDataType[]::new);
        // 原始的列映射
        Map<String, Column> originalColumnMap =
                columns.stream().collect(Collectors.toMap(Column::getName, column -> column));
        List<ProjectionColumn> projectionColumns = new ArrayList<>();
        Map<String, Integer> addedProjectionColumnNames = new HashMap<>();

        SqlNodeList selectExpressionList = sqlSelect.getSelectList();
        for (int i = 0; i < selectExpressionList.size(); i++) {
            SqlNode sqlNode = selectExpressionList.get(i);
            // 获取i列的数据类型
            RelDataType relDataType = relDataTypes[i];
            ProjectionColumn projectionColumn;

            // A projection column could be <EXPR> AS <IDENTIFIER>...
            // 处理带别名的表达式
            if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                List<SqlNode> operandList = sqlBasicCall.getOperandList();
                Preconditions.checkArgument(
                        SqlKind.AS.equals(sqlBasicCall.getOperator().kind)
                                && operandList.size() == 2
                                && operandList.get(1) instanceof SqlIdentifier,
                        "Unrecognized projection expression: "
                                + sqlBasicCall
                                + ". Should be <EXPR> AS <IDENTIFIER>");

                // It's the identifier node for aliased column.
                // 别名的节点
                SqlIdentifier aliasNode = (SqlIdentifier) operandList.get(1);
                String columnName = aliasNode.names.get(aliasNode.names.size() - 1);

                Preconditions.checkArgument(
                        !isMetadataColumn(columnName, supportedMetadataColumns),
                        "Column name %s is reserved and shading it is not allowed.",
                        columnName);

                // This is the actual expression node of this projection column.
                // as 左侧的表达式
                SqlNode exprNode = operandList.get(0);
                // 简单重命名（如 col_a AS col_b）：调用 resolveProjectionColumnFromIdentifier。
                // 这被视为“转发”或“别名”，尽量保留原列的元数据（如注释）
                if (exprNode instanceof SqlIdentifier) {
                    // This is a simple column rename like col_a AS col_b. Simply forward it to
                    // avoid losing metadata info like comments and default expressions.
                    SqlIdentifier identifierExprNode = (SqlIdentifier) exprNode;
                    String originalName =
                            identifierExprNode.names.get(identifierExprNode.names.size() - 1);
                    projectionColumn =
                            resolveProjectionColumnFromIdentifier(
                                    relDataType,
                                    originalColumnMap,
                                    originalName,
                                    columnName,
                                    supportedMetadataColumns);
                } else {
                    // 计算表达式（如 upper(name) AS n）：
                    List<String> originalColumnNames = parseColumnNameList(exprNode);
                    Map<String, String> columnNameMap = generateColumnNameMap(originalColumnNames);
                    projectionColumn =
                            ProjectionColumn.ofCalculated(
                                    columnName,
                                    DataTypeConverter.convertCalciteRelDataTypeToDataType(
                                            relDataType),
                                    exprNode.toString(),
                                    JaninoCompiler.translateSqlNodeToJaninoExpression(
                                            exprNode, udfDescriptors, columnNameMap),
                                    originalColumnNames,
                                    columnNameMap);
                }
            }
            // ... or an existing column's name identifier.
            //处理直接引用标识符 (如 id)
            else if (sqlNode instanceof SqlIdentifier) {
                SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                projectionColumn =
                        resolveProjectionColumnFromIdentifier(
                                relDataType,
                                originalColumnMap,
                                columnName,
                                columnName,
                                supportedMetadataColumns);
            } else {
                throw new ParseException("Unrecognized projection: " + sqlNode.toString());
            }
            // Projection columns comes later could override previous ones.
            // 处理冲突与去重（Override 逻辑）
            // 覆盖规则：如果在同一个投影定义中出现了同名的列，后面的定义会覆盖前面的。例如 SELECT a, b, a AS b，最终结果中 b 列将由 a 的值决定
            String projectionColumnName = projectionColumn.getColumnName();
            if (addedProjectionColumnNames.containsKey(projectionColumnName)) {
                // If we already have one column with identical name, replace it
                projectionColumns.set(
                        addedProjectionColumnNames.get(projectionColumnName), projectionColumn);
            } else {
                // Otherwise, append it at the end. Don't forget to set the index!
                projectionColumns.add(projectionColumn);
                addedProjectionColumnNames.put(projectionColumnName, projectionColumns.size() - 1);
            }
        }
        return projectionColumns;
    }

    /**
     * Create a projection column from a simple identifier node (could be an upstream physical
     * column or a metadata column).
     */
    // 主要任务是：根据一个简单的 SQL 标识符（Identifier），判断它到底是一个物理列、一个元数据列，还是一个重命名操作，并为其创建最合适的执行模型。
    public static ProjectionColumn resolveProjectionColumnFromIdentifier(
            RelDataType relDataType,
            Map<String, Column> originalColumnMap,
            String identifier,
            String projectedColumnName,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        Map<String, String> columnNameMap =
                Collections.singletonMap(identifier, MAPPED_SINGLE_COLUMN_NAME);
        // 元数据列分支 (Metadata Column)
        // 元数据列被视为一种特殊的“计算列”。
        if (isMetadataColumn(identifier, supportedMetadataColumns)) {
            // For a metadata column, we simply generate a projection column with the same
            return ProjectionColumn.ofCalculated(
                    projectedColumnName,
                    // Metadata columns should never be null
                    DataTypeConverter.convertCalciteRelDataTypeToDataType(relDataType).notNull(),
                    identifier,
                    columnNameMap.get(identifier),
                    Collections.singletonList(identifier),
                    columnNameMap);
        }

        Preconditions.checkArgument(
                originalColumnMap.containsKey(identifier),
                "Referenced column %s is not present in original table.",
                identifier);

        Column column = originalColumnMap.get(identifier);
        // 直接转发分支 (Forwarded Column)
        if (Objects.equals(identifier, projectedColumnName)) {
            return ProjectionColumn.ofForwarded(column, MAPPED_SINGLE_COLUMN_NAME);
        } else {
            // 别名转发分支 (Aliased Column)
            return ProjectionColumn.ofAliased(
                    column, projectedColumnName, MAPPED_SINGLE_COLUMN_NAME);
        }
    }
    // 任务是将用户编写的 SQL WHERE 子句（例如 age > 18 AND status = 'ACTIVE'）翻译成可以在 JVM 中直接运行的 Java 布尔表达式。
    // 是 Flink CDC 实现动态过滤的核心，让系统能够在不重新编译整个项目的情况下，根据用户配置实时过滤数据。
    public static String translateFilterExpressionToJaninoExpression(
            String filterExpression,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            Map<String, String> columnNameMap) {
        // 如果用户没有配置过滤条件，则返回空字符串。在执行框架中，空字符串通常意味着“不过滤，通过所有数据”。
        if (isNullOrWhitespaceOnly(filterExpression)) {
            return "";
        }
        SqlSelect sqlSelect = TransformParser.parseFilterExpression(filterExpression);
        // 检查解析出的语法树是否包含有效的 WHERE 节点。
        if (!sqlSelect.hasWhere()) {
            return "";
        }
        // where 变量现在持有了整个条件表达式的根节点（例如，如果条件是 a > 1，则根节点是一个代表 > 算子的 SqlCall）。
        SqlNode where = sqlSelect.getWhere();
        return JaninoCompiler.translateSqlNodeToJaninoExpression(
                where, udfDescriptors, columnNameMap);
    }
    // 主要职责是提取投影表达式（Projection Expression）最终输出的列名集合。
    // 确定“经过 Transform 转换后，目标表长什么样”。它关注的是结果列的名字，而不是其背后的逻辑或类型。
    public static List<String> parseComputedColumnNames(
            String projection, SupportedMetadataColumn[] supportedMetadataColumns) {
        List<String> columnNames = new ArrayList<>();
        if (isNullOrWhitespaceOnly(projection)) {
            return columnNames;
        }
        // 将投影字符串转换为 Calcite 的 SqlSelect 对象，以便遍历 SELECT 列表。
        SqlSelect sqlSelect = parseProjectionExpression(projection);
        if (sqlSelect.getSelectList().isEmpty()) {
            return columnNames;
        }

        for (SqlNode sqlNode : sqlSelect.getSelectList()) {
            // 处理 AS 别名（计算列/重命名列）
            if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                if (SqlKind.AS.equals(sqlBasicCall.getOperator().kind)) {
                    String columnName = null;
                    List<SqlNode> operandList = sqlBasicCall.getOperandList();
                    // 遍历 AS 算子的操作数（通常左边是表达式，右边是别名）
                    for (SqlNode operand : operandList) {
                        if (operand instanceof SqlIdentifier) {
                            //  提取标识符作为最终的列名
                            SqlIdentifier sqlIdentifier = (SqlIdentifier) operand;
                            columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                        }
                    }
                    if (columnNames.contains(columnName)) {
                        throw new ParseException("Duplicate column definitions: " + columnName);
                    }
                    columnNames.add(columnName);
                } else {
                    throw new ParseException("Unrecognized projection: " + sqlBasicCall);
                }
            } else if (sqlNode instanceof SqlIdentifier) {
                String columnName = sqlNode.toString();
                // 只有当标识符属于系统元数据列（如 __table_name__）时，才会被加入最终列名单
                if (isMetadataColumn(columnName, supportedMetadataColumns)
                        && !columnNames.contains(columnName)) {
                    columnNames.add(columnName);
                }
            } else {
                throw new ParseException("Unrecognized projection: " + sqlNode.toString());
            }
        }
        return columnNames;
    }

    public static List<String> parseFilterColumnNameList(String filterExpression) {
        if (isNullOrWhitespaceOnly(filterExpression)) {
            return new ArrayList<>();
        }
        SqlSelect sqlSelect = parseFilterExpression(filterExpression);
        if (!sqlSelect.hasWhere()) {
            return new ArrayList<>();
        }
        SqlNode where = sqlSelect.getWhere();
        return parseColumnNameList(where);
    }
    // 唯一目标是：从一个 SQL 表达式中“榨取”出所有被引用的原始列名。
    private static List<String> parseColumnNameList(SqlNode sqlNode) {
        List<String> columnNameList = new ArrayList<>();
        if (sqlNode instanceof SqlIdentifier) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
            // 获取标识符的最后一部分（处理类似 schema.table.column 的情况）
            // 这是递归的终点。当解析到最底层的字段名（如 id 或 age）时，直接提取名称并存入列表。
            String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
            columnNameList.add(columnName);
        } else if (sqlNode instanceof SqlCall) {
            SqlCall sqlCall = (SqlCall) sqlNode;
            // 递归查找操作数（Operands）中的标识符
            // SqlCall 代表一个算子或函数。它不直接包含列名，但它的**操作数（Operands）**里可能包含列名。程序会调用辅助方法 findSqlIdentifier 继续向下钻取。
            findSqlIdentifier(sqlCall.getOperandList(), columnNameList);
        } else if (sqlNode instanceof SqlNodeList) {
            // 处理一组表达式（例如 IN (col1, col2, col3) 里的列表）
            SqlNodeList sqlNodeList = (SqlNodeList) sqlNode;
            findSqlIdentifier(sqlNodeList.getList(), columnNameList);
        }
        return columnNameList;
    }
    // 通过深度优先搜索（DFS）遍历 Calcite 的语法树节点，精准地定位并提取出所有的列名标识符。
    private static void findSqlIdentifier(List<SqlNode> sqlNodes, List<String> columnNameList) {
        for (SqlNode sqlNode : sqlNodes) {
            if (sqlNode instanceof SqlIdentifier) {
                SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                // 获取标识符的最末尾名称（例如从 "my_table"."id" 中提取 "id"）
                String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                columnNameList.add(columnName);
            } else if (sqlNode instanceof SqlCall) {
                SqlCall sqlCall = (SqlCall) sqlNode;
                // / 获取算子的所有参数，继续向下搜索
                findSqlIdentifier(sqlCall.getOperandList(), columnNameList);
            } else if (sqlNode instanceof SqlNodeList) {
                SqlNodeList sqlNodeList = (SqlNodeList) sqlNode;
                // // 展开列表，对列表中的每个节点递归
                findSqlIdentifier(sqlNodeList.getList(), columnNameList);
            }
        }
    }
    // 将用户编写的投影（Projection）片段“包装”成一个完整的 SQL 查询语句，以便 Calcite 解析器能够处理。
    private static SqlSelect parseProjectionExpression(String projection) {
        StringBuilder statement = new StringBuilder();
        statement.append("SELECT "); // 添加 SQL 关键字 SELECT
        statement.append(projection);
        statement.append(" FROM "); // 添加 FROM 关键字
        statement.append(DEFAULT_TABLE);
        return parseSelect(statement.toString());
    }

    private static List<Column> copyFillMetadataColumn(
            List<Column> columns, SupportedMetadataColumn[] supportedMetadataColumns) {
        // Add metaColumn for SQLValidator.validate
        List<Column> columnsWithMetadata = new ArrayList<>(columns);
        METADATA_COLUMNS.stream()
                .map(col -> Column.physicalColumn(col.f0, col.f1))
                .forEach(columnsWithMetadata::add);
        Stream.of(supportedMetadataColumns)
                .map(sCol -> Column.physicalColumn(sCol.getName(), sCol.getType()))
                .forEach(columnsWithMetadata::add);
        return columnsWithMetadata;
    }
    // 判断是否为元数据
    private static boolean isMetadataColumn(
            String columnName, SupportedMetadataColumn[] supportedMetadataColumns) {
        return METADATA_COLUMNS.stream().anyMatch(col -> col.f0.equals(columnName))
                || Stream.of(supportedMetadataColumns)
                        .anyMatch(col -> col.getName().equals(columnName));
    }

    public static SqlSelect parseFilterExpression(String filterExpression) {
        StringBuilder statement = new StringBuilder();
        statement.append("SELECT * FROM ");
        statement.append(DEFAULT_TABLE);
        if (!isNullOrWhitespaceOnly(filterExpression)) {
            statement.append(" WHERE ");
            statement.append(filterExpression);
        }
        return parseSelect(statement.toString());
    }

    public static boolean hasAsterisk(@Nullable String projection) {
        if (isNullOrWhitespaceOnly(projection)) {
            // Providing an empty projection expression is equivalent to writing `*` explicitly.
            return true;
        }
        return parseProjectionExpression(projection).getOperandList().stream()
                .anyMatch(TransformParser::hasAsterisk);
    }

    private static boolean hasAsterisk(SqlNode sqlNode) {
        if (sqlNode instanceof SqlIdentifier) {
            return ((SqlIdentifier) sqlNode).isStar();
        } else if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            return sqlBasicCall.getOperandList().stream().anyMatch(TransformParser::hasAsterisk);
        } else if (sqlNode instanceof SqlNodeList) {
            SqlNodeList sqlNodeList = (SqlNodeList) sqlNode;
            return sqlNodeList.getList().stream().anyMatch(TransformParser::hasAsterisk);
        } else {
            return false;
        }
    }
    // 将不确定的原始列名映射为一组标准化的、符合 Java 变量命名规范的内部变量名。
    public static Map<String, String> generateColumnNameMap(List<String> originalColumnNames) {
        int i = 0;
        Map<String, String> columnNameMap = new HashMap<>();
        for (String columnName : originalColumnNames) {
            if (!columnNameMap.containsKey(columnName)) {
                columnNameMap.put(columnName, MAPPED_COLUMN_NAME_PREFIX + i);
                i++;
            }
        }
        return columnNameMap;
    }
}
