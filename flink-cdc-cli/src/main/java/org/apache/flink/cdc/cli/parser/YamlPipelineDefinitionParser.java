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

package org.apache.flink.cdc.cli.parser;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.composer.definition.ModelDef;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.definition.UdfDef;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR;
import static org.apache.flink.cdc.common.utils.ChangeEventUtils.resolveSchemaEvolutionOptions;
import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/** Parser for converting YAML formatted pipeline definition to {@link PipelineDef}. */
// 核心作用是实现将 YAML 格式的文本（文件或字符串）反序列化和结构化为 Flink CDC 的抽象流水线定义对象 PipelineDef。
// 不仅负责基本的 YAML 解析，还处理了配置项的校验、必填字段的检查，以及对特定组件（如 Sink）的默认行为进行设置。
// 主要工作流程：
// I/O 操作： 读取指定路径的文件内容或接收 YAML 字符串。
// JSON 树转换： 使用 Jackson 库将 YAML 转化为可操作的 JsonNode 结构。
// 组件拆解和校验： 遍历 JsonNode，逐一解析 source、sink、route、transform 等顶级组件。
// 配置合并： 将全局配置与用户在 YAML 中提供的配置合并，生成最终的 PipelineDef。
public class YamlPipelineDefinitionParser implements PipelineDefinitionParser {

    // Parent node keys
    // 顶级键名常量。
    // 定义了 YAML 文件中主要的配置节点，例如 source、sink、route、transform、pipeline、model
    private static final String SOURCE_KEY = "source";
    private static final String SINK_KEY = "sink";
    private static final String ROUTE_KEY = "route";
    private static final String TRANSFORM_KEY = "transform";
    private static final String PIPELINE_KEY = "pipeline";
    private static final String MODEL_KEY = "model";

    // Source / sink keys
    // 组件内部键名常量。
    // 定义了 Source 和 Sink 内部的配置字段，例如 type、name、include.schema.changes 等。
    private static final String TYPE_KEY = "type";
    private static final String NAME_KEY = "name";
    private static final String INCLUDE_SCHEMA_EVOLUTION_TYPES = "include.schema.changes";
    private static final String EXCLUDE_SCHEMA_EVOLUTION_TYPES = "exclude.schema.changes";

    // Route keys
    // 路由键名常量。
    // 定义了 Route 规则中的字段，例如 source-table、sink-table。
    private static final String ROUTE_SOURCE_TABLE_KEY = "source-table";
    private static final String ROUTE_SINK_TABLE_KEY = "sink-table";
    private static final String ROUTE_REPLACE_SYMBOL = "replace-symbol";
    private static final String ROUTE_DESCRIPTION_KEY = "description";

    // Transform keys
    // 转换键名常量。
    // 定义了 Transform 规则中的字段，例如 projection、filter、primary-keys。
    private static final String TRANSFORM_SOURCE_TABLE_KEY = "source-table";
    private static final String TRANSFORM_PROJECTION_KEY = "projection";
    private static final String TRANSFORM_FILTER_KEY = "filter";
    private static final String TRANSFORM_DESCRIPTION_KEY = "description";
    private static final String TRANSFORM_CONVERTER_AFTER_TRANSFORM_KEY =
            "converter-after-transform";

    // UDF related keys
    // UDF 键名常量。 定义了自定义函数相关的字段，例如 user-defined-function、name、classpath。
    private static final String UDF_KEY = "user-defined-function";
    private static final String UDF_FUNCTION_NAME_KEY = "name";
    private static final String UDF_CLASSPATH_KEY = "classpath";

    // Model related keys
    // Model 键名常量。 定义了模型相关的字段，例如 model、model-name、class-name。
    private static final String MODEL_NAME_KEY = "model-name";

    private static final String MODEL_CLASS_NAME_KEY = "class-name";

    public static final String TRANSFORM_PRIMARY_KEY_KEY = "primary-keys";

    public static final String TRANSFORM_PARTITION_KEY_KEY = "partition-keys";

    public static final String TRANSFORM_TABLE_OPTION_KEY = "table-options";
    // Jackson YAML 对象映射器。
    // 使用 ObjectMapper(new YAMLFactory()) 初始化，专门用于将 YAML 格式的数据结构反序列化为 Java 对象或 JsonNode。
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    /** Parse the specified pipeline definition file. */
    @Override
    public PipelineDef parse(Path pipelineDefPath, Configuration globalPipelineConfig)
            throws Exception {
        FileSystem fileSystem = FileSystem.get(pipelineDefPath.toUri());
        FSDataInputStream pipelineInStream = fileSystem.open(pipelineDefPath);
        return parse(mapper.readTree(pipelineInStream), globalPipelineConfig);
    }

    @Override
    public PipelineDef parse(String pipelineDefText, Configuration globalPipelineConfig)
            throws Exception {
        return parse(mapper.readTree(pipelineDefText), globalPipelineConfig);
    }
    // 负责将 YAML 配置解析为最终的 PipelineDef 对象
    private PipelineDef parse(JsonNode pipelineDefJsonNode, Configuration globalPipelineConfig)
            throws Exception {

        // UDFs are optional. We parse UDF first and remove it from the pipelineDefJsonNode since
        // it's not of plain data types and must be removed before calling toPipelineConfig.
        // 用于存储解析后的用户自定义函数定义。
        List<UdfDef> udfDefs = new ArrayList<>();
        // 用于存储解析后的模型定义。
        final List<ModelDef> modelDefs = new ArrayList<>();
        // 检查根节点下是否存在名为 pipeline 的配置节点。
        // 通常，UDF 和 Model 是嵌套在 pipeline 节点下的。
        if (pipelineDefJsonNode.get(PIPELINE_KEY) != null) {
            // remove 方法返回被移除的节点（如果存在）。
            Optional.ofNullable(
                            ((ObjectNode) pipelineDefJsonNode.get(PIPELINE_KEY)).remove(UDF_KEY))
                    .ifPresent(node -> node.forEach(udf -> udfDefs.add(toUdfDef(udf))));

            Optional.ofNullable(
                            ((ObjectNode) pipelineDefJsonNode.get(PIPELINE_KEY)).remove(MODEL_KEY))
                    .ifPresent(node -> modelDefs.addAll(parseModels(node)));
        }

        // Pipeline configs are optional
        // 此时该节点中已经移除了 UDF 和 Model 列表，只剩下普通的键值对配置项。
        Configuration userPipelineConfig = toPipelineConfig(pipelineDefJsonNode.get(PIPELINE_KEY));

        // 从解析出的用户配置中读取 PIPELINE_SCHEMA_CHANGE_BEHAVIOR 选项的值，并存储到 schemaChangeBehavior 变量中
        SchemaChangeBehavior schemaChangeBehavior =
                userPipelineConfig.get(PIPELINE_SCHEMA_CHANGE_BEHAVIOR);

        // Source is required
        // 强制检查根节点下是否存在 SOURCE_KEY（即 source 节点）。如果不存在，则抛出异常，因为 Source 是必需的。
        SourceDef sourceDef =
                toSourceDef(
                        checkNotNull(
                                pipelineDefJsonNode.get(SOURCE_KEY),
                                "Missing required field \"%s\" in pipeline definition",
                                SOURCE_KEY));

        // Sink is required
        // 强制检查根节点下是否存在 SINK_KEY（即 sink 节点）。如果不存在，则抛出异常，因为 Sink 是必需的。
        SinkDef sinkDef =
                toSinkDef(
                        checkNotNull(
                                pipelineDefJsonNode.get(SINK_KEY),
                                "Missing required field \"%s\" in pipeline definition",
                                SINK_KEY),
                        schemaChangeBehavior);

        // Transforms are optional
        // 解析 Transform 定义（可选）：
        List<TransformDef> transformDefs = new ArrayList<>();
        Optional.ofNullable(pipelineDefJsonNode.get(TRANSFORM_KEY))
                .ifPresent(
                        node ->
                                node.forEach(
                                        transform -> transformDefs.add(toTransformDef(transform))));

        // Routes are optional
        // 解析 Route 定义（可选）
        List<RouteDef> routeDefs = new ArrayList<>();
        Optional.ofNullable(pipelineDefJsonNode.get(ROUTE_KEY))
                .ifPresent(node -> node.forEach(route -> routeDefs.add(toRouteDef(route))));

        // Merge user config into global config
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.addAll(globalPipelineConfig);
        pipelineConfig.addAll(userPipelineConfig);
        // 返回最终的 PipelineDef
        return new PipelineDef(
                sourceDef, sinkDef, routeDefs, transformDefs, udfDefs, modelDefs, pipelineConfig);
    }
    // 用于将 YAML 配置中的 Source 定义节点解析为 SourceDef 对象。
    private SourceDef toSourceDef(JsonNode sourceNode) {
        Map<String, String> sourceMap =
                mapper.convertValue(sourceNode, new TypeReference<Map<String, String>>() {});

        // "type" field is required
        String type =
                checkNotNull(
                        sourceMap.remove(TYPE_KEY),
                        "Missing required field \"%s\" in source configuration",
                        TYPE_KEY);

        // "name" field is optional
        String name = sourceMap.remove(NAME_KEY);

        return new SourceDef(type, name, Configuration.fromMap(sourceMap));
    }
    // 用于将 YAML 配置中的 Sink 定义节点解析为 SinkDef 对象。
    // 因为它不仅提取了 Sink 的连接器信息，还处理了复杂的 Schema 变更事件（Schema Evolution Event） 的包含和排除逻辑，特别是与流水线的 SchemaChangeBehavior 相关的默认行为。
    private SinkDef toSinkDef(JsonNode sinkNode, SchemaChangeBehavior schemaChangeBehavior) {
        // 用于存储用户明确要求 包含 的 Schema 变更事件类型（例如 ADD_COLUMN）。
        List<String> includedSETypes = new ArrayList<>();
        // 用于存储用户明确要求 排除 的 Schema 变更事件类型。
        List<String> excludedSETypes = new ArrayList<>();
        boolean excludedFieldNotPresent = sinkNode.get(EXCLUDE_SCHEMA_EVOLUTION_TYPES) == null;

        Optional.ofNullable(sinkNode.get(INCLUDE_SCHEMA_EVOLUTION_TYPES))
                .ifPresent(e -> e.forEach(tag -> includedSETypes.add(tag.asText())));

        Optional.ofNullable(sinkNode.get(EXCLUDE_SCHEMA_EVOLUTION_TYPES))
                .ifPresent(e -> e.forEach(tag -> excludedSETypes.add(tag.asText())));
        // 如果用户没有明确指定要包含哪些类型，则默认包含所有已知的 Schema 变更事件类型 (SchemaChangeEventTypeFamily.ALL)，
        // 将其标签添加到 includedSETypes 列表中。
        if (includedSETypes.isEmpty()) {
            // If no schema evolution types are specified, include all schema evolution types by
            // default.
            Arrays.stream(SchemaChangeEventTypeFamily.ALL)
                    .map(SchemaChangeEventType::getTag)
                    .forEach(includedSETypes::add);
        }
        // 在宽松模式下，为了防止潜在的灾难性操作（如删除整个表），如
        // 果没有用户明确配置排除列表，系统默认会排除 DROP_TABLE（删除表）和 TRUNCATE_TABLE（清空表）这两种事件。
        if (excludedFieldNotPresent && SchemaChangeBehavior.LENIENT.equals(schemaChangeBehavior)) {
            // In lenient mode, we exclude DROP_TABLE and TRUNCATE_TABLE by default. This could be
            // overridden by manually specifying excluded types.
            Stream.of(SchemaChangeEventType.DROP_TABLE, SchemaChangeEventType.TRUNCATE_TABLE)
                    .map(SchemaChangeEventType::getTag)
                    .forEach(excludedSETypes::add);
        }
        // 根据收集到的包含列表和排除列表，计算出最终应该由 Sink 处理的 Schema 变更事件集合
        Set<SchemaChangeEventType> declaredSETypes =
                resolveSchemaEvolutionOptions(includedSETypes, excludedSETypes);
        // 这些键值对是 YamlPipelineDefinitionParser 内部使用的配置，不应作为连接器参数传递给底层的 Sink 连接器。
        if (sinkNode instanceof ObjectNode) {
            ((ObjectNode) sinkNode).remove(INCLUDE_SCHEMA_EVOLUTION_TYPES);
            ((ObjectNode) sinkNode).remove(EXCLUDE_SCHEMA_EVOLUTION_TYPES);
        }

        Map<String, String> sinkMap =
                mapper.convertValue(sinkNode, new TypeReference<Map<String, String>>() {});

        // "type" field is required
        String type =
                checkNotNull(
                        sinkMap.remove(TYPE_KEY),
                        "Missing required field \"%s\" in sink configuration",
                        TYPE_KEY);

        // "name" field is optional
        String name = sinkMap.remove(NAME_KEY);

        return new SinkDef(type, name, Configuration.fromMap(sinkMap), declaredSETypes);
    }

    private RouteDef toRouteDef(JsonNode routeNode) {
        String sourceTable =
                checkNotNull(
                                routeNode.get(ROUTE_SOURCE_TABLE_KEY),
                                "Missing required field \"%s\" in route configuration",
                                ROUTE_SOURCE_TABLE_KEY)
                        .asText();
        String sinkTable =
                checkNotNull(
                                routeNode.get(ROUTE_SINK_TABLE_KEY),
                                "Missing required field \"%s\" in route configuration",
                                ROUTE_SINK_TABLE_KEY)
                        .asText();
        String replaceSymbol =
                Optional.ofNullable(routeNode.get(ROUTE_REPLACE_SYMBOL))
                        .map(JsonNode::asText)
                        .orElse(null);
        String description =
                Optional.ofNullable(routeNode.get(ROUTE_DESCRIPTION_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        return new RouteDef(sourceTable, sinkTable, replaceSymbol, description);
    }
    // 用于将 YAML 配置中的单个用户自定义函数 (UDF) 节点解析为 UdfDef 对象。
    // 接收一个 JsonNode 对象 udfNode，该节点代表 YAML 中配置的一个 UDF 结构（通常是 name 和 classpath 字段），返回一个封装了这些信息的 UdfDef 对象。
    private UdfDef toUdfDef(JsonNode udfNode) {
        // 如果子节点存在且非空，则将其内容提取为 String 类型，赋值给 functionName 变量。
        String functionName =
                checkNotNull(
                                udfNode.get(UDF_FUNCTION_NAME_KEY),
                                "Missing required field \"%s\" in UDF configuration",
                                UDF_FUNCTION_NAME_KEY)
                        .asText();

        // 尝试从当前的 UDF 配置节点中获取名称为 UDF_CLASSPATH_KEY（即 classpath）的子节点。
        String classpath =
                checkNotNull(
                                udfNode.get(UDF_CLASSPATH_KEY),
                                "Missing required field \"%s\" in UDF configuration",
                                UDF_CLASSPATH_KEY)
                        .asText();

        return new UdfDef(functionName, classpath);
    }

    private TransformDef toTransformDef(JsonNode transformNode) {
        String sourceTable =
                checkNotNull(
                                transformNode.get(TRANSFORM_SOURCE_TABLE_KEY),
                                "Missing required field \"%s\" in transform configuration",
                                TRANSFORM_SOURCE_TABLE_KEY)
                        .asText();
        String projection =
                Optional.ofNullable(transformNode.get(TRANSFORM_PROJECTION_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        // When the star is in the first place, a backslash needs to be added for escape.
        if (!StringUtils.isNullOrWhitespaceOnly(projection) && projection.contains("\\*")) {
            projection = projection.replace("\\*", "*");
        }
        String filter =
                Optional.ofNullable(transformNode.get(TRANSFORM_FILTER_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        String primaryKeys =
                Optional.ofNullable(transformNode.get(TRANSFORM_PRIMARY_KEY_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        String partitionKeys =
                Optional.ofNullable(transformNode.get(TRANSFORM_PARTITION_KEY_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        String tableOptions =
                Optional.ofNullable(transformNode.get(TRANSFORM_TABLE_OPTION_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        String description =
                Optional.ofNullable(transformNode.get(TRANSFORM_DESCRIPTION_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);
        String postTransformConverter =
                Optional.ofNullable(transformNode.get(TRANSFORM_CONVERTER_AFTER_TRANSFORM_KEY))
                        .map(JsonNode::asText)
                        .orElse(null);

        return new TransformDef(
                sourceTable,
                projection,
                filter,
                primaryKeys,
                partitionKeys,
                tableOptions,
                description,
                postTransformConverter);
    }
    // 用于将 YAML 配置中 pipeline 节点下的所有配置项转化为 Flink 内部的 Configuration 对象。
    // 接收一个 JsonNode 对象 pipelineConfigNode，该节点通常代表 YAML 中 <root>.pipeline 节点下的配置键值对。它返回一个封装了这些键值对的 Flink Configuration 对象。
    private Configuration toPipelineConfig(JsonNode pipelineConfigNode) {
        if (pipelineConfigNode == null || pipelineConfigNode.isNull()) {
            return new Configuration();
        }
        Map<String, String> pipelineConfigMap =
                mapper.convertValue(
                        pipelineConfigNode, new TypeReference<Map<String, String>>() {});
        return Configuration.fromMap(pipelineConfigMap);
    }
    // 用于解析 YAML 配置中的 Model 定义（Model Definition）节点，并将其转换为 ModelDef 对象列表。
    private List<ModelDef> parseModels(JsonNode modelsNode) {
        List<ModelDef> modelDefs = new ArrayList<>();
        Preconditions.checkNotNull(modelsNode, "`model` in `pipeline` should not be empty.");
        if (modelsNode.isArray()) {
            for (JsonNode modelNode : modelsNode) {
                modelDefs.add(convertJsonNodeToModelDef(modelNode));
            }
        } else {
            modelDefs.add(convertJsonNodeToModelDef(modelsNode));
        }
        return modelDefs;
    }
    // 用于将 YAML 配置中的单个 Model 定义节点解析为 ModelDef 对象。
    private ModelDef convertJsonNodeToModelDef(JsonNode modelNode) {
        // 尝试从当前 Model 节点中获取名称为 MODEL_NAME_KEY（即 model-name）的子节点。
        String name =
                checkNotNull(
                                modelNode.get(MODEL_NAME_KEY),
                                "Missing required field \"%s\" in `model`",
                                MODEL_NAME_KEY)
                        .asText();
        // 尝试从当前 Model 节点中获取名称为 MODEL_CLASS_NAME_KEY（即 class-name）的子节点。
        String model =
                checkNotNull(
                                modelNode.get(MODEL_CLASS_NAME_KEY),
                                "Missing required field \"%s\" in `model`",
                                MODEL_CLASS_NAME_KEY)
                        .asText();
        // 使用 Jackson ObjectMapper 将整个 modelNode（包括 model-name、class-name 以及所有其他配置项）转换为一个通用的 Map<String, String> 对象。
        Map<String, String> properties = mapper.convertValue(modelNode, Map.class);
        return new ModelDef(name, model, properties);
    }
}
