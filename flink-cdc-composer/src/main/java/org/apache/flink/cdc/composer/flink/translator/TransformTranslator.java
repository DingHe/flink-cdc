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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.composer.definition.ModelDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.definition.UdfDef;
import org.apache.flink.cdc.runtime.operators.transform.PostTransformOperator;
import org.apache.flink.cdc.runtime.operators.transform.PostTransformOperatorBuilder;
import org.apache.flink.cdc.runtime.operators.transform.PreTransformOperator;
import org.apache.flink.cdc.runtime.operators.transform.PreTransformOperatorBuilder;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Translator used to build {@link PreTransformOperator} and {@link PostTransformOperator} for event
 * transform.
 */
// TransformTranslator 是 Flink CDC Pipeline 框架中负责算子翻译的核心类。
// 它的主要任务是将用户在 YAML 或配置文件中定义的逻辑转换（如列投影、行过滤、UDF 调用等）翻译成 Flink DataStream API 中的物理算子。
// 在 Flink CDC 的流式处理链路中，数据通常需要经过“转换”层。TransformTranslator 的作用是将高层级的转换定义（TransformDef）转化为两个关键的运行期算子：
// PreTransformOperator (前置转换算子)：通常标记为 Transform:Schema。它主要负责处理元数据和 Schema 的变更，确保下游算子知道数据的结构。
// PostTransformOperator (后置转换算子)：通常标记为 Transform:Data。它负责执行具体的数据转换逻辑，比如计算表达式、过滤行、应用时区转换等。
public class TransformTranslator {

    /** Package of built-in model. */
    // 定义了内置计算模型的包路径前缀
    public static final String PREFIX_CLASSPATH_BUILT_IN_MODEL =
            "org.apache.flink.cdc.runtime.model.";
    // 构建并添加“前置转换”算子到 DataStream 拓扑中
    public DataStream<Event> translatePreTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            List<UdfDef> udfFunctions,
            List<ModelDef> models,
            SupportedMetadataColumn[] supportedMetadataColumns,
            boolean shouldStoreSchemasInState) {
        // 如果 transforms 列表为空，说明用户没有定义任何转换逻辑。此时直接返回原始的 input 流，不做任何处理
        if (transforms.isEmpty()) {
            return input;
        }
        return input.transform(
                "Transform:Schema", // 算子名称,明确告诉开发者，这个阶段正在进行基于 Schema 的数据转换。
                new EventTypeInfo(), // 输出类型信息
                generatePreTransform(  // 算子具体实现逻辑
                        transforms,
                        udfFunctions,
                        models, // 机器学习模型转换（如果配置了）
                        supportedMetadataColumns, //元数据列（如 _db_name）
                        shouldStoreSchemasInState)); // 是否将 Schema 存储在状态中（用于支持断点续传）。
    }
    // 使用构建器模式（Builder Pattern）具体实例化 PreTransformOperator。
    // 总的来说，PreTransformOperator只是提取实际引用到的列，构建新的数据记录，往下发
    private PreTransformOperator generatePreTransform(
            List<TransformDef> transforms,
            List<UdfDef> udfFunctions,
            List<ModelDef> models,
            SupportedMetadataColumn[] supportedMetadataColumns,
            boolean shouldStoreSchemasInState) {
        // 获取 PreTransformOperator 的建造者实例。
        PreTransformOperatorBuilder preTransformFunctionBuilder = PreTransformOperator.newBuilder();
        // 遍历并添加转换规则（Transform Definitions）
        for (TransformDef transform : transforms) {
            preTransformFunctionBuilder.addTransform(
                    transform.getSourceTable(), // 匹配的源表（支持通配符）
                    transform.getProjection(), // 投影列（SELECT 部分）
                    transform.getFilter(), // 过滤条件（WHERE 部分）
                    transform.getPrimaryKeys(),  // 转换后的主键定义
                    transform.getPartitionKeys(), // 转换后的主键定义
                    transform.getTableOptions(),  // 表级别参数配置
                    transform.getPostTransformConverter(),  // 后置转换逻辑（如类型转换器
                    supportedMetadataColumns); // 支持的元数据列（如 __db_name）
        }

        preTransformFunctionBuilder
                .addUdfFunctions(
                        udfFunctions.stream()
                                .map(this::udfDefToUDFTuple)
                                .collect(Collectors.toList()))
                .addUdfFunctions(
                        models.stream().map(this::modelToUDFTuple).collect(Collectors.toList()))
                .shouldStoreSchemasInState(shouldStoreSchemasInState);

        return preTransformFunctionBuilder.build();
    }
    // 构建并添加“后置转换”算子，负责具体的数据行处理。
    // Flink CDC 转换引擎中的第二个核心阶段。如果说 PreTransform 主要负责在“架构层”根据 Schema 进行初步过滤和投影，
    // 那么 PostTransform 则更侧重于在“数据执行层”完成最终的数值计算、时区转换以及复杂的 UDF 逻辑。
    public DataStream<Event> translatePostTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            String timezone,
            List<UdfDef> udfFunctions,
            List<ModelDef> models,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        // 如果用户没有定义任何投影（Projection）或过滤（Filter）逻辑，那么不需要添加任何额外的处理开销
        if (transforms.isEmpty()) {
            return input;
        }

        PostTransformOperatorBuilder postTransformFunctionBuilder =
                PostTransformOperator.newBuilder();
        // 循环添加转换规则
        for (TransformDef transform : transforms) {
            // 只有当规则中确实写了 projection（投影列）或 filter（过滤条件）时，才会处理。
            if (transform.isValidProjection() || transform.isValidFilter()) {
                postTransformFunctionBuilder.addTransform(
                        transform.getSourceTable(),
                        transform.getProjection(),
                        transform.getFilter(),
                        transform.getPrimaryKeys(),
                        transform.getPartitionKeys(),
                        transform.getTableOptions(),
                        transform.getPostTransformConverter(),
                        supportedMetadataColumns);
            }
        }
        postTransformFunctionBuilder.addTimezone(timezone); // 为算子设置全局时区，确保在执行时间相关的 SQL 函数（如 TO_TIMESTAMP）时，所有规则共享一致的时区上下文。
        // 将用户定义的 Java UDF 函数和 AI 模型函数加载到算子中
        postTransformFunctionBuilder.addUdfFunctions(
                udfFunctions.stream().map(this::udfDefToUDFTuple).collect(Collectors.toList()));
        postTransformFunctionBuilder.addUdfFunctions(
                models.stream().map(this::modelToUDFTuple).collect(Collectors.toList()));
        // 将逻辑构建器转化为真正的 Flink 物理算子，并接入数据流中。
        // input.transform(...)：这是 Flink DataStream API 的标准用法。它在 Flink 的拓扑图中插入一个名为 "Transform:Data" 的节点，指定输出类型为 EventTypeInfo
        return input.transform(
                "Transform:Data", new EventTypeInfo(), postTransformFunctionBuilder.build());
    }
    // 将 ModelDef（模型定义）转换为 Flink 内部使用的三元组格式。
    // Tuple3<模型名, 类路径, 参数Map>。它会自动为类名加上 PREFIX_CLASSPATH_BUILT_IN_MODEL 前缀。
    private Tuple3<String, String, Map<String, String>> modelToUDFTuple(ModelDef model) {
        return Tuple3.of(
                model.getModelName(),
                PREFIX_CLASSPATH_BUILT_IN_MODEL + model.getClassName(),
                model.getParameters());
    }
    // 将 UdfDef（UDF 定义）转换为内部使用的三元组格式。
    // Tuple3<UDF名, 类路径, 空Map>
    private Tuple3<String, String, Map<String, String>> udfDefToUDFTuple(UdfDef udf) {
        return Tuple3.of(udf.getName(), udf.getClasspath(), new HashMap<>());
    }
}
