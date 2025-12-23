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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Builder of {@link PreTransformOperator}. */
// PreTransformOperatorBuilder 是 Flink CDC 转换架构中的建造者（Builder）类。它遵循设计模式中的“建造者模式”，专门用于收集、组织并校验各种转换逻辑，最终实例化一个完整的 PreTransformOperator。
// 在 Flink CDC Pipeline 启动时，系统需要将用户在 YAML 配置文件中定义的各种 transform 规则转化为算子。PreTransformOperatorBuilder 的作用包括：
// 规则聚合：作为一个暂存区，收集所有的转换规则（TransformRule）和自定义函数（UDF）
// 解耦配置与实例化：它将复杂的算子初始化逻辑与外部配置解析解耦。用户通过链式调用（Fluent API）不断添加规则，最后通过一个 build() 方法产出算子。
// 配置算子状态行为：决定算子是否需要将 Schema 信息存储在 Flink 的状态（State）中，这关系到作业在 Checkpoint/Savepoint 后的恢复行为。
public class PreTransformOperatorBuilder {
    // 存储所有已添加的转换规则。
    private final List<TransformRule> transformRules = new ArrayList<>();
    // 如果为 true，PreTransformOperator 会在 Checkpoint 时将 Schema 元数据持久化。
    // 这在处理复杂的 Schema 演进（Schema Evolution）时非常重要，确保作业重启后能正确解析历史数据。
    private boolean shouldStoreSchemasInState;
    // 存储 UDF（用户自定义函数）的描述元组。
    private final List<Tuple3<String, String, Map<String, String>>> udfFunctions =
            new ArrayList<>();
    // 添加一个基础的转换规则。
    public PreTransformOperatorBuilder addTransform(
            String tableInclusions, @Nullable String projection, @Nullable String filter) {
        transformRules.add(
                new TransformRule(
                        tableInclusions,
                        projection,
                        filter,
                        "",
                        "",
                        "",
                        null,
                        new SupportedMetadataColumn[0]));
        return this;
    }
    // 添加一个完整的高级转换规则。
    public PreTransformOperatorBuilder addTransform(
            String tableInclusions,
            @Nullable String projection,
            @Nullable String filter,
            String primaryKey,
            String partitionKey,
            String tableOption,
            @Nullable String postTransformConverter,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        transformRules.add(
                new TransformRule(
                        tableInclusions,
                        projection,
                        filter,
                        primaryKey,
                        partitionKey,
                        tableOption,
                        postTransformConverter,
                        supportedMetadataColumns));
        return this;
    }
    // 批量注册用户自定义函数。
    public PreTransformOperatorBuilder addUdfFunctions(
            List<Tuple3<String, String, Map<String, String>>> udfFunctions) {
        this.udfFunctions.addAll(udfFunctions);
        return this;
    }

    public PreTransformOperatorBuilder shouldStoreSchemasInState(
            boolean shouldStoreSchemasInState) {
        this.shouldStoreSchemasInState = shouldStoreSchemasInState;
        return this;
    }

    public PreTransformOperator build() {
        return new PreTransformOperator(transformRules, udfFunctions, shouldStoreSchemasInState);
    }
}
