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

import java.util.Map;
import java.util.Objects;

/**
 * Common properties of model.
 *
 * <p>A transformation definition contains:
 *
 * <ul>
 *   <li>modelName: The name of function.
 *   <li>className: The model to transform data.
 *   <li>parameters: The parameters that used to configure the model.
 * </ul>
 */
// ModelDef 类的核心作用是以结构化的方式表示 Flink CDC 流水线中需要注册、加载和配置的、用于数据处理或转换的自定义模型
// 虽然在 Flink CDC 的核心 CDC（Change Data Capture）场景中，其具体用途可能不如 Source/Sink 明显，但它为流水线设计提供了一个可扩展的机制，用于集成复杂的数据转换逻辑，例如机器学习模型、数据清洗逻辑等，这些逻辑通常以一个类的方式提供。
// 概括来说，它定义了“用于数据转换的模型的名称、位置和参数”：
// 模型名称 (ModelName): 在流水线中引用该模型的逻辑名称
// 类名 (ClassName): 包含模型实现逻辑的类的完全限定名。
// 参数 (Parameters): 用于配置该模型的运行时参数。
public class ModelDef {
    // 模型的逻辑名称。
    // 这是在 Flink CDC 流水线配置中引用该模型时使用的名称，类似于一个函数的别名。
    private final String modelName;
    // 模型的实现类名。
    // 必需，指定了包含该模型逻辑的 Java 类的完全限定名（例如 com.example.MyDataProcessorModel）。这是 Flink 运行时加载和实例化该模型的依据。
    private final String className;
    // 模型的配置参数。
    // 这是一个键值对的映射，包含了在模型初始化或运行时需要的特定配置。例如，可能是模型路径、版本号或其他运行时超参数。
    private final Map<String, String> parameters;

    public ModelDef(String modelName, String className, Map<String, String> parameters) {
        this.modelName = modelName;
        this.className = className;
        this.parameters = parameters;
    }

    public String getModelName() {
        return modelName;
    }

    public String getClassName() {
        return className;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ModelDef modelDef = (ModelDef) o;
        return Objects.equals(modelName, modelDef.modelName)
                && Objects.equals(className, modelDef.className)
                && Objects.equals(parameters, modelDef.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelName, className, parameters);
    }

    @Override
    public String toString() {
        return "ModelDef{"
                + "name='"
                + modelName
                + '\''
                + ", model='"
                + className
                + '\''
                + ", parameters="
                + parameters
                + '}';
    }
}
