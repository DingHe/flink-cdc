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
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.core.fs.Path;

/** Parsing pipeline definition files and generate {@link PipelineDef}. */
// 核心作用是标准化流水线定义文件的解析过程
// 由于 Flink CDC 流水线定义可以采用不同的格式（例如 YAML、JSON 或其他自定义格式），这个接口提供了一个统一的入口，将这些外部配置源读取进来，并将其内容转化为 Flink CDC 内部能够理解和处理的 PipelineDef 对象。
public interface PipelineDefinitionParser {

    /**
     * Parse the specified pipeline definition file path, merge global configurations, then generate
     * the {@link PipelineDef}.
     */
    // 从指定的文件路径解析流水线定义。
    PipelineDef parse(Path pipelineDefPath, Configuration globalPipelineConfig) throws Exception;

    /**
     * Parse the specified pipeline definition string, merge global configurations, then generate
     * the {@link PipelineDef}.
     */
    // 从指定的配置字符串（文本）解析流水线定义。
    // pipelineDefText (String)： 包含流水线定义的配置文本字符串（例如 YAML 格式的字符串）。
    PipelineDef parse(String pipelineDefText, Configuration globalPipelineConfig) throws Exception;
}
