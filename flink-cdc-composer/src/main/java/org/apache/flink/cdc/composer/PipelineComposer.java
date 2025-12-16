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

package org.apache.flink.cdc.composer;

import org.apache.flink.cdc.composer.definition.PipelineDef;

/** Composer for translating a pipeline definition to an execution. */
// PipelineComposer 是 Flink CDC 管道编排层的核心接口。
// 它的主要作用是充当定义（Definition）和执行（Execution）之间的桥梁或翻译器。
// 将定义转化为执行： 它负责将一个抽象的、描述性的管道定义对象（PipelineDef）翻译、配置并转换成一个具体的可运行的 Flink 任务执行对象（PipelineExecution）
// 解耦： 这个接口将用户对 CDC 任务的意图（即 PipelineDef 中定义的 Source、Sink、转换逻辑、配置等）与 Flink 运行时执行这些意图的方式隔离开来
// 可扩展性： 通过使用接口，允许 Flink CDC 支持不同的编排和执行策略，例如，未来的实现可以支持不同的 Flink API（如 DataStream 或 Table API）来执行同一个管道定义。
public interface PipelineComposer {

    /**
     * Composing pipeline execution from the specified pipeline definition.
     *
     * @param pipelineDef definition of the pipeline
     */
    PipelineExecution compose(PipelineDef pipelineDef);
}
