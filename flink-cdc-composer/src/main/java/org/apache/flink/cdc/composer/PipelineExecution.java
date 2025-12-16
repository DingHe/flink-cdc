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

/** A pipeline execution that can be executed by a computing engine. */
// 表示可执行流水线任务的接口 PipelineExecution
// 是 Flink CDC 任务定义（PipelineDef）到实际计算引擎（如 Flink Runtime）执行步骤的中间抽象。
// 核心作用是定义一个可以被计算引擎执行的 Flink CDC 流水线作业的抽象契约。
// 在 Flink CDC 的任务提交流程中，PipelineComposer (流水线构建器) 会读取用户的抽象定义 PipelineDef，并将其翻译成一个实现了 PipelineExecution 接口的实例。
// 这个实例随后就可以通过调用 execute() 方法，提交到实际的计算环境（如 Flink 集群）中运行。
public interface PipelineExecution {

    /** Execute the pipeline. */
    // 作用： 启动流水线的执行过程。
    ExecutionInfo execute() throws Exception;

    /** Information of the execution. */
    class ExecutionInfo {
        private final String id;
        private final String description;

        public ExecutionInfo(String id, String description) {
            this.id = id;
            this.description = description;
        }

        public String getId() {
            return id;
        }

        public String getDescription() {
            return description;
        }
    }
}
