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

package org.apache.flink.cdc.composer.flink;

import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A pipeline execution that run the defined pipeline via Flink's {@link
 * StreamExecutionEnvironment}.
 */
// 专门负责将抽象的流水线任务转化为实际运行在 Apache Flink 上的作业。
// 核心作用是封装一个准备好提交给 Flink 计算引擎执行的 CDC 作业。
// 主要职责：
// 持有 Flink 环境： 存储已经配置好 Source、Sink 和转换逻辑的 StreamExecutionEnvironment 对象。
// 执行任务： 实现 execute() 方法，负责将封装好的 Flink 作业异步或同步地提交到 Flink 集群。
public class FlinkPipelineExecution implements PipelineExecution {
    // Flink 流执行环境。
    // 这是一个核心属性，它代表了 Flink CDC 作业的完整运行时环境和数据流图。
    // 所有 Source、Sink、Transform 逻辑都已经被配置并添加到这个环境中，等待调用 execute() 提交。
    private final StreamExecutionEnvironment env;
    private final String jobName;
    private final boolean isBlocking;

    public FlinkPipelineExecution(
            StreamExecutionEnvironment env, String jobName, boolean isBlocking) {
        this.env = env;
        this.jobName = jobName;
        this.isBlocking = isBlocking;
    }
    // 执行 Flink 流水线。
    // 实现了 PipelineExecution 接口的核心方法，负责将任务提交到 Flink 集群。
    @Override
    public ExecutionInfo execute() throws Exception {
        JobClient jobClient = env.executeAsync(jobName);
        if (isBlocking) {
            jobClient.getJobExecutionResult().get();
        }
        return new ExecutionInfo(jobClient.getJobID().toString(), jobName);
    }
}
