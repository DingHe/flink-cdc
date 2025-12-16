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

package org.apache.flink.cdc.cli;

import org.apache.flink.cdc.cli.parser.PipelineDefinitionParser;
import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineDeploymentExecutor;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment;
import org.apache.flink.cdc.composer.flink.deployment.K8SApplicationDeploymentExecutor;
import org.apache.flink.cdc.composer.flink.deployment.YarnApplicationDeploymentExecutor;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.cli.CommandLine;

import java.util.List;

import static org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment.REMOTE;

/** Executor for doing the composing and submitting logic for {@link CliFrontend}. */
// 封装了从解析配置到最终提交 Flink 作业的整个流程。
// 统一配置管理： 接收并存储由 CliFrontend 解析和收集到的所有关键配置信息（流水线定义路径、Flink 配置、全局 CDC 配置、外部 JARs 等）。
// 确定部署目标： 根据 Flink 配置中的部署目标（如 local, remote, yarn-application, kubernetes-application），选择正确的执行策略。
// 解析和组合： 负责读取流水线定义文件 (pipeline.yaml)，并使用 PipelineComposer 将其翻译成 Flink 可执行的作业图或部署包。
// 执行和部署： 协调部署执行器（DeploymentExecutor）或 Flink 管道组合器（PipelineComposer）将作业提交到目标集群或在本地运行。

public class CliExecutor {
    // 存储 CDC 流水线定义文件（如 pipeline.yaml）在文件系统中的路径。
    private final Path pipelineDefPath;
    // 存储经过命令行参数覆盖和 Savepoint 设置后的最终 Flink 运行时配置。
    private final org.apache.flink.configuration.Configuration flinkConfig;
    // 存储从 flink-cdc.yaml 或命令行加载的 Flink CDC 全局配置。
    private final Configuration globalPipelineConfig;
    // 存储通过命令行指定的、需要在作业运行时添加到 Classpath 的额外 JAR 包的路径列表。
    private final List<Path> additionalJars;
    // 存储 Flink 安装目录的路径，主要用于 Application 模式部署时定位 Flink 依赖。
    private final Path flinkHome;
    // 存储原始的命令行解析结果，以便在部署执行器中需要访问原始命令行参数时使用。
    private final CommandLine commandLine;
    private PipelineComposer composer = null;

    public CliExecutor(
            CommandLine commandLine,
            Path pipelineDefPath,
            org.apache.flink.configuration.Configuration flinkConfig,
            Configuration globalPipelineConfig,
            List<Path> additionalJars,
            Path flinkHome) {
        this.commandLine = commandLine;
        this.pipelineDefPath = pipelineDefPath;
        this.flinkConfig = flinkConfig;
        this.globalPipelineConfig = globalPipelineConfig;
        this.additionalJars = additionalJars;
        this.flinkHome = flinkHome;
    }

    public PipelineExecution.ExecutionInfo run() throws Exception {
        // Create Submit Executor to deployment flink cdc job Or Run Flink CDC Job
        String deploymentTargetStr = getDeploymentTarget();
        ComposeDeployment deploymentTarget =
                ComposeDeployment.getDeploymentFromName(deploymentTargetStr);
        switch (deploymentTarget) {
            case KUBERNETES_APPLICATION:
                return deployWithApplicationComposer(new K8SApplicationDeploymentExecutor());
            case YARN_APPLICATION:
                return deployWithApplicationComposer(new YarnApplicationDeploymentExecutor());
            case LOCAL:
                return deployWithComposer(FlinkPipelineComposer.ofMiniCluster());
            case REMOTE:
            case YARN_SESSION:
                return deployWithComposer(
                        FlinkPipelineComposer.ofRemoteCluster(flinkConfig, additionalJars));
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Deployment target %s is not supported", deploymentTargetStr));
        }
    }

    private PipelineExecution.ExecutionInfo deployWithApplicationComposer(
            PipelineDeploymentExecutor composeExecutor) throws Exception {
        return composeExecutor.deploy(commandLine, flinkConfig, additionalJars, flinkHome);
    }
    // 负责在 Session 模式（Remote） 或 Local 模式（MiniCluster） 下，执行 Flink CDC 流水线的加载、组合和最终提交。
    // composer (PipelineComposer): 接收一个 PipelineComposer 实例，它负责将抽象的流水线定义转化为 Flink 特定的执行逻辑。
    private PipelineExecution.ExecutionInfo deployWithComposer(PipelineComposer composer)
            throws Exception {
        // 流水线定义文件解析
        // 明确指定使用 YAML 格式来解析用户提供的流水线定义文件。
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(pipelineDefPath, globalPipelineConfig);
        // 使用传入的 PipelineComposer（通常是 FlinkPipelineComposer 的 Session/Local 模式实例）
        // 将抽象的 PipelineDef 转化为一个可执行的 PipelineExecution 对象。
        PipelineExecution execution = composer.compose(pipelineDef);
        return execution.execute();
    }

    @VisibleForTesting
    public PipelineExecution.ExecutionInfo deployWithNoOpComposer() throws Exception {
        return deployWithComposer(this.composer);
    }

    // The main class for running application mode
    public static void main(String[] args) throws Exception {
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = pipelineDefinitionParser.parse(args[0], new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPipelineComposer flinkPipelineComposer =
                FlinkPipelineComposer.ofApplicationCluster(env);
        PipelineExecution execution = flinkPipelineComposer.compose(pipelineDef);
        execution.execute();
    }
    // 获取composer
    @VisibleForTesting
    void setComposer(PipelineComposer composer) {
        this.composer = composer;
    }
    // 获取最终flink运行的参数
    @VisibleForTesting
    public org.apache.flink.configuration.Configuration getFlinkConfig() {
        return flinkConfig;
    }
    // 获取全局配置
    @VisibleForTesting
    public Configuration getGlobalPipelineConfig() {
        return globalPipelineConfig;
    }
    // 获取其他依赖包
    @VisibleForTesting
    public List<Path> getAdditionalJars() {
        return additionalJars;
    }
    // 获取执行方式
    public String getDeploymentTarget() {
        return flinkConfig.get(DeploymentOptions.TARGET);
    }
}
