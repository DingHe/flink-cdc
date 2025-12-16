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

import org.apache.flink.cdc.cli.utils.ConfigurationUtils;
import org.apache.flink.cdc.cli.utils.FlinkEnvironmentUtils;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.flink.shaded.guava31.com.google.common.base.Joiner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.cli.CliFrontendOptions.FLINK_CONFIG;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_ALLOW_NON_RESTORED_OPTION;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_CLAIM_MODE;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_PATH_OPTION;
import static org.apache.flink.cdc.cli.CliFrontendOptions.TARGET;
import static org.apache.flink.cdc.cli.CliFrontendOptions.USE_MINI_CLUSTER;
import static org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment.LOCAL;
import static org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment.REMOTE;

/** The frontend entrypoint for the command-line interface of Flink CDC. */
// 解析命令行参数： 处理用户在终端输入的参数，如流水线定义文件路径、Flink 配置、Savepoint 设置、外部 JAR 包路径等。
// 加载环境配置： 确定 Flink 的安装目录 (FLINK_HOME) 和 Flink CDC 的配置目录，并加载相应的配置文件。
// 构建执行器： 基于解析的参数和配置，创建 CliExecutor 实例。
// 启动流水线： 调用 CliExecutor 运行 CDC 流水线（Pipeline），并将其部署到 Flink 集群上（可以是本地 MiniCluster 或远程集群）
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);

    // 存储 Flink 安装目录环境变量的名称字符串，即 "FLINK_HOME"。
    private static final String FLINK_HOME_ENV_VAR = "FLINK_HOME";
    // 存储 Flink CDC 配置目录环境变量的名称字符串，即 "FLINK_CDC_HOME"。
    private static final String FLINK_CDC_HOME_ENV_VAR = "FLINK_CDC_HOME";

    public static void main(String[] args) throws Exception {
        // 初始化命令行选项
        Options cliOptions = CliFrontendOptions.initializeOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(cliOptions, args);

        // Help message
        // 检查是否需要打印帮助信息
        if (args.length == 0 || commandLine.hasOption(CliFrontendOptions.HELP)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setLeftPadding(4);
            formatter.setWidth(80);
            formatter.printHelp(" ", cliOptions);
            return;
        }

        // Create executor and execute the pipeline
        // 创建执行器
        // 调用 run() 执行流水线
        PipelineExecution.ExecutionInfo result = createExecutor(commandLine).run();

        // Print execution result
        // 打印结果
        printExecutionInfo(result);
    }

    // CLI 任务启动逻辑的核心，负责将命令行参数、各种配置文件和环境信息整合起来，
    // 最终构建一个可执行的 CliExecutor 实例。
    @VisibleForTesting
    static CliExecutor createExecutor(CommandLine commandLine) throws Exception {
        // The pipeline definition file would remain unparsed
        List<String> unparsedArgs = commandLine.getArgList();
        // 如果列表为空，则意味着用户没有提供必须的流水线定义文件路径，因此抛出异常。
        if (unparsedArgs.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing pipeline definition file path in arguments. ");
        }
        // 将第一个未解析的参数作为路径字符串，并将其封装成 Flink 的 Path 对象。
        Path pipelineDefPath = new Path(unparsedArgs.get(0));
        // Take the first unparsed argument as the pipeline definition file
        LOG.info("Real Path pipelineDefPath {}", pipelineDefPath);
        // Global pipeline configuration
        // 加载 Flink CDC 的全局配置文件（参见对该方法的解读），这些配置通常应用于整个 CDC 管道。
        Configuration globalPipelineConfig = getGlobalConfig(commandLine);

        // Load Flink environment
        Path flinkHome = getFlinkHome(commandLine);
        Configuration configuration = FlinkEnvironmentUtils.loadFlinkConfiguration(flinkHome);

        // To override the Flink configuration
        // 覆盖 Flink 配置
        overrideFlinkConfiguration(configuration, commandLine);

        // 将 Flink CDC 使用的 Configuration 对象（org.apache.flink.cdc.common.configuration.Configuration）转换为 Flink 运行时所需的配置对象
        org.apache.flink.configuration.Configuration flinkConfig =
                org.apache.flink.configuration.Configuration.fromMap(configuration.toMap());

        // Savepoint
        SavepointRestoreSettings savepointSettings = createSavepointRestoreSettings(commandLine);
        SavepointRestoreSettings.toConfiguration(savepointSettings, flinkConfig);

        // Additional JARs
        List<Path> additionalJars =
                Arrays.stream(
                                Optional.ofNullable(
                                                commandLine.getOptionValues(CliFrontendOptions.JAR))
                                        .orElse(new String[0]))
                        .map(Path::new)
                        .collect(Collectors.toList());

        // Build executor
        return new CliExecutor(
                commandLine,
                pipelineDefPath,
                flinkConfig,
                globalPipelineConfig,
                additionalJars,
                flinkHome);
    }
    // 作用是根据命令行参数，动态地覆盖或修改已经加载的 Flink 运行时配置（flinkConfig）。
    // 不返回任何值，而是直接修改传入的 flinkConfig 对象。
    private static void overrideFlinkConfiguration(
            Configuration flinkConfig, CommandLine commandLine) {
        // 如果命令行包含 USE_MINI_CLUSTER 选项（例如 --use-mini-cluster），
        // 则将目标设为 LOCAL.getName()（即在本地启动 MiniCluster）
        // 否则，从命令行获取 TARGET 选项的值（例如 --target remote 或 --target yarn-per-job），
        // 如果未提供，则默认为 REMOTE.getName()（即提交到外部集群）。
        String target =
                commandLine.hasOption(USE_MINI_CLUSTER)
                        ? LOCAL.getName()
                        : commandLine.getOptionValue(TARGET, REMOTE.getName());
        flinkConfig.set(
                ConfigOptions.key(DeploymentOptions.TARGET.key()).stringType().defaultValue(target),
                target);

        // 处理动态配置参数（-D）
        Properties properties = commandLine.getOptionProperties(FLINK_CONFIG.getOpt());
        LOG.info("Dynamic flink config items found: {}", properties);
        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            if (StringUtils.isNullOrWhitespaceOnly(key)
                    || StringUtils.isNullOrWhitespaceOnly(value)) {
                throw new IllegalArgumentException(
                        String.format(
                                "null or white space argument for key or value: %s=%s",
                                key, value));
            }
            ConfigOption<String> configOption =
                    ConfigOptions.key(key.trim()).stringType().defaultValue(value.trim());
            flinkConfig.set(configOption, value.trim());
        }
    }

    // 专门用于根据命令行输入构建 Flink 的 Savepoint 恢复配置 (SavepointRestoreSettings)。
    // 接受一个 CommandLine 对象（已解析的命令行参数），并返回一个 Flink 专用的 SavepointRestoreSettings 对象。
    private static SavepointRestoreSettings createSavepointRestoreSettings(
            CommandLine commandLine) {
        // 检查命令行参数中是否包含指定 Savepoint 路径的选项（例如 --savepointPath）。
        if (commandLine.hasOption(SAVEPOINT_PATH_OPTION.getOpt())) {
            String savepointPath = commandLine.getOptionValue(SAVEPOINT_PATH_OPTION.getOpt());
            // 检查命令行中是否设置了“允许非恢复状态”的选项（例如 --allowNonRestoredState）。
            // 这个标志允许作业从 Savepoint 恢复时跳过那些在 Savepoint 中有状态但在新作业图中不存在的操作符状态。
            boolean allowNonRestoredState =
                    commandLine.hasOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION.getOpt());
            final Object restoreMode;
            // 检查命令行是否指定了 Savepoint 的恢复模式（Claim Mode），例如 NO_CLAIM 或 CLAIM。
            if (commandLine.hasOption(SAVEPOINT_CLAIM_MODE)) {
                restoreMode =
                        org.apache.flink.configuration.ConfigurationUtils.convertValue(
                                commandLine.getOptionValue(SAVEPOINT_CLAIM_MODE),
                                ConfigurationUtils.getClaimModeClass());
            } else {
                restoreMode = SavepointConfigOptions.RESTORE_MODE.defaultValue();
            }
            // allowNonRestoredState is always false because all operators are predefined.
            // 使用反射构建 SavepointRestoreSettings
            return (SavepointRestoreSettings)
                    Arrays.stream(SavepointRestoreSettings.class.getMethods())
                            .filter(
                                    method ->
                                            method.getName().equals("forPath")
                                                    && method.getParameterCount() == 3)
                            .findFirst()
                            .map(
                                    method -> {
                                        try {
                                            return method.invoke(
                                                    null,
                                                    savepointPath,
                                                    allowNonRestoredState,
                                                    restoreMode);
                                        } catch (IllegalAccessException
                                                | InvocationTargetException e) {
                                            throw new RuntimeException(
                                                    "Failed to invoke SavepointRestoreSettings#forPath nethod.",
                                                    e);
                                        }
                                    })
                            .orElseThrow(
                                    () ->
                                            new RuntimeException(
                                                    "Failed to resolve SavepointRestoreSettings#forPath method."));
        } else {
            return SavepointRestoreSettings.none();
        }
    }
    // 核心作用是确定 Flink 运行环境的安装根目录（FLINK_HOME）。
    private static Path getFlinkHome(CommandLine commandLine) {
        // Check command line arguments first
        // 尝试从命令行中获取 -h 或 --flink-home 选项的值
        String flinkHomeFromArgs = commandLine.getOptionValue(CliFrontendOptions.FLINK_HOME);
        if (flinkHomeFromArgs != null) {
            LOG.debug("Flink home is loaded by command-line argument: {}", flinkHomeFromArgs);
            return new Path(flinkHomeFromArgs);
        }

        // Fallback to environment variable
        // 尝试获取名为 FLINK_HOME 的系统环境变量的值。
        String flinkHomeFromEnvVar = System.getenv(FLINK_HOME_ENV_VAR);
        if (flinkHomeFromEnvVar != null) {
            LOG.debug("Flink home is loaded by environment variable: {}", flinkHomeFromEnvVar);
            return new Path(flinkHomeFromEnvVar);
        }

        throw new IllegalArgumentException(
                "Cannot find Flink home from either command line arguments \"--flink-home\" "
                        + "or the environment variable \"FLINK_HOME\". "
                        + "Please make sure Flink home is properly set. ");
    }
    // 用于确定和加载 Flink CDC 的全局配置文件。
    private static Configuration getGlobalConfig(CommandLine commandLine) throws Exception {
        // Try to get global config path from command line
        // 尝试从命令行中获取全局配置文件的路径选项
        String globalConfig = commandLine.getOptionValue(CliFrontendOptions.GLOBAL_CONFIG);
        if (globalConfig != null) {
            Path globalConfigPath = new Path(globalConfig);
            LOG.info("Using global config in command line: {}", globalConfigPath);
            // 调用工具类方法加载指定路径的配置文件（例如 YAML 或 Properties 文件），并返回加载后的 Configuration 对象，程序执行结束。
            return ConfigurationUtils.loadConfigFile(globalConfigPath);
        }

        // Fallback to Flink CDC home
        String flinkCdcHome = System.getenv(FLINK_CDC_HOME_ENV_VAR);
        if (flinkCdcHome != null) {
            // 构造默认的全局配置文件路径。它将 FLINK_CDC_HOME 路径与 conf/flink-cdc.yaml 拼接起来。
            Path globalConfigPath =
                    new Path(
                            flinkCdcHome, Joiner.on(File.separator).join("conf", "flink-cdc.yaml"));
            LOG.info("Using global config in FLINK_CDC_HOME: {}", globalConfigPath);
            return ConfigurationUtils.loadConfigFile(globalConfigPath);
        }

        // Fallback to empty configuration
        LOG.warn(
                "Cannot find global configuration in command-line or FLINK_CDC_HOME. Will use empty global configuration.");
        return new Configuration();
    }

    private static void printExecutionInfo(PipelineExecution.ExecutionInfo info) {
        System.out.println("Pipeline has been submitted to cluster.");
        System.out.printf("Job ID: %s\n", info.getId());
        System.out.printf("Job Description: %s\n", info.getDescription());
    }
}
