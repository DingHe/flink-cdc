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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.RuntimeExecutionMode;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.composer.flink.translator.DataSourceTranslator;
import org.apache.flink.cdc.composer.flink.translator.PartitioningTranslator;
import org.apache.flink.cdc.composer.flink.translator.SchemaOperatorTranslator;
import org.apache.flink.cdc.composer.flink.translator.TransformTranslator;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Composer for translating data pipeline to a Flink DataStream job. */
// FlinkPipelineComposer 是 Flink CDC 管道编排器在 Flink DataStream API 上的具体实现。
// 核心职责： 它的主要职责是将一个抽象的 CDC 管道定义 (PipelineDef) 翻译、组装和配置成一个完整的、可运行的 Flink DataStream 作业拓扑（Job Graph）。
// 面向 Flink： 它专注于 Flink 特有的配置（如 StreamExecutionEnvironment、并行度、Runtime 模式）和 DataStream API 的操作（如 addSource、map、keyBy、addSink）。
// 拓扑构建： 它通过使用一系列的 Translator 工具类，按顺序将 Source、预转换、后转换、分区、Schema 演化操作以及 Sink 链接起来，形成一个完整的 CDC 数据流。
// 它是 Flink CDC 配置到 Flink 代码的转换引擎。
@Internal
public class FlinkPipelineComposer implements PipelineComposer {
    // Flink 执行环境
    // 存储用于构建和执行 Flink 作业的 StreamExecutionEnvironment 实例。它是 Flink DataStream 作业的配置入口和执行上下文。
    private final StreamExecutionEnvironment env;
    // 执行模式标志
    // 标记作业提交后是否应该阻塞当前进程，等待作业完成。
    // 通常在 ofMiniCluster() 场景中为 true，而在提交到远程集群时为 false。
    private final boolean isBlocking;
    // 创建远程集群编排器
    // 用于将 CDC 作业提交到远程 Flink 集群。
    // 它接收 Flink 配置和额外的 JAR 包路径，创建 StreamExecutionEnvironment 实例，
    // 并将所需的 JAR（如自定义 UDF 或连接器）添加到环境中。isBlocking 设置为 false。
    public static FlinkPipelineComposer ofRemoteCluster(
            org.apache.flink.configuration.Configuration flinkConfig, List<Path> additionalJars) {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment(flinkConfig);
        additionalJars.forEach(
                jarPath -> {
                    try {
                        FlinkEnvironmentUtils.addJar(
                                env,
                                jarPath.makeQualified(jarPath.getFileSystem()).toUri().toURL());
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Unable to convert JAR path \"%s\" to URL when adding JAR to Flink environment",
                                        jarPath),
                                e);
                    }
                });
        return new FlinkPipelineComposer(env, false);
    }
    // 创建应用集群编排器	用于在**应用模式（Application Mode）下执行作业。
    // 它接收一个已存在的 StreamExecutionEnvironment 实例（通常由 Flink 框架提供），并直接使用它来构建作业。
    // isBlocking 设置为 false。
    public static FlinkPipelineComposer ofApplicationCluster(StreamExecutionEnvironment env) {
        return new FlinkPipelineComposer(env, false);
    }

    // 创建 Mini Cluster 编排器	用于在本地 Mini Cluster** 或本地开发/测试中使用。
    // 它调用 StreamExecutionEnvironment.getExecutionEnvironment() 获取本地环境，并将 isBlocking 设置为 true，
    // 意味着提交作业后会等待作业运行完成。
    public static FlinkPipelineComposer ofMiniCluster() {
        return new FlinkPipelineComposer(
                StreamExecutionEnvironment.getExecutionEnvironment(), true);
    }

    private FlinkPipelineComposer(StreamExecutionEnvironment env, boolean isBlocking) {
        this.env = env;
        this.isBlocking = isBlocking;
    }
    // 负责将抽象的管道定义转换为可执行的 Flink 作业
    @Override
    public PipelineExecution compose(PipelineDef pipelineDef) {
        Configuration pipelineDefConfig = pipelineDef.getConfig();
        // 从配置中读取用户指定的管道并行度。PipelineOptions.PIPELINE_PARALLELISM 是 Flink CDC 定义的配置项键。
        int parallelism = pipelineDefConfig.get(PipelineOptions.PIPELINE_PARALLELISM);
        // 设置默认并行度
        env.getConfig().setParallelism(parallelism);
        // 构建拓扑
        translate(env, pipelineDef);

        // Add framework JARs
        addFrameworkJars();

        return new FlinkPipelineExecution(
                env, pipelineDefConfig.get(PipelineOptions.PIPELINE_NAME), isBlocking);
    }
    // 负责将抽象的 CDC 管道定义转化为实际的 Flink 算子链。
    private void translate(StreamExecutionEnvironment env, PipelineDef pipelineDef) {
        Configuration pipelineDefConfig = pipelineDef.getConfig();
        // 再次获取管道的并行度，用于后续算子的配置。
        int parallelism = pipelineDefConfig.get(PipelineOptions.PIPELINE_PARALLELISM);
        // 获取用户配置的、当源端表结构发生变化时，CDC 管道应该采取的行为（例如，是忽略、失败还是应用演化）。
        SchemaChangeBehavior schemaChangeBehavior =
                pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR);

        // 检查配置中指定的运行时执行模式是否为 BATCH
        boolean isBatchMode =
                RuntimeExecutionMode.BATCH.equals(
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_EXECUTION_RUNTIME_MODE));
        // 根据配置，设置 StreamExecutionEnvironment 的运行时模式为 BATCH（批处理）或 STREAMING（流处理）。
        if (isBatchMode) {
            env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.BATCH);
        } else {
            env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.STREAMING);
        }

        // Initialize translators
        // 初始化 Source 翻译器
        DataSourceTranslator sourceTranslator = new DataSourceTranslator();
        // 初始化 Transform 翻译器
        TransformTranslator transformTranslator = new TransformTranslator();
        // 初始化 Partitioning 翻译器
        PartitioningTranslator partitioningTranslator = new PartitioningTranslator();
        // 初始化 Schema Operator 翻译器
        // 这是最关键的翻译器之一，负责将 Schema 演化逻辑 (SchemaChangeBehavior) 转换为 Flink 算子。它的构造函数接收多项配置：
        // 1. schemaChangeBehavior：Schema 变更行为。
        // 2. PIPELINE_SCHEMA_OPERATOR_UID：Schema 算子的唯一标识符。
        // 3. PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT：Schema 算子 RPC（远程过程调用）的超时时间。
        // 4. PIPELINE_LOCAL_TIME_ZONE：本地时区设置。
        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        schemaChangeBehavior,
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID),
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT),
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        // 初始化 Sink 翻译器
        DataSinkTranslator sinkTranslator = new DataSinkTranslator();

        // And required constructors
        // 初始化算子 ID 生成器
        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());
        // 根据配置创建实际的 DataSource 实例。
        DataSource dataSource =
                sourceTranslator.createDataSource(pipelineDef.getSource(), pipelineDefConfig, env);
        DataSink dataSink =
                sinkTranslator.createDataSink(pipelineDef.getSink(), pipelineDefConfig, env);

        boolean isParallelMetadataSource = dataSource.isParallelMetadataSource();

        // O ---> Source
        DataStream<Event> stream =
                sourceTranslator.translate(pipelineDef.getSource(), dataSource, env, parallelism);

        // Source ---> PreTransform
        // 解析出引用的所有列，然后生成所有列组成的schema的数据记录
        stream =
                transformTranslator.translatePreTransform(
                        stream,
                        pipelineDef.getTransforms(),
                        pipelineDef.getUdfs(),
                        pipelineDef.getModels(),
                        dataSource.supportedMetadataColumns(),
                        !isParallelMetadataSource && !isBatchMode);

        // PreTransform ---> PostTransform
        // 执行数据的转换规则
        stream =
                transformTranslator.translatePostTransform(
                        stream,
                        pipelineDef.getTransforms(),
                        pipelineDef.getConfig().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                        pipelineDef.getUdfs(),
                        pipelineDef.getModels(),
                        dataSource.supportedMetadataColumns());

        if (isParallelMetadataSource) {
            // Translate a distributed topology for sources with distributed tables
            // PostTransform -> Partitioning
            DataStream<PartitioningEvent> partitionedStream =
                    partitioningTranslator.translateDistributed(
                            stream,
                            parallelism,
                            parallelism,
                            dataSink.getDataChangeEventHashFunctionProvider(parallelism));

            // Partitioning -> Schema Operator
            stream =
                    schemaOperatorTranslator.translateDistributed(
                            partitionedStream,
                            parallelism,
                            dataSink.getMetadataApplier()
                                    .setAcceptedSchemaEvolutionTypes(
                                            pipelineDef
                                                    .getSink()
                                                    .getIncludedSchemaEvolutionTypes()),
                            pipelineDef.getRoute());

        } else {
            // Translate a regular topology for sources without distributed tables
            // PostTransform ---> Schema Operator
            stream =
                    schemaOperatorTranslator.translateRegular(
                            stream,
                            parallelism,
                            isBatchMode,
                            dataSink.getMetadataApplier()
                                    .setAcceptedSchemaEvolutionTypes(
                                            pipelineDef
                                                    .getSink()
                                                    .getIncludedSchemaEvolutionTypes()),
                            pipelineDef.getRoute());

            // Schema Operator ---(shuffled)---> Partitioning
            stream =
                    partitioningTranslator.translateRegular(
                            stream,
                            parallelism,
                            parallelism,
                            isBatchMode,
                            schemaOperatorIDGenerator.generate(),
                            dataSink.getDataChangeEventHashFunctionProvider(parallelism));
        }

        // Schema Operator -> Sink -> X
        sinkTranslator.translate(
                pipelineDef.getSink(),
                stream,
                dataSink,
                isBatchMode,
                schemaOperatorIDGenerator.generate());
    }

    private void addFrameworkJars() {
        try {
            Set<URI> frameworkJars = new HashSet<>();
            // Common JAR
            // We use the core interface (Event) to search the JAR
            Optional<URL> commonJar = getContainingJar(Event.class);
            if (commonJar.isPresent()) {
                frameworkJars.add(commonJar.get().toURI());
            }
            // Runtime JAR
            // We use the serializer of the core interface (EventSerializer) to search the JAR
            Optional<URL> runtimeJar = getContainingJar(EventSerializer.class);
            if (runtimeJar.isPresent()) {
                frameworkJars.add(runtimeJar.get().toURI());
            }
            for (URI jar : frameworkJars) {
                FlinkEnvironmentUtils.addJar(env, jar.toURL());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to search and add Flink CDC framework JARs", e);
        }
    }

    private Optional<URL> getContainingJar(Class<?> clazz) throws Exception {
        URL container = clazz.getProtectionDomain().getCodeSource().getLocation();
        if (Files.isDirectory(Paths.get(container.toURI()))) {
            return Optional.empty();
        }
        return Optional.of(container);
    }
}
