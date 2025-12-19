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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceFunctionProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkEnvironmentUtils;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Translator used to build {@link DataSource} which will generate a {@link DataStream}. */
// 负责将用户定义的配置（SourceDef）转换为实际可运行的 Flink DataStream 应用程序。
// DataSourceTranslator 的核心作用是充当 “翻译官” 或 “适配器”。
// 它负责将用户在 CDC Pipeline 配置中定义的逻辑数据源定义（SourceDef，例如指定了 type='mysql' 和连接参数）转化为 Flink 运行时可以理解和执行的 DataStreamSource。
// 核心职责：
// 工厂发现与实例化： 根据用户定义的类型（如 mysql），发现并加载对应的 DataSourceFactory。
// 构建 DataSource： 利用发现的工厂和配置参数，实例化一个逻辑上的 DataSource 对象。
// 适配 Flink API： 将 DataSource 中封装的 EventSourceProvider（可能是新的 Flink Source API 或传统的 SourceFunction API）适配和集成到当前的 StreamExecutionEnvironment 中，生成最终的 DataStreamSource<Event>，从而开始数据流的捕获。


@Internal
public class DataSourceTranslator {

    // 负责将逻辑上的 DataSource 变成物理上的 DataStream
    // 将 DataSource 注册到 Flink 的 StreamExecutionEnvironment 中，生成包含 Event（Flink CDC 内部事件格式）的流。
    public DataStreamSource<Event> translate(
            SourceDef sourceDef, // 用户的源配置定义，包含名称、类型和原始配置 Map。
            DataSource dataSource, // 由 createDataSource 方法创建的逻辑数据源对象，封装了底层连接器的提供者。
            StreamExecutionEnvironment env, // Flink 的流执行环境，用于注册和启动 Source。
            int sourceParallelism) { // Source 任务的并行度。
        // Get source provider
        EventSourceProvider eventSourceProvider = dataSource.getEventSourceProvider();

        // 如果提供者是 FlinkSourceProvider，则调用 env.fromSource(...)。
        //它会自动配置 WatermarkStrategy.noWatermarks()（因为 CDC 数据通常不需要传统的时间窗口水位线）和 EventTypeInfo（CDC 内部序列化器）
        if (eventSourceProvider instanceof FlinkSourceProvider) {
            // Source
            FlinkSourceProvider sourceProvider = (FlinkSourceProvider) eventSourceProvider;
            return env.fromSource(
                            sourceProvider.getSource(),
                            WatermarkStrategy.noWatermarks(),
                            sourceDef.getName().orElse(generateDefaultSourceName(sourceDef)),
                            new EventTypeInfo())
                    .setParallelism(sourceParallelism);
        } else if (eventSourceProvider instanceof FlinkSourceFunctionProvider) {
            // SourceFunction
            FlinkSourceFunctionProvider sourceFunctionProvider =
                    (FlinkSourceFunctionProvider) eventSourceProvider;
            DataStreamSource<Event> stream =
                    env.addSource(sourceFunctionProvider.getSourceFunction(), new EventTypeInfo())
                            .setParallelism(sourceParallelism);
            if (sourceDef.getName().isPresent()) {
                stream.name(sourceDef.getName().get());
            }
            return stream;
        } else {
            // Unknown provider type
            throw new IllegalStateException(
                    String.format(
                            "Unsupported EventSourceProvider type \"%s\"",
                            eventSourceProvider.getClass().getCanonicalName()));
        }
    }
    // 该方法负责“从无到有”创建逻辑数据源对象。
    public DataSource createDataSource(
            SourceDef sourceDef, Configuration pipelineConfig, StreamExecutionEnvironment env) {
        // Search the data source factory
        // 根据配置的标识符查找数据源工厂类
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sourceDef.getType(), DataSourceFactory.class);
        // Add source JAR to environment
        // 把找到的类的jar添加到flink的类路径
        FactoryDiscoveryUtils.getJarPathByIdentifier(sourceFactory)
                .ifPresent(jar -> FlinkEnvironmentUtils.addJar(env, jar));
        FactoryHelper.DefaultContext context =
                new FactoryHelper.DefaultContext(
                        sourceDef.getConfig(),
                        pipelineConfig,
                        Thread.currentThread().getContextClassLoader());
        // 创建数据源类
        return sourceFactory.createDataSource(context);
    }

    private String generateDefaultSourceName(SourceDef sourceDef) {
        return String.format("Flink CDC Event Source: %s", sourceDef.getType());
    }
}
