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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.FlinkEnvironmentUtils;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.runtime.operators.sink.BatchDataSinkFunctionOperator;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkFunctionOperator;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkWriterOperatorFactory;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;

import java.lang.reflect.InvocationTargetException;

/** Translator used to build {@link DataSink} for given {@link DataStream}. */
// DataSinkTranslator 是 Pipeline 编排层与 Flink 算子层的“翻译官”。它负责将用户在配置文件中定义的抽象 Sink 描述，转化成 Flink 运行时可以执行的 DataStream 转换（Transformation）和具体的 Operator。
// DataSinkTranslator 的核心作用是将 逻辑定义的 Sink 转换为 物理执行的 Flink 算子链。
// 它屏蔽了 Flink 不同 Sink 版本的差异（Sink V2 和旧版 SinkFunction），并处理了 CDC 场景下特有的 Schema 变更对齐（与 SchemaOperator 协同）以及 二阶段提交（2PC） 拓扑的构建。
@Internal
public class DataSinkTranslator {
    // 字符串常量 "Sink Writer: "。
    // 用于为 Flink Web UI 中的写入算子生成易读的名称。
    private static final String SINK_WRITER_PREFIX = "Sink Writer: ";
    // 字符串常量 "Sink Committer: "。用于为二阶段提交中的提交算子生成名称。
    private static final String SINK_COMMITTER_PREFIX = "Sink Committer: ";
    // 根据配置实例化 DataSink 对象
    public DataSink createDataSink(
            SinkDef sinkDef, Configuration pipelineConfig, StreamExecutionEnvironment env) {
        // Search the data sink factory
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sinkDef.getType(), DataSinkFactory.class);

        // Include sink connector JAR
        // 自动将 Connector 的 JAR 包加入 Flink 环境
        FactoryDiscoveryUtils.getJarPathByIdentifier(sinkFactory)
                .ifPresent(jar -> FlinkEnvironmentUtils.addJar(env, jar));

        // Create data sink
        // 最后调用工厂方法创建 DataSink。
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        sinkDef.getConfig(),
                        pipelineConfig,
                        Thread.currentThread().getContextClassLoader()));
    }
    public void translate(
            SinkDef sinkDef,
            DataStream<Event> input,
            DataSink dataSink,
            OperatorID schemaOperatorID) {
        translate(sinkDef, input, dataSink, false, schemaOperatorID);
    }
    // 检查 DataSink 提供的是 Sink V2 (FlinkSinkProvider) 还是 旧版 SinkFunction (FlinkSinkFunctionProvider)，然后跳转到对应的 sinkTo 私有方法。
    public void translate(
            SinkDef sinkDef,
            DataStream<Event> input,
            DataSink dataSink,
            boolean isBatchMode,
            OperatorID schemaOperatorID) {
        // Get sink provider
        EventSinkProvider eventSinkProvider = dataSink.getEventSinkProvider();
        String sinkName = generateSinkName(sinkDef);
        if (eventSinkProvider instanceof FlinkSinkProvider) {
            // Sink V2
            FlinkSinkProvider sinkProvider = (FlinkSinkProvider) eventSinkProvider;
            Sink<Event> sink = sinkProvider.getSink();
            sinkTo(input, sink, sinkName, isBatchMode, schemaOperatorID);
        } else if (eventSinkProvider instanceof FlinkSinkFunctionProvider) {
            // SinkFunction
            FlinkSinkFunctionProvider sinkFunctionProvider =
                    (FlinkSinkFunctionProvider) eventSinkProvider;
            SinkFunction<Event> sinkFunction = sinkFunctionProvider.getSinkFunction();
            sinkTo(input, sinkFunction, sinkName, isBatchMode, schemaOperatorID);
        }
    }
    // 负责将 Flink CDC 的数据流（DataStream<Event>）正式连接到 Flink 的 Sink V2 算子上。
    // 它不仅处理了普通的数据写入，还考虑了事务性写入（2PC）和写入前的拓扑转换。
    @VisibleForTesting
    void sinkTo(
            DataStream<Event> input, // 输入的数据流，承载的是 CDC Event
            Sink<Event> sink, // 具体的 Sink 实现对象（如 DorisSink, StarRocksSink）
            String sinkName, // Sink 的名称，用于 UI 显示
            boolean isBatchMode, // 是否为批处理模式
            OperatorID schemaOperatorID) { // 上游 SchemaOperator 的唯一 ID，用于元数据同步
        DataStream<Event> stream = input;
        // Pre-write topology
        // 检查 Sink 是否实现了 WithPreWriteTopology 接口。
        // 有些 Sink 要求数据在写入前必须经过特定处理。
        // Iceberg 或某些分桶存储系统可能要求在写入前按主键进行 keyBy 重分区，以减少文件句柄开销。
        if (sink instanceof WithPreWriteTopology) {
            stream = ((WithPreWriteTopology<Event>) sink).addPreWriteTopology(stream);
        }
        // 判断是否支持二阶段提交 (2PC)
        // 如果 Sink 实现了 TwoPhaseCommittingSink，说明它支持 Exactly-Once（精准一次） 语义。
        if (sink instanceof TwoPhaseCommittingSink) {
            // 该方法会额外添加 Committer 算子，负责在 Flink Checkpoint 完成后提交事务。
            addCommittingTopology(sink, stream, sinkName, isBatchMode, schemaOperatorID);
        } else {
            // 处理普通写入（非事务性 Sink）
            // 如果 Sink 不支持事务（仅支持 At-Least-Once 或更弱的语义），则直接构建写入算子。
            stream.transform(
                    SINK_WRITER_PREFIX + sinkName,
                    CommittableMessageTypeInfo.noOutput(),
                    new DataSinkWriterOperatorFactory<>(sink, isBatchMode, schemaOperatorID));
        }
    }
    // 处理 旧版 Sink 接口（SinkFunction） 的实现逻辑。它的主要任务是根据运行模式（流或批）将用户定义的 SinkFunction 包装成 Flink 内部的物理算子（Operator），并将其挂载到 Flink 的执行计划中。
    private void sinkTo(
            DataStream<Event> input,
            SinkFunction<Event> sinkFunction,
            String sinkName,
            boolean isBatchMode,
            OperatorID schemaOperatorID) {
        // 在 Flink 引擎中，所有的 SinkFunction 必须被包装在一个 StreamOperator（具体到 Sink 则是 StreamSink）中才能运行。这个变量将持有包装后的算子实例。
        StreamSink<Event> sinkOperator;
        // isBatchMode (批模式)：使用 BatchDataSinkFunctionOperator。批模式通常不需要处理复杂的分布式 DDL 同步，因此逻辑较简单。
        if (isBatchMode) {
            sinkOperator = new BatchDataSinkFunctionOperator(sinkFunction);
        } else {
            // 关键意义：在流模式下，该算子需要与 SchemaRegistry 通信。当它收到 DDL 屏障时，必须上报给协调器，确保分布式环境下 Schema 变更的正确性。
            sinkOperator = new DataSinkFunctionOperator(sinkFunction, schemaOperatorID);
        }
        final StreamExecutionEnvironment executionEnvironment = input.getExecutionEnvironment();
        PhysicalTransformation<Event> transformation =
                new LegacySinkTransformation<>(
                        input.getTransformation(), // // 上游的转换流程
                        SINK_WRITER_PREFIX + sinkName, // 算子在 UI 上显示的名称
                        sinkOperator, // 刚才创建的包装算子
                        executionEnvironment.getParallelism(), // 使用环境默认的并行度
                        false); // 是否允许修改并行度（此处为 false）
        executionEnvironment.addOperator(transformation);
    }
    // Flink CDC 实现 Exactly-Once（精准一次） 语义的关键逻辑。它基于 Flink Sink V2 架构，构建了一个完整的二阶段提交（2PC）流水线。
    // <CommT>: 这是一个泛型占位符，代表 Committable（可提交信息）。它是 Sink 在第一阶段写入完成后，传递给第二阶段提交的具体元数据（例如文件路径、事务 ID 等）。
    private <CommT> void addCommittingTopology(
            Sink<Event> sink,
            DataStream<Event> inputStream,
            String sinkName,
            boolean isBatchMode,
            OperatorID schemaOperatorID) {
        TypeInformation<CommittableMessage<CommT>> typeInformation =
                CommittableMessageTypeInfo.of(() -> getCommittableSerializer(sink));
        // 构建 Sink Writer（第一阶段：写入/预提交）
        // 将原始数据流转换为“待提交消息流”。
        DataStream<CommittableMessage<CommT>> written =
                inputStream.transform(
                        SINK_WRITER_PREFIX + sinkName,
                        typeInformation,
                        new DataSinkWriterOperatorFactory<>(sink, isBatchMode, schemaOperatorID));
        // 处理预提交拓扑（可选）
        // 如果 Sink 需要在正式提交前进行额外处理。
        // 场景：例如，某些存储系统要求在提交前先对所有分片进行一次聚合或校验。如果 Sink 实现了 WithPreCommitTopology 接口，这里会插入额外的算子逻辑。
        DataStream<CommittableMessage<CommT>> preCommitted = written;
        if (sink instanceof WithPreCommitTopology) {
            preCommitted =
                    ((WithPreCommitTopology<Event, CommT>) sink).addPreCommitTopology(written);
        }

        // TODO: Hard coding checkpoint
        // 构建 Sink Committer（第二阶段：正式提交）
        // 事务提交强依赖 Checkpoint。这里硬编码为 true（CDC 场景下通常必须开启）。
        // 核心逻辑：它会监听 Flink 的 notifyCheckpointComplete() 回调。只有当 JobManager 确认 Checkpoint 成功完成后，该算子才会取出刚才收到的 CommittableMessage，真正执行外部系统的事务提交。
        boolean isCheckpointingEnabled = true;
        DataStream<CommittableMessage<CommT>> committed =
                preCommitted.transform(
                        SINK_COMMITTER_PREFIX + sinkName,
                        typeInformation,
                        getCommitterOperatorFactory(sink, isBatchMode, isCheckpointingEnabled));
        // 如果提交成功后需要做扫尾工作。
        // 场景：例如，清理临时目录、发送成功通知或更新元数据索引。如果 Sink 实现了 WithPostCommitTopology，则在此处挂载最后的处理逻辑。
        if (sink instanceof WithPostCommitTopology) {
            ((WithPostCommitTopology<Event, CommT>) sink).addPostCommitTopology(committed);
        }
    }

    private String generateSinkName(SinkDef sinkDef) {
        return sinkDef.getName()
                .orElse(String.format("Flink CDC Event Sink: %s", sinkDef.getType()));
    }

    private static <CommT> SimpleVersionedSerializer<CommT> getCommittableSerializer(Object sink) {
        // FIX ME: TwoPhaseCommittingSink has been deprecated, and its signature has changed
        // during Flink 1.18 to 1.19. Remove this when Flink 1.18 is no longer supported.
        try {
            return (SimpleVersionedSerializer<CommT>)
                    sink.getClass().getDeclaredMethod("getCommittableSerializer").invoke(sink);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to get CommittableSerializer", e);
        }
    }

    private static <CommT>
            OneInputStreamOperatorFactory<CommittableMessage<CommT>, CommittableMessage<CommT>>
                    getCommitterOperatorFactory(
                            Sink<Event> sink, boolean isBatchMode, boolean isCheckpointingEnabled) {
        // FIX ME: OneInputStreamOperatorFactory is an @Internal class, and its signature has
        // changed during Flink 1.18 to 1.19. Remove this when Flink 1.18 is no longer supported.
        try {
            return (OneInputStreamOperatorFactory<
                            CommittableMessage<CommT>, CommittableMessage<CommT>>)
                    Class.forName(
                                    "org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory")
                            .getDeclaredConstructors()[0]
                            .newInstance(sink, isBatchMode, isCheckpointingEnabled);

        } catch (ClassNotFoundException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
            throw new RuntimeException("Failed to create CommitterOperatorFactory", e);
        }
    }
}
