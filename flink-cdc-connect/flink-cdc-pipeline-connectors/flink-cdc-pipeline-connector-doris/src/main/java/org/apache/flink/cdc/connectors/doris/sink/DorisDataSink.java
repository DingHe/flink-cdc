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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.batch.DorisBatchSink;

import java.io.Serializable;
import java.time.ZoneId;

/** A {@link DataSink} for "Doris" connector. */
// 连接 Flink CDC 框架与 Apache Doris 存储系统的核心桥梁。
// 数据写入代理：它负责根据配置创建流式（Streaming）或批式（Batch）的 Doris 写入算子，处理数据的 INSERT/UPDATE/DELETE。
// 结构同步代理：它负责创建 DorisMetadataApplier，使得上游数据库（如 MySQL）的 DDL 变更（如加列）能够自动在 Doris 中执行。
public class DorisDataSink implements DataSink, Serializable {
    // 基础连接配置。包含 Doris 的集群地址（FE 地址）、查询端口、用户名、密码等基本信息。
    private final DorisOptions dorisOptions;
    // 读取配置。虽然这是 Sink 类，但在某些复杂的写入场景（如分表写入或预检查）中，可能需要读取 Doris 的元数据信息。
    private final DorisReadOptions readOptions;
    // 执行参数配置。这是关键属性，包含了写入的具体行为设置，如：
    // 是否开启批处理模式 (enableBatchMode)
    // Stream Load 的频率、重试次数、缓冲区大小等。
    private final DorisExecutionOptions executionOptions;
    // Flink CDC 的通用配置对象。用于传递全局 Pipeline 级别的参数
    private Configuration configuration;
    // 时区设置。用于处理不同系统间的时间戳转换，确保源库与 Doris 之间的时间数据一致。
    private final ZoneId zoneId;

    public DorisDataSink(
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions dorisExecutionOptions,
            Configuration configuration,
            ZoneId zoneId) {
        this.dorisOptions = dorisOptions;
        this.readOptions = dorisReadOptions;
        this.executionOptions = dorisExecutionOptions;
        this.configuration = configuration;
        this.zoneId = zoneId;
    }
    // 根据执行模式，提供具体的 Doris 数据写入算子。
    @Override
    public EventSinkProvider getEventSinkProvider() {
        // 流模式 (Default)：返回 DorisSink。这是基于 Flink Sink V2 实现的，通常用于实时 CDC 同步，支持高频的小批次提交。
        if (!executionOptions.enableBatchMode()) {
            return FlinkSinkProvider.of(
                    new DorisSink<>(
                            dorisOptions,
                            readOptions,
                            executionOptions,
                            new DorisEventSerializer(zoneId, configuration)));
        } else {
            // 批模式：返回 DorisBatchSink。优化了大批量数据的写入性能，通常用于离线初始化或快照阶段。
            return FlinkSinkProvider.of(
                    new DorisBatchSink<>(
                            dorisOptions,
                            readOptions,
                            executionOptions,
                            new DorisEventSerializer(zoneId, configuration)));
        }
    }
    // 返回用于处理 DDL 的执行器。
    @Override
    public MetadataApplier getMetadataApplier() {
        return new DorisMetadataApplier(dorisOptions, configuration);
    }
}
