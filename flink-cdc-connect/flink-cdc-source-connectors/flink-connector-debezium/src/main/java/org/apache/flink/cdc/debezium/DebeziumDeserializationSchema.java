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

package org.apache.flink.cdc.debezium;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the Debezium SourceRecord into data types
 * (Java/Scala objects) that are processed by Flink.
 *
 * @param <T> The type created by the deserialization schema.
 */
// 定义了如何将底层连接器捕获到的原始数据转换为 Flink 可以处理的数据类型。
// 在 Flink CDC 的工作流程中，底层的增量数据捕获通常是由 Debezium 引擎完成的。
// Debezium 在捕获到数据库变更（如 MySQL 的 Binlog）后，会将其封装成 Kafka Connect 的 SourceRecord 格式。
// DebeziumDeserializationSchema 的作用就是充当 “翻译官”：
// 格式转换：它负责将复杂的、嵌套的 SourceRecord（包含 before、after 数据、source 元数据等）解析并转换成 Flink 算子能够识别和处理的具体 Java/Scala 对象（例如 RowData、String 或自定义的 POJO）。
// 解耦：它将底层的变更数据捕获（CDC）逻辑与上层的数据处理逻辑解耦。开发者可以通过实现这个接口，自由定义数据的输出格式。
// T  表示反序列化后生成的结果类型。常见的实现中，如果是 Flink SQL 模式，T 通常是 RowData；如果是 DataStream 模式，用户可以自定义为 String 或自定义的 Bean。
@PublicEvolving
public interface DebeziumDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /** Deserialize the Debezium record, it is represented in Kafka {@link SourceRecord}. */
    // SourceRecord record:  Debezium 输出的原始记录。
    // 参数 Collector<T> out: Flink 的收集器。将 record 转换成目标类型 T 后，调用 out.collect(element) 将数据发送到下游。
    void deserialize(SourceRecord record, Collector<T> out) throws Exception;
}
