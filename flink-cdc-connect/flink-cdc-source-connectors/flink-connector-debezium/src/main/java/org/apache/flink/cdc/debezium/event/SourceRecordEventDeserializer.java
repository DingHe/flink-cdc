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

package org.apache.flink.cdc.debezium.event;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.EventDeserializer;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Deserializer to deserialize {@link SourceRecord} to {@link Event}. */
// 为基于 Debezium 的反序列化器提供一套通用的模板（Template）和骨架。
// 在 Flink CDC 的 Pipeline 架构中，数据流由统一的 Event 对象组成。然而，许多连接器（如 MySQL、Postgres）底层仍然依赖 Debezium 来获取原始记录（即 SourceRecord）。
// 模板方法设计模式：它实现了 deserialize 方法的主逻辑（即：判断记录类型 -> 分发给具体的处理函数），而将“如何判断”和“如何解析”的细节留给具体的子类（如 MySqlEventDeserializer）去实现。
// 分类分发：它将繁杂的原始 SourceRecord 归类为三类：数据变更（DML）、结构变更（DDL）或无意义记录（如心跳）。
// 屏蔽底层复杂性：它为子类提供了操作 Kafka Connect 数据结构的工具方法，简化了从复杂的嵌套 Struct 中提取信息的过程。
@Internal
public abstract class SourceRecordEventDeserializer implements EventDeserializer<SourceRecord> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SourceRecordEventDeserializer.class);
    // 这是该类的“心脏”，实现了 EventDeserializer 接口定义的方法。
    @Override
    public List<? extends Event> deserialize(SourceRecord record) throws Exception {
        // 判断是否为数据变更
        if (isDataChangeRecord(record)) {
            LOG.trace("Process data change record: {}", record);
            return deserializeDataChangeRecord(record);
         // 判断是否为结构变更
        } else if (isSchemaChangeRecord(record)) {
            LOG.trace("Process schema change record: {}", record);
            return deserializeSchemaChangeRecord(record);
        } else {
            LOG.trace("Ignored other record: {}", record);
            return Collections.emptyList();
        }
    }

    /** Whether the given record is a data change record. */
    protected abstract boolean isDataChangeRecord(SourceRecord record);

    /** Whether the given record is a schema change record. */
    protected abstract boolean isSchemaChangeRecord(SourceRecord record);

    /** Deserialize given data change record to {@link DataChangeEvent}. */
    protected abstract List<DataChangeEvent> deserializeDataChangeRecord(SourceRecord record)
            throws Exception;

    /** Deserialize given schema change record to {@link SchemaChangeEvent}. */
    protected abstract List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record)
            throws Exception;

    /** Get {@link TableId} from data change record. */
    protected abstract TableId getTableId(SourceRecord record);

    /** Get metadata from data change record. */
    protected abstract Map<String, String> getMetadata(SourceRecord record);
    // 从给定的 Schema 对象中获取指定字段名（fieldName）的子 Schema。
    public static Schema fieldSchema(Schema schema, String fieldName) {
        return schema.field(fieldName).schema();
    }
    // 从一个 Struct（结构体对象）中获取指定字段名的子 Struct。
    public static Struct fieldStruct(Struct value, String fieldName) {
        return value.getStruct(fieldName);
    }
}
