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

package org.apache.flink.cdc.common.event;

import org.apache.flink.cdc.common.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.List;

/** Deserializer to deserialize given record to {@link Event}. */
// 核心作用是将底层连接器捕获到的**原始记录（Raw Record）**转换为 Flink CDC 内部通用的 Event（事件）对象。
// 在传统的 Flink CDC 连接器中，数据转换通常依赖于 DebeziumDeserializationSchema。而在新的 Pipeline 架构中，为了实现更强的通用性和可扩展性，Flink CDC 引入了统一的事件模型（Event），包括：
//DataChangeEvent：代表数据的增删改。
//SchemaChangeEvent：代表表结构的变更（如 Add Column）。
// EventDeserializer 就像是一个标准化加工厂，无论输入的是 MySQL 的 Binlog 还是 PostgreSQL 的逻辑复制流，经过它的处理后，输出的都是统一格式的 Event。
@PublicEvolving
public interface EventDeserializer<T> extends Serializable {

    /** Deserialize given record to {@link Event}s. */
    List<? extends Event> deserialize(T record) throws Exception;
}
