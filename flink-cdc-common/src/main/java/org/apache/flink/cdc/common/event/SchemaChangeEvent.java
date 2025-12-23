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

/**
 * Class {@code SchemaChangeEvent} represents the changes in the table structure of the external
 * system, such as CREATE, DROP, RENAME and so on.
 */
// DDL 事件抽象：它将各种复杂的数据库结构操作（如建表、删表、增加列、修改列名等）统一抽象为一种事件类型。
// Schema 演进（Schema Evolution）驱动：它是 Flink CDC 实现“自动同步表结构变更”的基石。当上游数据库执行了 DDL，该事件会流向下游，通知 Transform 算子更新内部缓存，或通知 Sink 算子在目标端同步修改表结构。
// 拓扑解耦：通过接口定义，Flink CDC 框架可以统一处理来自不同数据库的结构变更，而不需要关心底层数据库具体的 SQL 语法。
@PublicEvolving
public interface SchemaChangeEvent extends ChangeEvent, Serializable {
    /** Returns its {@link SchemaChangeEventType}. */
    // 获取该结构变更的具体类型。
    SchemaChangeEventType getType();

    /** Creates a copy of {@link SchemaChangeEvent} with new {@link TableId}. */
    SchemaChangeEvent copy(TableId newTableId);
}
