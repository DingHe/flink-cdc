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

/**
 * Class {@code ChangeEvent} represents the change events of external systems, including {@link
 * DataChangeEvent} and {@link SchemaChangeEvent}.
 */
// 统一变更抽象：它将数据变更（DataChangeEvent，如 INSERT/UPDATE/DELETE）和结构变更（SchemaChangeEvent，如 CREATE/ALTER TABLE）统一起来。
// 这意味着下游算子（如之前提到的 PreTransformOperator）可以通过判断一个事件是否属于 ChangeEvent，来确定该事件是否与某个具体的表结构相关。
// 强制标识归属：所有的变更事件都必须能够明确指出自己发生在“哪张表”上。ChangeEvent 通过强制实现 tableId() 方法，确保了变更流中的每一个元素都能被定位到具体的物理或逻辑表。


@PublicEvolving
public interface ChangeEvent extends Event {

    /** Describes the database table corresponding to the occurrence of a change event. */
    // 用于获取触发此变更事件的源表标识符。
    TableId tableId();
}
