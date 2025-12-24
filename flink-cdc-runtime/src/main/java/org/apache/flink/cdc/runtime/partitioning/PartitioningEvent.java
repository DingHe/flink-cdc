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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;

import java.util.Objects;

/**
 * A wrapper around {@link Event}, which contains the target partition number and will be used in
 * {@link EventPartitioner}.
 */
// PartitioningEvent 的核心作用是 “给 CDC 事件打标签以实现精准分区”。
// 在 Flink 算子之间传输数据时，如果直接传输原始 Event（如 DataChangeEvent），Flink 的分区器（Partitioner）往往需要重新解析事件内容才能决定发往哪个并行子任务，这会带来性能损耗。
// PartitioningEvent 通过装饰器模式封装了原始事件，并额外携带了“目标分区号（Target Partition）”：
// 路由导向：它显式告诉 EventPartitioner 这条数据应该去往哪个下游 Subtask。
@Internal
public class PartitioningEvent implements Event {
    private final Event payload;  // 实际携带的“货物”
    private final int sourcePartition;  // 标识该事件来自上游哪个并行子任务（Subtask ID）
    private final int targetPartition; // 标识该事件应该去往哪个下游并行子任务。

    /**
     * For partitioning events with regular topology, source partition information is not necessary.
     */
    public static PartitioningEvent ofRegular(Event payload, int targetPartition) {
        return new PartitioningEvent(payload, -1, targetPartition);
    }

    /**
     * For distributed topology, we need to track its upstream source subTask ID to correctly
     * distinguish events from different partitions.
     */
    public static PartitioningEvent ofDistributed(
            Event payload, int sourcePartition, int targetPartition) {
        return new PartitioningEvent(payload, sourcePartition, targetPartition);
    }

    private PartitioningEvent(Event payload, int sourcePartition, int targetPartition) {
        this.payload = payload;
        this.sourcePartition = sourcePartition;
        this.targetPartition = targetPartition;
    }

    public Event getPayload() {
        return payload;
    }

    public int getSourcePartition() {
        return sourcePartition;
    }

    public int getTargetPartition() {
        return targetPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitioningEvent that = (PartitioningEvent) o;
        return sourcePartition == that.sourcePartition
                && targetPartition == that.targetPartition
                && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, sourcePartition, targetPartition);
    }

    @Override
    public String toString() {
        return "PartitioningEvent{"
                + "payload="
                + payload
                + ", sourcePartition="
                + sourcePartition
                + ", targetPartition="
                + targetPartition
                + '}';
    }
}
