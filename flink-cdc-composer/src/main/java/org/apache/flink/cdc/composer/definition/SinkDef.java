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

package org.apache.flink.cdc.composer.definition;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Definition of a data sink.
 *
 * <p>A sink definition contains:
 *
 * <ul>
 *   <li>type: connector type of the sink, which will be used for discovering sink implementation.
 *       Required in the definition.
 *   <li>name: name of the sink. Optional in the definition.
 *   <li>config: configuration of the sink
 * </ul>
 */
// 用于描述数据汇（Sink）的 POJO 类 SinkDef。它与 SourceDef 类似，都属于 composer.definition 包，用于在抽象层面定义流水线中的一个组件。
// 在解析用户定义的流水线文件后，用于在内存中存储和传递数据汇所需的所有关键信息的数据结构。这些信息将被 PipelineComposer 用来发现和实例化具体的 Flink CDC Sink Connector。
// Schema 演进类型 (Schema Evolution): 特别地，它还指定了哪些类型的 Schema 变更事件应该被写入到目标数据汇。
public class SinkDef {
    // 数据汇连接器类型。
    // 必需，指定了要使用的具体 CDC Sink Connector，例如 "kafka", "hudi", 或 "jdbc"。
    private final String type;
    // 数据汇的逻辑名称。
    // 可选。如果提供，它在整个流水线定义中作为该数据汇的唯一标识符。
    @Nullable private final String name;
    // 数据汇的详细配置。
    // 包含了连接和写入数据汇所需的所有键值对参数，例如目标地址、写入模式、批大小等。
    private final Configuration config;
    // 包含的 Schema 演进事件类型。
    // 指定哪些类型的 Schema 变更事件（如添加列、删除表等）应该被 Sink Connector 处理并传递到目标数据汇中。
    private final Set<SchemaChangeEventType> includedSchemaEvolutionTypes;

    public SinkDef(String type, @Nullable String name, Configuration config) {
        this.type = type;
        this.name = name;
        this.config = config;
        this.includedSchemaEvolutionTypes =
                Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet());
    }

    public SinkDef(
            String type,
            @Nullable String name,
            Configuration config,
            Set<SchemaChangeEventType> includedSchemaEvolutionTypes) {
        this.type = type;
        this.name = name;
        this.config = config;
        this.includedSchemaEvolutionTypes = includedSchemaEvolutionTypes;
    }

    public String getType() {
        return type;
    }

    public Optional<String> getName() {
        return Optional.ofNullable(name);
    }

    public Configuration getConfig() {
        return config;
    }

    public Set<SchemaChangeEventType> getIncludedSchemaEvolutionTypes() {
        return includedSchemaEvolutionTypes;
    }

    @Override
    public String toString() {
        return "SinkDef{"
                + "type='"
                + type
                + '\''
                + ", name='"
                + name
                + '\''
                + ", config="
                + config
                + ", includedSchemaEvolutionTypes="
                + includedSchemaEvolutionTypes
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SinkDef sinkDef = (SinkDef) o;
        return Objects.equals(type, sinkDef.type)
                && Objects.equals(name, sinkDef.name)
                && Objects.equals(config, sinkDef.config)
                && Objects.equals(
                        includedSchemaEvolutionTypes, sinkDef.includedSchemaEvolutionTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, config, includedSchemaEvolutionTypes);
    }
}
