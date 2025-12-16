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

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Definition of a data source.
 *
 * <p>A source definition contains:
 *
 * <ul>
 *   <li>type: connector type of the source, which will be used for discovering source
 *       implementation. Required in the definition.
 *   <li>name: name of the source. Optional in the definition.
 *   <li>config: configuration of the source
 * </ul>
 */
// 核心作用是以抽象和结构化的方式表示 Flink CDC 流水线中的一个数据输入端点
// 在解析用户定义的流水线文件（如 YAML 文件）后，用于在内存中存储和传递数据源所需的所有关键信息的数据结构。
// 它定义了“从哪里、以什么方式读取数据”：
// 连接器类型 (Type): 确定应该使用哪个 CDC 连接器（例如 MySQL、Postgres、Kafka）
// 名称 (Name): 用于在流水线中标识此源的逻辑名称（可选）
// 配置 (Config): 包含连接此源所需的所有详细参数（例如主机名、端口、数据库名、表列表等）。
public class SourceDef {
    // 数据源连接器类型。
    // 这是必需的，它指定了要使用的具体 CDC 连接器，例如 "mysql", "postgresql", 或 "kafka"。在运行时，它用于发现对应的 Source Connector 实现。
    private final String type;
    // 数据源的逻辑名称。
    // 这是可选的。如果提供，它在整个流水线定义中作为该数据源的唯一标识符。如果为 null，则表示没有指定名称。
    @Nullable private final String name;
    // 数据源的详细配置。
    // 这是一个 Configuration 对象，包含了连接和读取数据源所需的所有键值对参数，例如 hostname, port, username, password, database-name 等。
    private final Configuration config;

    public SourceDef(String type, @Nullable String name, Configuration config) {
        this.type = type;
        this.name = name;
        this.config = config;
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

    @Override
    public String toString() {
        return "SourceDef{"
                + "type='"
                + type
                + '\''
                + ", name='"
                + name
                + '\''
                + ", config="
                + config
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
        SourceDef sourceDef = (SourceDef) o;
        return Objects.equals(type, sourceDef.type)
                && Objects.equals(name, sourceDef.name)
                && Objects.equals(config, sourceDef.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, config);
    }
}
