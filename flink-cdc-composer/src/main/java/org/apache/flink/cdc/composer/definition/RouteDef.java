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

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Definition of a router.
 *
 * <p>A router definition contains:
 *
 * <ul>
 *   <li>sourceTable: a regex pattern for matching input table IDs. Required for the definition.
 *   <li>sinkTable: a string for replacing matched table IDs as output. Required for the definition.
 *   <li>description: description for the router. Optional for the definition.
 * </ul>
 */
// 用于描述路由规则（Routing Rule）的 POJO 类 RouteDef。它属于 composer.definition 包，用于在抽象层面定义数据流经 CDC 管道时，如何将源表映射到目标表。
// 在 Flink CDC 中，数据流通常以表 ID (Table ID) 为单位进行传输。
// 当数据从 Source 端读取后，在写入 Sink 端之前，可能需要根据业务需求更改其目标表名。RouteDef 就是用于定义这种表名转换和路由逻辑的数据结构。
public class RouteDef {
    // 源表模式 (SourceTable): 使用正则表达式匹配来自 Source 的输入表 ID。
    private final String sourceTable;
    // 目标表格式 (SinkTable): 使用一个替换字符串，结合捕获组，生成目标 Sink 表 ID。
    private final String sinkTable;
    // 替换符号 (ReplaceSymbol): 用于指定在替换操作中使用的特殊占位符（虽然在提供的代码中其用法不完全清晰，但通常与表名替换逻辑相关）。
    private final String replaceSymbol;
    // 路由规则的描述。 这是可选的，用于对该路由规则提供人性化的说明或备注。
    @Nullable private final String description;

    public RouteDef(
            String sourceTable,
            String sinkTable,
            @Nullable String replaceSymbol,
            @Nullable String description) {
        this.sourceTable = sourceTable;
        this.sinkTable = sinkTable;
        this.replaceSymbol = replaceSymbol;
        this.description = description;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public Optional<String> getReplaceSymbol() {
        return Optional.ofNullable(replaceSymbol);
    }

    public Optional<String> getDescription() {
        return Optional.ofNullable(description);
    }

    @Override
    public String toString() {
        return "RouteDef{"
                + "sourceTable="
                + sourceTable
                + ", sinkTable="
                + sinkTable
                + ", replaceSymbol="
                + replaceSymbol
                + ", description='"
                + description
                + '\''
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
        RouteDef routeDef = (RouteDef) o;
        return Objects.equals(sourceTable, routeDef.sourceTable)
                && Objects.equals(sinkTable, routeDef.sinkTable)
                && Objects.equals(replaceSymbol, routeDef.replaceSymbol)
                && Objects.equals(description, routeDef.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceTable, sinkTable, replaceSymbol, description);
    }
}
