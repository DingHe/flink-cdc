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

package org.apache.flink.cdc.common.schema;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.utils.Predicates;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/** Selectors for filtering tables. */
// Selectors 类是 Flink CDC 中用于**表过滤（Table Filtering）**的核心组件。
// 它主要负责解析用户定义的表名匹配规则，并判断给定的 TableId（由命名空间、模式名、表名组成）是否应该被包含在处理流程中。
// 在 CDC 任务配置中，用户通常不会同步整个数据库，而是通过通配符或特定名称来指定一部分表（例如 db_name.table_.*）。Selectors 类的作用如下：
// 规则解析：将用户输入的字符串（支持逗号分隔多条规则，支持点号分隔层级）解析为内部的匹配器。
// 多层级匹配：支持不同深度的标识符匹配。它可以处理只有表名（table）、模式名+表名（schema.table）或命名空间+模式名+表名（ns.schema.table）的情况。
// 过滤执行：在运行时，针对每一个 TableChangeEvent 或数据行，快速判断其所属的表是否命中用户定义的“包含规则（Inclusions）”。
public class Selectors {
    // 存储解析后的 Selector 实例列表。
    // 每一条逗号分隔的规则都会对应一个 Selector 对象。
    private List<Selector> selectors;

    private Selectors() {}

    /**
     * A {@link Selector} that determines whether a table identified by a given {@link TableId} is
     * to be included.
     */
    private static class Selector {
        // 这三个属性是 Java 8 的谓词函数。
        private final Predicate<String> namespacePred;
        private final Predicate<String> schemaNamePred;
        private final Predicate<String> tableNamePred;

        public Selector(String namespace, String schemaName, String tableName) {
            this.namespacePred =
                    namespace == null ? (namespacePred) -> false : Predicates.includes(namespace);
            this.schemaNamePred =
                    schemaName == null
                            ? (schemaNamePred) -> false
                            : Predicates.includes(schemaName);
            this.tableNamePred =
                    tableName == null ? (tableNamePred) -> false : Predicates.includes(tableName);
        }

        public boolean isMatch(TableId tableId) {

            String namespace = tableId.getNamespace();
            String schemaName = tableId.getSchemaName();

            if (namespace == null || namespace.isEmpty()) {
                if (schemaName == null || schemaName.isEmpty()) {
                    return tableNamePred.test(tableId.getTableName());
                }
                return schemaNamePred.test(tableId.getSchemaName())
                        && tableNamePred.test(tableId.getTableName());
            }
            return namespacePred.test(tableId.getNamespace())
                    && schemaNamePred.test(tableId.getSchemaName())
                    && tableNamePred.test(tableId.getTableName());
        }
    }

    /** Match the {@link TableId} against the {@link Selector}s. * */
    // 判断给定的 TableId 是否匹配。
    public boolean isMatch(TableId tableId) {
        for (Selector selector : selectors) {
            if (selector.isMatch(tableId)) {
                return true;
            }
        }
        return false;
    }

    /** Builder for {@link Selectors}. */
    public static class SelectorsBuilder {

        private List<Selector> selectors;

        public SelectorsBuilder includeTables(String tableInclusions) {

            if (tableInclusions == null || tableInclusions.isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid table inclusion pattern cannot be null or empty");
            }

            List<Selector> selectors = new ArrayList<>();
            Set<String> tableSplitSet =
                    Predicates.setOf(
                            tableInclusions, Predicates.RegExSplitterByComma::split, (str) -> str);
            for (String tableSplit : tableSplitSet) {
                List<String> tableIdList =
                        Predicates.listOf(
                                tableSplit, Predicates.RegExSplitterByDot::split, (str) -> str);
                Iterator<String> iterator = tableIdList.iterator();
                if (tableIdList.size() == 1) {
                    selectors.add(new Selector(null, null, iterator.next()));
                } else if (tableIdList.size() == 2) {
                    selectors.add(new Selector(null, iterator.next(), iterator.next()));
                } else if (tableIdList.size() == 3) {
                    selectors.add(new Selector(iterator.next(), iterator.next(), iterator.next()));
                } else {
                    throw new IllegalArgumentException(
                            "Invalid table inclusion pattern: " + tableInclusions);
                }
            }
            this.selectors = selectors;
            return this;
        }

        public Selectors build() {
            Selectors selectors = new Selectors();
            selectors.selectors = this.selectors;
            return selectors;
        }
    }
}
