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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * a Pojo class to describe the information of the primaryKeys/partitionKeys/options transformation
 * of {@link Schema}.
 */
// Flink CDC 转换算子中的一个 POJO（简单 Java 对象）类。它专门用于处理和解析用户在转换规则（Transform Rule）中定义的关于“表元数据”的修改信息。
// 在 Flink CDC Pipeline 的配置中，用户不仅可以过滤数据，还可以重新定义目标表的结构属性。
// 例如，用户可能希望在同步过程中修改下游表的主键、指定分区键或添加特定的表参数。
// 解析配置字符串：将用户在 YAML 或配置中填写的、由逗号分隔的字符串（如 "id,name"）解析成程序可处理的集合对象（如 List）。
// 元数据重定向：它作为 Schema 转换过程中的中间载体。当 SchemaChangeEvent（结构变更事件）流过转换算子时，算子会读取该类中的信息，并覆盖掉原始 Schema 中的主键、分区键和选项。
public class SchemaMetadataTransform implements Serializable {

    private static final long serialVersionUID = 1L;
    // 存储重新定义后的主键列名列表。
    // CDC 依赖主键来处理数据的更新（UPDATE）和删除（DELETE）。如果用户在转换中定义了新主键，后续的 Sink 将根据此列表来保证数据幂等性。
    private List<String> primaryKeys = new ArrayList<>();
    // 存储重新定义后的分区键列名列表。
    // 主要用于下游的分区存储（如 HDFS、Paimon 或 Iceberg）。它决定了数据在写入目标端时如何进行物理拆分。
    private List<String> partitionKeys = new ArrayList<>();
    // 存储表级别的额外配置选项。
    // 允许用户动态地为特定的表注入参数，例如设置下游连接器的并发度、缓冲区大小等自定义属性。
    private Map<String, String> options = new HashMap<>();

    public SchemaMetadataTransform(
            String primaryKeyString, String partitionKeyString, String tableOptionString) {
        if (!StringUtils.isNullOrWhitespaceOnly(primaryKeyString)) {
            String[] primaryKeyArr = primaryKeyString.split(",");
            for (int i = 0; i < primaryKeyArr.length; i++) {
                primaryKeyArr[i] = primaryKeyArr[i].trim();
            }
            primaryKeys = Arrays.asList(primaryKeyArr);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(partitionKeyString)) {
            String[] partitionKeyArr = partitionKeyString.split(",");
            for (int i = 0; i < partitionKeyArr.length; i++) {
                partitionKeyArr[i] = partitionKeyArr[i].trim();
            }
            partitionKeys = Arrays.asList(partitionKeyArr);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(tableOptionString)) {
            for (String tableOption : tableOptionString.split(",")) {
                String[] kv = tableOption.split("=");
                if (kv.length != 2) {
                    throw new IllegalArgumentException(
                            "table option format error: "
                                    + tableOptionString
                                    + ", it should be like `key1=value1,key2=value2`.");
                }
                options.put(kv[0].trim(), kv[1].trim());
            }
        }
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public Map<String, String> getOptions() {
        return options;
    }
}
