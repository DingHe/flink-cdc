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

import org.apache.flink.cdc.common.utils.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The projection of transform applies to describe a projection of filtering tables. Projection
 * includes the original columns of the data table and the user-defined computed columns.
 *
 * <p>A transformation projection contains:
 *
 * <ul>
 *   <li>projection: a string for projecting the row of matched table as output.
 *   <li>projectionColumns: a list for recording all columns transformation of the projection.
 * </ul>
 */
// TransformProjection 是 Flink CDC 转换引擎中用于描述 “投影变换”（Projection） 的核心类。在数据处理领域，投影通常指从源数据中选择特定列、删除不需要的列，或者通过表达式计算生成新列的操作。
// 在 Flink CDC Pipeline 的 transform 配置中，用户可以通过 projection 参数定义输出结果的样式。该类的具体作用如下：
// 定义输出结构：它承载了用户定义的 SQL 式投影表达式（如 id, name, age + 1 AS next_age）。
// 存储列级转换信息：它不仅保存原始的投影字符串，还持有一个解析后的列转换列表（ProjectionColumn），这决定了最终输出的 Schema 包含哪些字段。
// 转换逻辑的载体：它是转换算子识别“如何裁剪列”和“如何新增计算列”的依据。
public class TransformProjection implements Serializable {
    private static final long serialVersionUID = 1L;
    // 存储原始的投影字符串。
    private final String projection;
    // 存储经过解析后的详细列转换对象列表。
    // 每个 ProjectionColumn 包含了该列是原始列还是计算列、表达式是什么、目标类型是什么等关键元数据。
    // 这是构建目标表 Schema 的核心依据。
    private final List<ProjectionColumn> projectionColumns;

    public TransformProjection(String projection, List<ProjectionColumn> projectionColumns) {
        this.projection = projection;
        this.projectionColumns = projectionColumns;
    }

    public String getProjection() {
        return projection;
    }

    public List<ProjectionColumn> getProjectionColumns() {
        return projectionColumns;
    }

    public boolean isValid() {
        return !StringUtils.isNullOrWhitespaceOnly(projection);
    }

    public static Optional<TransformProjection> of(String projection) {
        if (StringUtils.isNullOrWhitespaceOnly(projection)) {
            return Optional.empty();
        }
        return Optional.of(new TransformProjection(projection, new ArrayList<>()));
    }

    @Override
    public String toString() {
        return "TransformProjection{"
                + "projection='"
                + getProjection()
                + '\''
                + ", projectionColumns="
                + getProjectionColumns()
                + '}';
    }
}
