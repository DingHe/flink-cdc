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

import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.runtime.operators.transform.converter.PostTransformConverter;

import javax.annotation.Nullable;

import java.util.Optional;

/** Post-Transformation rule used by {@link PostTransformOperator}. */
// PostTransformer 的主要作用是封装单条转换规则的完整上下文。
// 在 Flink CDC 的 YAML 配置中，用户会定义多条 transform 规则。
// 每条规则可能包含“作用于哪些表”、“保留哪些列”、“过滤掉哪些行”以及“自定义转换逻辑”。PostTransformer 将这些松散的配置项打包成一个对象，方便 PostTransformOperator 在运行时快速匹配并执行。
public class PostTransformer {
    // 匹配器：定义了该转换规则对哪些库、哪些表生效。
    // 它包含了 includeTables 和 excludeTables 的逻辑，算子通过它判断是否要对当前收到的 TableId 应用此规则。
    private final Selectors selectors;
    // 投影定义：封装了 SELECT 部分的逻辑。
    // 例如包含了哪些原始列、哪些计算列（如 column_a + 1）以及列的别名定义。
    private final @Nullable TransformProjection projection;
    // 过滤器定义：封装了 WHERE 部分的逻辑。包含了用于行级过滤的表达式（如 age > 18），只有满足该表达式的数据行才会被下发。
    private final @Nullable TransformFilter filter;
    // 后置转换插件：这是一个扩展接口。
    // 允许在标准的 SQL 转换逻辑之后，执行一些特殊的物理格式转换或自定义的逻辑处理。
    private final @Nullable PostTransformConverter postTransformConverter;
    // 元数据列支持：定义了当前规则可以访问的元数据字段（如 __source_ts, __db_name 等）。
    // 这使得用户可以在 projection 或 filter 中使用这些元数据。
    private final SupportedMetadataColumn[] supportedMetadataColumns;

    public PostTransformer(
            Selectors selectors,
            @Nullable TransformProjection projection,
            @Nullable TransformFilter filter,
            @Nullable PostTransformConverter postTransformConverter,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        this.selectors = selectors;
        this.projection = projection;
        this.filter = filter;
        this.postTransformConverter = postTransformConverter;
        this.supportedMetadataColumns = supportedMetadataColumns;
    }

    public Selectors getSelectors() {
        return selectors;
    }

    public Optional<TransformProjection> getProjection() {
        return Optional.ofNullable(projection);
    }

    public Optional<TransformFilter> getFilter() {
        return Optional.ofNullable(filter);
    }

    public Optional<PostTransformConverter> getPostTransformConverter() {
        return Optional.ofNullable(postTransformConverter);
    }

    public SupportedMetadataColumn[] getSupportedMetadataColumns() {
        return supportedMetadataColumns;
    }
}
