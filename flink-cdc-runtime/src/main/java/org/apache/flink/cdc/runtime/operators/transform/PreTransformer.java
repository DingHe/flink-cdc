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

import javax.annotation.Nullable;

import java.util.Optional;

/** Pre-Transformation rule used by {@link PreTransformOperator}. */
// PreTransformer 的作用就是将Flink CDC 的 YAML 配置在代码层面实例化。
// 它是一个“规则束”，将“哪些表需要变（Selectors）”、“列怎么变（Projection）”以及“哪些行要留（Filter）”这三个逻辑封装在一起。
// 之所以称为 Pre（预），是因为这些操作通常发生在数据流进入核心 Schema 演化处理或写入 Sink 之前的阶段。
public class PreTransformer {
    private final Selectors selectors;

    private final Optional<TransformProjection> projection;
    private final Optional<TransformFilter> filter;

    public PreTransformer(
            Selectors selectors,
            @Nullable TransformProjection projection,
            @Nullable TransformFilter filter) {
        this.selectors = selectors;
        this.projection = projection != null ? Optional.of(projection) : Optional.empty();
        this.filter = filter != null ? Optional.of(filter) : Optional.empty();
    }

    public Selectors getSelectors() {
        return selectors;
    }

    public Optional<TransformProjection> getProjection() {
        return projection;
    }

    public Optional<TransformFilter> getFilter() {
        return filter;
    }
}
