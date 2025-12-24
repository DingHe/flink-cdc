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

package org.apache.flink.cdc.runtime.operators.schema.common;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Selectors;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Calculates how upstream data change events should be dispatched to downstream tables. Returns one
 * or many destination Table IDs based on provided routing rules.
 */
// TableIdRouter（表 ID 路由处理器）扮演着“交通指挥官”的角色。
// 它决定了上游采集到的数据事件（Data Events）和结构变更事件（Schema Events）最终应该流向哪一个或哪几个目标表。
// TableIdRouter 的核心作用是实现 “逻辑表到物理表”的映射。它的具体职责包括：
// 映射计算：根据用户定义的路由规则（Route Rules），将原始的 sourceTableId 转换为目标端的 sinkTableId。
// 分表合并支持：支持将多个满足正则匹配的源表（如 order_01, order_02）合并路由到同一个目标表（如 order_all）。
// 动态表名替换：支持通过占位符（replaceSymbol）动态生成目标表名。
// 性能优化：通过 Guava Cache 缓存路由结果，避免对每一条 CDC 数据都进行昂贵的正则表达式匹配计算。
// 逻辑分组：为 Schema 演进提供支持，将属于同一条路由规则的源表归为一组，以便后续进行“加宽表”合并。
public class TableIdRouter {
    // 存储解析后的路由规则。
    // f0 (Selectors)：基于正则的过滤器，用于判断源表 ID 是否匹配此规则。
    // f1 (String)：目标表的标识符字符串。
    // f2 (String)：替换占位符（replaceSymbol），用于动态替换逻辑。
    private final List<Tuple3<Selectors, String, String>> routes;
    // 路由结果缓存。
    // Key 是源表 ID，Value 是对应的目标表 ID 列表。
    private final LoadingCache<TableId, List<TableId>> routingCache;
    // 静态常量，设置为 1 天。
    // 表示路由结果在缓存中 24 小时未访问后失效，防止内存无限膨胀。
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    public TableIdRouter(List<RouteRule> routingRules) {
        this.routes = new ArrayList<>();
        // 遍历用户传入的 RouteRule 列表。
        for (RouteRule rule : routingRules) {
            try {
                String tableInclusions = rule.sourceTable;
                // 将规则中的 sourceTable 正则表达式编译为 Selectors。
                Selectors selectors =
                        new Selectors.SelectorsBuilder().includeTables(tableInclusions).build();
                routes.add(new Tuple3<>(selectors, rule.sinkTable, rule.replaceSymbol));
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Failed to parse regular expression in routing rule %s. Notice that `.` is used to separate Table ID components. To use it as a regex token, put a `\\` before to escape it.",
                                rule),
                        e);
            }
        }
        this.routingCache =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, List<TableId>>() {
                                    @Override
                                    public @Nonnull List<TableId> load(@Nonnull TableId key) {
                                        return calculateRoute(key);
                                    }
                                });
    }
    // 直接从缓存中获取 sourceTableId 对应的目标 ID 列表。如果缓存中没有，会自动触发加载逻辑。
    public List<TableId> route(TableId sourceTableId) {
        return routingCache.getUnchecked(sourceTableId);
    }

    // 实际执行路由算法的内部私有方法
    private List<TableId> calculateRoute(TableId sourceTableId) {
        List<TableId> routedTableIds =
                routes.stream()
                        .filter(route -> route.f0.isMatch(sourceTableId))
                        .map(route -> resolveReplacement(sourceTableId, route))
                        .collect(Collectors.toList());
        if (routedTableIds.isEmpty()) {
            // 如果没有任何规则匹配，默认将数据发送到与源表名同名的目标表（即 routedTableIds.add(sourceTableId)）
            routedTableIds.add(sourceTableId);
        }
        return routedTableIds;
    }
    // 处理目标表名的占位符替换。
    private TableId resolveReplacement(
            TableId originalTable, Tuple3<Selectors, String, String> route) {
        if (route.f2 != null) {
            return TableId.parse(route.f1.replace(route.f2, originalTable.getTableName()));
        }
        return TableId.parse(route.f1);
    }

    /**
     * Group the source tables that conform to the same routing rule together. The total number of
     * groups is less than or equal to the number of routing rules. For the source tables within
     * each group, their table structures will be merged to obtain the widest table structure in
     * that group. The structures of all tables within the group will be expanded to this widest
     * table structure.
     *
     * @param tableIdSet The tables need to be grouped by the router
     * @return The tables grouped by the router
     */
    // 将源表集合按规则进行分组。
    public List<Set<TableId>> groupSourceTablesByRouteRule(Set<TableId> tableIdSet) {
        if (routes.isEmpty()) {
            return new ArrayList<>();
        }
        List<Set<TableId>> routedTableIds =
                routes.stream()
                        .map(
                                route -> {
                                    return tableIdSet.stream()
                                            .filter(
                                                    tableId -> {
                                                        return route.f0.isMatch(tableId);
                                                    })
                                            .collect(Collectors.toSet());
                                })
                        .collect(Collectors.toList());
        return routedTableIds;
    }
}
