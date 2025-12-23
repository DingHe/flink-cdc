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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.transform.converter.PostTransformConverters;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/**
 * A data process function that performs column filtering, calculated column evaluation & final
 * projection.
 */
// 在 Flink CDC 的转换架构中，如果说 PreTransformOperator 是数据初加工，那么 PostTransformOperator 就是最终精加工阶段。
// 它位于转换链的后端，主要负责在数据流向 Sink 之前执行最后的过滤、复杂的 UDF 计算以及最终的投影。
// 最终投影（Final Projection）：根据用户定义的规则，决定哪些列最终输出到下游（Sink）。
// 数据过滤（Row-level Filtering）：执行 WHERE 子句逻辑，过滤掉不符合条件的行。
// UDF 执行（User Defined Functions）：调用用户自定义函数处理复杂的字段计算。
// 动态 Schema 管理：在 DDL（表结构变更）发生时，实时重新计算并生成下游所需的最终 Schema。
public class PostTransformOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;
    // 转换过程中处理时间/日期类型时使用的时区。
    private final String timezone;
    // 序列化后的转换规则列表，
    // 包含用户在 YAML 配置文件中定义的投影和过滤表达式。
    private final List<TransformRule> transformRules;
    // 记录某张表是否使用了通配符 *。
    // 如果是，则源表的 DDL 变更通常会自动同步到下游。
    private final Map<TableId, Boolean> hasAsteriskMap;
    // 记录每张表最终投影后的列名列表，用于 DDL 变更时的增量过滤。
    private final Map<TableId, List<String>> projectedColumnsMap;
    // 核心状态映射。
    // 缓存了每张表转换前后的 Schema 对比信息，是数据转换的基准字典。
    private final Map<TableId, PostTransformChangeInfo> postTransformInfoMap;

    // Tuple3 items are: function name, class path, and extra options.
    // UDF 的元数据，包含函数名、类路径以及参数配置。
    private final List<Tuple3<String, String, Map<String, String>>> udfFunctions;
    // 算子启动时根据规则生成的实时转换执行器。
    private transient List<PostTransformer> transformers;
    // UDF 的描述符集合。
    private transient List<UserDefinedFunctionDescriptor> udfDescriptors;
    // 反射生成的 UDF 实例对象，用于运行时调用。
    private transient List<Object> udfFunctionInstances;

    // Querying a TransformProjectionProcessor with an upstream TableId and effective
    // post-transformer.
    // Guava Table 缓存，存储 (表ID, 转换器) 对应的 投影处理器。
    private transient Table<TableId, PostTransformer, TransformProjectionProcessor>
            projectionProcessors;
    // Guava Table 缓存，存储 (表ID, 转换器) 对应的 过滤处理器。
    private transient Table<TableId, PostTransformer, TransformFilterProcessor> filterProcessors;

    public static PostTransformOperatorBuilder newBuilder() {
        return new PostTransformOperatorBuilder();
    }

    PostTransformOperator(
            List<TransformRule> transformRules,
            String timezone,
            List<Tuple3<String, String, Map<String, String>>> udfFunctions) {
        this.timezone = timezone;
        this.transformRules = transformRules;
        this.hasAsteriskMap = new HashMap<>();
        this.projectedColumnsMap = new HashMap<>();
        this.postTransformInfoMap = new ConcurrentHashMap<>();
        this.udfFunctions = udfFunctions;
    }

    @Override
    public void open() throws Exception {
        super.open();

        // Initialize multi-key lookup tables
        this.projectionProcessors = HashBasedTable.create();
        this.filterProcessors = HashBasedTable.create();

        // Be sure to initialize UDF related fields before creating transformers
        initializeUdf();

        this.transformers = createTransformers();
    }

    @Override
    public void close() throws Exception {
        super.close();
        TransformExpressionCompiler.cleanUp();
        destroyUdf();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        try {
            processElementInternal(element);
        } catch (Exception e) {
            Event event = element.getValue();
            TableId tableId = null;
            Schema schemaBefore = null;
            Schema schemaAfter = null;

            if (event instanceof ChangeEvent) {
                tableId = ((ChangeEvent) event).tableId();
                PostTransformChangeInfo info = postTransformInfoMap.get(tableId);
                if (info != null) {
                    schemaBefore = info.getPreTransformedSchema();
                    schemaAfter = info.getPostTransformedSchema();
                }
            }

            throw new TransformException(
                    "post-transform", event, tableId, schemaBefore, schemaAfter, e);
        }
    }
    // PostTransformOperator 的数据处理核心枢纽。
    // 如果把 Flink CDC 比作一条高速公路，那么这个方法就是收费站的分类闸口：它负责接收所有的事件（Event），判断它们来自哪张表，匹配对应的转换逻辑，并分发给专门的处理器。
    private void processElementInternal(StreamRecord<Event> element) {
        // 从 Flink 的 StreamRecord 封装中取出真实的 Event 对象
        Event event = element.getValue();
        if (event == null) {
            return;
        }

        // Reject processing non-schema or data change events.
        // 仅处理变更事件
        if (!(event instanceof ChangeEvent)) {
            throw new UnsupportedOperationException("Unexpected stream record event: " + event);
        }

        ChangeEvent changeEvent = (ChangeEvent) event;
        TableId tableId = changeEvent.tableId();
        // 获取匹配的转换器
        List<PostTransformer> transformers = getEffectiveTransformers(tableId);

        // Short-circuit if there's no effective transformers.
        // 如果没有规则匹配当前表，则原样发送。
        if (transformers.isEmpty()) {
            output.collect(element);
            return;
        }
        // 处理建表事件
        if (event instanceof CreateTableEvent) {
            // 根据转换规则（如新增了计算列）重新构建目标表的 Schema。
            processCreateTableEvent((CreateTableEvent) event, transformers)
                    .map(StreamRecord::new)
                    .ifPresent(output::collect);
            // 因为 Schema 变了，之前缓存的 PostTransformChangeInfo（字段索引映射等）必须作废，下次处理数据时重新生成。
            invalidateCache(tableId);
        // 处理 Schema 变更事件
        } else if (event instanceof SchemaChangeEvent) {
            processSchemaChangeEvent((SchemaChangeEvent) event, transformers)
                    .map(StreamRecord::new)
                    .ifPresent(output::collect);
            invalidateCache(tableId);
        // 处理数据行变更事件
        } else if (event instanceof DataChangeEvent) {
            processDataChangeEvent((DataChangeEvent) event, transformers)
                    .map(StreamRecord::new)
                    .ifPresent(output::collect);
        } else {
            throw new UnsupportedOperationException("Unexpected stream record event: " + event);
        }
    }

    // -------------------
    // Key methods for processing upstream events.
    // -------------------

    /**
     * Apply effective transform rules to {@link CreateTableEvent}s based on effective transformers.
     */
    // PostTransformOperator 中处理 DDL（建表）事件 的核心方法。
    // 它的主要任务是：根据转换规则（Transform Rules）修改原始的表结构（Schema），并将这种“转换关系”记录在内存中，以便后续处理数据行时使用。
    private Optional<Event> processCreateTableEvent(
            CreateTableEvent event, List<PostTransformer> effectiveTransformers) {
        // 获取当前事件对应的表 ID（库名.表名）以及该表在转换前的原始结构（preSchema）。
        TableId tableId = event.tableId();
        Schema preSchema = event.getSchema();

        // Apply transform rules and verify we can get a deterministic post schema
        // 应用规则并生成中间 Schema 列表
        List<Schema> schemas =
                effectiveTransformers.stream()
                        .map(trans -> transformSchema(preSchema, trans))
                        .collect(Collectors.toList());
        // 合并 Schema 并强化主键约束
        // 将多个规则产生的结构合并成一个最终确定的结构（postSchema）
        Schema postSchema =
                SchemaUtils.ensurePkNonNull(SchemaMergingUtils.strictlyMergeSchemas(schemas));

        // Update transform info map
        // 更新转换信息映射表（元数据缓存）
        // 将“原始结构”与“转换后结构”的对照关系存储在 postTransformInfoMap 中
        postTransformInfoMap.put(
                tableId, PostTransformChangeInfo.of(tableId, preSchema, postSchema));

        // Update "if-table-has-been–wildcard–matched" map
        // 判断用户的投影配置中是否使用了星号（如 SELECT *）
        boolean wildcardMatched =
                effectiveTransformers.stream()
                        .map(PostTransformer::getProjection)
                        .flatMap(this::optionalToStream)
                        .map(TransformProjection::getProjection)
                        .anyMatch(TransformParser::hasAsterisk);
        hasAsteriskMap.put(tableId, wildcardMatched);
        // 维护“被投影列”的映射
        projectedColumnsMap.put(
                tableId,
                preSchema.getColumnNames().stream()
                        .filter(postSchema.getColumnNames()::contains)
                        .collect(Collectors.toList()));

        return Optional.of(new CreateTableEvent(tableId, postSchema));
    }

    /**
     * Apply effective transform rules to other {@link SchemaChangeEvent}s based on effective
     * transformers and existing {@link PostTransformChangeInfo}.
     */
    // PostTransformOperator 处理 DDL（结构变更） 事件的核心逻辑。
    // 它的主要职责是：当原始表结构发生变化（如加减列、修改字段）时，如何根据用户定义的转换规则，计算出**转换后（目标端）**对应的结构变更事件。
    private Optional<Event> processSchemaChangeEvent(
            SchemaChangeEvent event, List<PostTransformer> effectiveTransformers) {
        // 获取发生变更的表 ID，并从缓存中取出该表变更前的转换信息（包含转换前后的旧 Schema 对照）。
        TableId tableId = event.tableId();
        PostTransformChangeInfo info = checkNotNull(postTransformInfoMap.get(tableId));

        // Apply schema change event to the pre-transformed schema
        Schema prevPreSchema = info.getPreTransformedSchema();
        // 将收到的 DDL 事件（event）作用于旧的原始 Schema（prevPreSchema），生成最新的原始 Schema（nextPreSchema）。
        Schema nextPreSchema = SchemaUtils.applySchemaChangeEvent(prevPreSchema, event);

        // Apply transform rules and verify we can get a deterministic post schema
        // 应用转换规则并计算“转换后”的新结构
        List<Schema> schemas =
                effectiveTransformers.stream()
                        .map(trans -> transformSchema(nextPreSchema, trans))
                        .collect(Collectors.toList());

        Schema nextPostSchema =
                SchemaUtils.ensurePkNonNull(SchemaMergingUtils.strictlyMergeSchemas(schemas));

        // Update transform info map
        // 更新内存缓存（状态同步）
        postTransformInfoMap.put(
                tableId, PostTransformChangeInfo.of(tableId, nextPreSchema, nextPostSchema));

        // Prepare transformed schema change events
        // 准备下发的 Schema 变更事件
        Schema prevPostSchema = info.getPostTransformedSchema();
        List<String> columnNamesBeforeChange = prevPostSchema.getColumnNames();
        // 如果用户写了 SELECT *，这意味着原始表的新增列通常需要透传到下游。
        if (hasAsteriskMap.getOrDefault(tableId, true)) {
            // See comments in PreTransformOperator#cacheChangeSchema method.
            return SchemaUtils.transformSchemaChangeEvent(true, columnNamesBeforeChange, event)
                    .map(Event.class::cast);
        } else {
            return SchemaUtils.transformSchemaChangeEvent(
                            false, projectedColumnsMap.get(tableId), event)
                    .map(Event.class::cast);
        }
    }

    /** Apply projection rules to given {@link DataChangeEvent}. */
    private Optional<Event> processDataChangeEvent(
            DataChangeEvent event, List<PostTransformer> effectiveTransformers) {
        TableId tableId = event.tableId();
        PostTransformChangeInfo info = checkNotNull(postTransformInfoMap.get(tableId));

        // Prepare transform context
        TransformContext context = new TransformContext();
        context.epochTime = System.currentTimeMillis();
        context.meta = event.meta();

        String beforeOp = event.opTypeString(false);
        String afterOp = event.opTypeString(true);

        for (PostTransformer transformer : effectiveTransformers) {
            TransformProjectionProcessor projectionProcessor =
                    getProjectionProcessor(tableId, transformer);
            TransformFilterProcessor filterProcessor = getFilterProcessor(tableId, transformer);

            RecordData beforeRow = null;
            RecordData afterRow = null;
            boolean filterPassed = true;

            if (event.before() != null) {
                context.opType = beforeOp;
                Tuple2<BinaryRecordData, Boolean> result =
                        transformRecord(
                                event.before(),
                                info,
                                projectionProcessor,
                                filterProcessor,
                                context);
                beforeRow = result.f0;
                filterPassed = result.f1;
            }

            if (event.after() != null) {
                context.opType = afterOp;
                Tuple2<BinaryRecordData, Boolean> result =
                        transformRecord(
                                event.after(), info, projectionProcessor, filterProcessor, context);
                afterRow = result.f0;
                filterPassed = result.f1;
            }

            if (filterPassed) {
                DataChangeEvent finalEvent =
                        DataChangeEvent.projectRecords(event, beforeRow, afterRow);
                if (transformer.getPostTransformConverter().isPresent()) {
                    return transformer
                            .getPostTransformConverter()
                            .get()
                            .convert(finalEvent)
                            .map(Event.class::cast);
                } else {
                    return Optional.of(finalEvent);
                }
            }
        }

        // Return original event if no transform predicate is satisfied/.
        return Optional.empty();
    }

    /**
     * Generates transformed version of schema based on upstream schema and effective transformer.
     */
    private Schema transformSchema(Schema preSchema, PostTransformer transformer) {
        List<ProjectionColumn> projectionColumns =
                TransformParser.generateProjectionColumns(
                        transformer
                                .getProjection()
                                .map(TransformProjection::getProjection)
                                .orElse(null),
                        preSchema.getColumns(),
                        udfDescriptors,
                        transformer.getSupportedMetadataColumns());
        return preSchema.copy(
                projectionColumns.stream()
                        .map(ProjectionColumn::getColumn)
                        .collect(Collectors.toList()));
    }

    /** Projects given {@link RecordData} based on given processor. */
    private Tuple2<BinaryRecordData, Boolean> transformRecord(
            RecordData recordData,
            PostTransformChangeInfo info,
            @Nullable TransformProjectionProcessor projectionProcessor,
            @Nullable TransformFilterProcessor filterProcessor,
            TransformContext context) {
        RecordData.FieldGetter[] preFieldGetters = info.getPreTransformedFieldGetters();
        Schema preSchema = info.getPreTransformedSchema();
        Schema postSchema = info.getPostTransformedSchema();
        BinaryRecordDataGenerator postGenerator = info.getPostTransformedRecordDataGenerator();

        Object[] preRow = new Object[preFieldGetters.length];
        for (int i = 0; i < preFieldGetters.length; i++) {
            preRow[i] =
                    DataTypeConverter.convertToOriginal(
                            preFieldGetters[i].getFieldOrNull(recordData),
                            preSchema.getColumnDataTypes().get(i));
        }

        Object[] postRow =
                projectionProcessor != null ? projectionProcessor.project(preRow, context) : preRow;

        // Filter predicate test might refer to both PreTransformed only columns (that have been
        // eliminated from transform result) and PostTransformed only columns (that do not exist
        // until expression evaluation finishes). So we need pass both rows to FilterProcessor.
        boolean filterPassed =
                filterProcessor == null || filterProcessor.test(preRow, postRow, context);

        Object[] postRowBinary = new Object[postSchema.getColumnCount()];
        for (int i = 0; i < postRow.length; i++) {
            postRowBinary[i] =
                    DataTypeConverter.convert(postRow[i], postSchema.getColumnDataTypes().get(i));
        }
        return Tuple2.of(postGenerator.generate(postRowBinary), filterPassed);
    }

    // -------------------
    // Convenience methods for coping with transient fields.
    // -------------------

    /** Obtain effective transformers based on given {@link TableId}. */
    // 返回该表有效的转换规则
    private List<PostTransformer> getEffectiveTransformers(TableId tableId) {
        return transformers.stream()
                .filter(trans -> trans.getSelectors().isMatch(tableId))
                .collect(Collectors.toList());
    }

    /**
     * Get the unique {@link TransformProjectionProcessor} based on provided {@link TableId} and
     * {@link PostTransformer}.
     */
    private TransformProjectionProcessor getProjectionProcessor(
            TableId tableId, PostTransformer postTransformer) {
        if (!projectionProcessors.contains(tableId, postTransformer)) {
            PostTransformChangeInfo changeInfo = postTransformInfoMap.get(tableId);
            projectionProcessors.put(
                    tableId,
                    postTransformer,
                    new TransformProjectionProcessor(
                            changeInfo,
                            postTransformer
                                    .getProjection()
                                    .map(TransformProjection::getProjection)
                                    .orElse(null),
                            timezone,
                            udfDescriptors,
                            udfFunctionInstances,
                            postTransformer.getSupportedMetadataColumns()));
        }
        return projectionProcessors.get(tableId, postTransformer);
    }

    /**
     * Get the unique {@link TransformFilterProcessor} based on provided {@link TableId} and {@link
     * PostTransformer}.
     */
    private TransformFilterProcessor getFilterProcessor(
            TableId tableId, PostTransformer postTransformer) {
        if (!filterProcessors.contains(tableId, postTransformer)) {
            if (!postTransformer.getFilter().isPresent()) {
                filterProcessors.put(tableId, postTransformer, TransformFilterProcessor.ofNoOp());
            } else {
                PostTransformChangeInfo changeInfo = postTransformInfoMap.get(tableId);
                filterProcessors.put(
                        tableId,
                        postTransformer,
                        TransformFilterProcessor.of(
                                changeInfo,
                                postTransformer.getFilter().orElse(null),
                                timezone,
                                udfDescriptors,
                                udfFunctionInstances,
                                postTransformer.getSupportedMetadataColumns()));
            }
        }
        return filterProcessors.get(tableId, postTransformer);
    }

    /**
     * Flush caches saved for given {@link TableId}. Be sure to invalidate caches after its schema
     * has been changed!
     */
    private void invalidateCache(TableId tableId) {
        projectionProcessors.row(tableId).clear();
        filterProcessors.row(tableId).clear();
    }
    // 将用户定义的原始转换规则（TransformRule）翻译成可执行的物理逻辑单元（PostTransformer）
    private List<PostTransformer> createTransformers() {
        List<PostTransformer> list = new ArrayList<>();
        for (TransformRule rule : transformRules) {
            String projection = rule.getProjection();
            String filterExpression = rule.getFilter();
            String tableInclusions = rule.getTableInclusions();
            Selectors selectors =
                    new Selectors.SelectorsBuilder().includeTables(tableInclusions).build();
            PostTransformer apply =
                    new PostTransformer(
                            selectors,
                            TransformProjection.of(projection).orElse(null),
                            TransformFilter.of(filterExpression, udfDescriptors).orElse(null),
                            PostTransformConverters.of(rule.getPostTransformConverter())
                                    .orElse(null),
                            rule.getSupportedMetadataColumns());
            list.add(apply);
        }
        return list;
    }
    // 在启动阶段（通常在 open 方法中调用）执行的关键初始化逻辑。它的核心职责是：将用户定义的 UDF 类实例化，并完成必要的生命周期初始化（如调用 open 方法）。
    private void initializeUdf() {
        // 将原始的 UDF 配置元组转换成结构化的描述符对象。
        this.udfDescriptors =
                udfFunctions.stream()
                        .map(UserDefinedFunctionDescriptor::new)
                        .collect(Collectors.toList());
        // 创建一个列表，用于存放所有已经实例化后的 UDF 对象。
        this.udfFunctionInstances = new ArrayList<>();

        for (UserDefinedFunctionDescriptor udf : udfDescriptors) {
            try {
                // 通过 Java 反射机制创建 UDF 对象的实例。
                Class<?> clazz = Class.forName(udf.getClasspath());
                Object udfInstance = clazz.getDeclaredConstructor().newInstance();
                udfFunctionInstances.add(udfInstance);
                // 检查该 UDF 是否实现了 Flink CDC 定义的标准接口（而非普通的 Flink UDF）。
                if (udf.isCdcPipelineUdf()) {
                    // We use reflection to invoke UDF methods since we may add more methods
                    // into UserDefinedFunction interface, thus the provided UDF classes
                    // might not be compatible with the interface definition in CDC common.
                    UserDefinedFunctionContext userDefinedFunctionContext =
                            () -> Configuration.fromMap(udf.getParameters());
                    udfInstance
                            .getClass()
                             // 找到该 UDF 实例的 open 方法，并将构造好的 context 传入。这允许 UDF 在正式处理数据前执行初始化逻辑（如建立连接）。
                            .getMethod("open", UserDefinedFunctionContext.class)// 反射调用 open 方法
                            .invoke(udfInstance, userDefinedFunctionContext);
                }
                // Do nothing for Flink-style UDF since their lifecycle hooks are not supported
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to instantiate UDF function " + udf, e);
            }
        }
    }

    private void destroyUdf() {
        if (udfDescriptors == null || udfFunctionInstances == null) {
            return;
        }
        for (int i = 0; i < udfDescriptors.size(); i++) {
            UserDefinedFunctionDescriptor udf = udfDescriptors.get(i);
            try {
                if (udf.isCdcPipelineUdf()) {
                    Object udfInstance = udfFunctionInstances.get(i);
                    udfInstance.getClass().getMethod("close").invoke(udfInstance);
                }
                // Do nothing for Flink-style UDF since their lifecycle hooks are not supported
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to destroy UDF " + udf, e);
            }
        }
        udfDescriptors.clear();
        udfFunctionInstances.clear();
    }

    /** Backport of {@code Optional#stream} before Java 11. */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private <T> Stream<T> optionalToStream(Optional<T> optional) {
        return optional.map(Stream::of).orElseGet(Stream::empty);
    }
}
