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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A data process function that filters out columns which aren't (directly & indirectly) referenced.
 */
// PreTransformOperator 是 Flink CDC Pipeline 框架中一个非常关键的物理算子。
// 它位于数据源（Source）之后，主要的职责是对数据流进行“预剪枝”和“Schema（结构）管理”。
// 简单来说，如果你在配置中定义了只转换某几列，或者定义了过滤器，这个算子会识别出哪些原始列是“有用”的，然后把没用的列在进入复杂的计算逻辑前就丢弃掉，以节省内存和网络开销。
// 列剪枝（Column Pruning）：通过解析用户定义的 Projection（投影）和 Filter（过滤）表达式，计算出下游真正需要的原始列，剔除无关列。
// Schema 演进管理：维护表结构的状态。当上游发生 ALTER TABLE 时，它负责更新内部 Schema 状态并决定是否需要将变更下传。
// 元数据重写：根据用户定义，修改表的主键（Primary Keys）、分区键（Partition Keys）或表参数。
// 容错与恢复：通过 Flink 的状态机制（State）持久化 Schema 信息，确保作业重启后依然能正确解析增量数据。
// 延迟发射 CreateTableEvent：在分布式场景下，如果数据比表结构先到，它能确保下游先收到表创建事件。

public class PreTransformOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;

    /** All tables which have been sent {@link CreateTableEvent} to downstream. */
    // 记录已经发送到下游的 TableId。
    // 防止在作业重启或某些异常流转中重复发送 CreateTableEvent。
    private final Set<TableId> alreadySentCreateTableEvents;
    // 用户定义的转换规则列表（原始字符串形式）
    private final List<TransformRule> transformRules;
    // 核心状态映射表。
    // 存储 TableId 对应的 Schema 变化信息（包含原始 Schema 和预转换后的 Schema）
    private final Map<TableId, PreTransformChangeInfo> preTransformChangeInfoMap;
    // 存储元数据转换逻辑（如修改主键、分区键的逻辑）。
    private final List<Tuple2<Selectors, SchemaMetadataTransform>> schemaMetadataTransformers;
    // UDF 函数的配置信息
    private final List<Tuple3<String, String, Map<String, String>>> udfFunctions;
    // 标志位，决定是否将 Schema 存入 Flink State。在批处理或分布式 Schema 模式下可能关闭。
    private final boolean shouldStoreSchemasInState;
    // Flink 的 ListState，用于在 Checkpoint 中存储序列化后的 Schema 信息。
    private transient ListState<byte[]> state;
    // 运行时解析后的 PreTransformer 列表，包含编译后的过滤和投影逻辑。
    private transient List<PreTransformer> transforms;
    // UDF 的运行时描述符，用于实例化具体的函数。
    private transient List<UserDefinedFunctionDescriptor> udfDescriptors;
    // 运行时处理器
    // 封装了对每一行二进制数据（BinaryRecordData）进行具体剪枝操作的逻辑。
    private transient Map<TableId, PreTransformProcessor> preTransformProcessorMap;
    // 标记某张表是否使用了 SELECT *（通配符）。如果用了通配符，则不能进行列剪枝。
    private transient Map<TableId, Boolean> hasAsteriskMap;

    public static PreTransformOperatorBuilder newBuilder() {
        return new PreTransformOperatorBuilder();
    }

    PreTransformOperator(
            List<TransformRule> transformRules,
            List<Tuple3<String, String, Map<String, String>>> udfFunctions,
            boolean shouldStoreSchemasInState) {
        this.preTransformChangeInfoMap = new ConcurrentHashMap<>();
        this.alreadySentCreateTableEvents = new HashSet<>();
        this.preTransformProcessorMap = new ConcurrentHashMap<>();
        this.schemaMetadataTransformers = new ArrayList<>();
        this.chainingStrategy = ChainingStrategy.ALWAYS;

        this.transformRules = transformRules;
        this.udfFunctions = udfFunctions;
        this.shouldStoreSchemasInState = shouldStoreSchemasInState;
    }
    // setup 方法是 Flink 算子生命周期中的一个重要钩子（Hook）。它在算子初始化早期被调用，主要任务是将用户配置的原始字符串规则解析并转化为运行时可执行的逻辑对象。
    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        this.udfDescriptors =
                this.udfFunctions.stream()
                        .map(udf -> new UserDefinedFunctionDescriptor(udf.f0, udf.f1, udf.f2))
                        .collect(Collectors.toList());

        // Initialize data fields in advance because they might be accessed in
        // `::initializeState` function when restoring from a previous state.
        this.transforms = new ArrayList<>();
        for (TransformRule transformRule : transformRules) {
            String tableInclusions = transformRule.getTableInclusions();
            String projection = transformRule.getProjection();
            String filter = transformRule.getFilter();
            String primaryKeys = transformRule.getPrimaryKey();
            String partitionKeys = transformRule.getPartitionKey();
            String tableOptions = transformRule.getTableOption();
            Selectors selectors =
                    new Selectors.SelectorsBuilder().includeTables(tableInclusions).build();
            transforms.add(
                    new PreTransformer(
                            selectors,
                            TransformProjection.of(projection).orElse(null),
                            TransformFilter.of(filter, udfDescriptors).orElse(null)));
            schemaMetadataTransformers.add(
                    new Tuple2<>(
                            selectors,
                            new SchemaMetadataTransform(primaryKeys, partitionKeys, tableOptions)));
        }
        this.preTransformProcessorMap = new ConcurrentHashMap<>();
        this.hasAsteriskMap = new ConcurrentHashMap<>();
    }
    // 核心作用是在 Flink 作业启动或从检查点（Checkpoint/Savepoint）恢复时，初始化并恢复用于转换逻辑的 Schema（表结构）状态。
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        // 执行算子基类的初始化逻辑，确保 Flink 算子的基础状态环境准备就绪。
        super.initializeState(context);
        // 检查是否需要将 Schema 存储在状态中。
        // 如果是 Batch（批处理）模式或分布式 Schema 模式，算子不需要自行持久化 Schema，直接返回。
        if (!shouldStoreSchemasInState) {
            // Skip schema persistency if we're in the distributed schema mode or the batch
            // execution mode.
            return;
        }
        // 从上下文中获取 Flink 的 OperatorStateStore，用于访问算子级别的状态（非 Keyed State）
        OperatorStateStore stateStore = context.getOperatorStateStore();
        ListStateDescriptor<byte[]> descriptor =
                new ListStateDescriptor<>("originalSchemaState", byte[].class);
        state = stateStore.getUnionListState(descriptor);
        // 检查当前作业是否是从之前的 Checkpoint 或 Savepoint 恢复的。如果是，则执行恢复逻辑。
        if (context.isRestored()) {
            for (byte[] serializedTableInfo : state.get()) {
                PreTransformChangeInfo stateTableChangeInfo =
                        PreTransformChangeInfo.SERIALIZER.deserialize(
                                PreTransformChangeInfo.SERIALIZER.getVersion(),
                                serializedTableInfo);
                preTransformChangeInfoMap.put(
                        stateTableChangeInfo.getTableId(), stateTableChangeInfo);

                CreateTableEvent restoredCreateTableEvent =
                        new CreateTableEvent(
                                stateTableChangeInfo.getTableId(),
                                stateTableChangeInfo.getPreTransformedSchema());
                // hasAsteriskMap needs to be recalculated after restoring from a checkpoint.
                cacheTransformRuleInfo(restoredCreateTableEvent);
            }
        }
    }
    // 核心作用是在 Flink 进行 Checkpoint（检查点） 或 Savepoint（保存点） 时，将算子内存中缓存的表结构（Schema）信息持久化到状态后端（State Backend）中。
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        if (!shouldStoreSchemasInState) {
            // Same reason in this#initializeState.
            return;
        }
        state.update(
                new ArrayList<>(
                        preTransformChangeInfoMap.values().stream()
                                .map(
                                        tableChangeInfo -> {
                                            try {
                                                return PreTransformChangeInfo.SERIALIZER.serialize(
                                                        tableChangeInfo);
                                            } catch (IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        })
                                .collect(Collectors.toList())));
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        clearOperator();
    }

    @Override
    public void close() throws Exception {
        super.close();
        clearOperator();
        this.state = null;
    }
    // 负责接收数据流中的每一个事件（Event），执行转换逻辑，并提供了极其完善的异常捕获与诊断机制。
    // StreamRecord 是 Flink 数据的容器，Event 是 Flink CDC 定义的事件基类（包括数据变更、表结构变更等）。
    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();

        try {
            processEvent(event);
        } catch (Exception e) {
            TableId tableId = null;
            Schema schemaBefore = null;
            Schema schemaAfter = null;

            if (event instanceof ChangeEvent) {
                tableId = ((ChangeEvent) event).tableId();
                PreTransformChangeInfo info = preTransformChangeInfoMap.get(tableId);
                if (info != null) {
                    schemaBefore = info.getSourceSchema();
                    schemaAfter = info.getPreTransformedSchema();
                }
            }

            throw new TransformException(
                    "pre-transform", event, tableId, schemaBefore, schemaAfter, e);
        }
    }
    // 作用是根据流入事件的类型（建表、删表、结构变更、数据变更），分别执行不同的处理逻辑。
    // 这是 Flink CDC 实现“动态转换”的核心：它既处理**元数据（Schema）的演变，也处理实际数据（Row）**的转换。
    private void processEvent(Event event) {
        // 当遇到新表时，初始化该表的转换逻辑（如确定哪些列被保留、哪些列被改名），并通知下游算子建立对应的目标表结构。
        if (event instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            // CreateTableEvent from Source Contains the latest schema,
            // which may be different with the schema currently being processed.
            // 如果处理器映射表中不包含该表的 ID
            if (!preTransformProcessorMap.containsKey(createTableEvent.tableId())) {
                //  1. cacheCreateTable：根据转换规则计算该表转换后的新 Schema，并缓存处理器
                //  2. output.collect：将转换后的建表事件发送给下游,发给下游的是经过计算后引用的列组成的schema
                output.collect(new StreamRecord<>(cacheCreateTable(createTableEvent)));
                alreadySentCreateTableEvents.add(createTableEvent.tableId());
            }
        // 处理删表事件 (DropTableEvent)
        } else if (event instanceof DropTableEvent) {
            preTransformProcessorMap.remove(((DropTableEvent) event).tableId());
            // 直接下发删表事件给下游
            output.collect(new StreamRecord<>(event));
        // 处理清空表事件 (TruncateTableEvent)
        } else if (event instanceof TruncateTableEvent) {
            // 清空表不涉及结构变化，直接转发给下游
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof SchemaChangeEvent) {
            // 补发机制：如果之前因为某种原因没发过 CreateTableEvent，现在补发
            lazilyEmitCreateTableEvent(event);
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            // 旧的处理器是基于旧 Schema 的，必须移除
            preTransformProcessorMap.remove(schemaChangeEvent.tableId());
            // cacheChangeSchema：计算 DDL 变更（如加列）在转换规则下产生的新 Schema
            // 如果该变更对下游仍有意义（ifPresent），则下发转换后的 DDL 事件
            cacheChangeSchema(schemaChangeEvent)
                    .ifPresent(e -> output.collect(new StreamRecord<>(e)));
        // 处理数据变更事件 (DataChangeEvent)
        } else if (event instanceof DataChangeEvent) {
            // 补发机制：确保在处理数据行之前，下游已经收到了建表信息
            lazilyEmitCreateTableEvent(event);
            // 1. processDataChangeEvent：这是最核心的步骤！
            //    在这里执行真正的列投影（Projection）和行过滤（Filter）
            // 2. 将转换后（或过滤后）的数据行发送给下游
            output.collect(new StreamRecord<>(processDataChangeEvent(((DataChangeEvent) event))));
        }
    }

    /** Emit related CreateTableEvent for the first time when meeting ChangeEvent. */
    // 防御性机制，称为惰性发射（Lazy Emit）。
    // 它的核心作用是确保下游算子（如 Sink）在接收到任何数据记录（DML）或结构变更（DDL）之前，已经收到了对应的建表语句（CreateTableEvent）。
    private void lazilyEmitCreateTableEvent(Event event) {
        ChangeEvent changeEvent = (ChangeEvent) event;
        // 检查内存集合 alreadySentCreateTableEvents，确认该表的“初始建表事件”是否已经发送给下游。
        if (!alreadySentCreateTableEvents.contains(changeEvent.tableId())) {
            PreTransformChangeInfo stateTableChangeInfo =
                    preTransformChangeInfoMap.get(changeEvent.tableId());
            CreateTableEvent createTableEvent =
                    new CreateTableEvent(
                            stateTableChangeInfo.getTableId(),
                            stateTableChangeInfo.getPreTransformedSchema());
            output.collect(new StreamRecord<>(createTableEvent));
            alreadySentCreateTableEvents.add(changeEvent.tableId());
        }
    }

    // 主要任务是根据用户定义的 transform 规则，计算出转换后的新表结构，并建立“原始结构”与“目标结构”之间的映射关系。
    // 接收原始的“建表事件”，返回转换后的“建表事件”（CreateTableEvent 继承自 SchemaChangeEvent）。
    private SchemaChangeEvent cacheCreateTable(CreateTableEvent event) {
        TableId tableId = event.tableId();
        // 从流入的事件中提取从源数据库（如 MySQL）读取到的最原始的表结构。
        Schema originalSchema = event.getSchema();
        // 根据用户配置的 projection（投影）规则，生成一个新的 CreateTableEvent。例如，如果用户配置了“只保留 A、B 两列”，那么转换后的事件里，Schema 就只剩下这两列。
        event = transformCreateTableEvent(event);
        Schema newSchema = (event).getSchema();
        preTransformChangeInfoMap.put(
                tableId, PreTransformChangeInfo.of(tableId, originalSchema, newSchema));
        return event;
    }
    // Flink CDC 处理 Schema Evolution（表结构演进） 的核心逻辑。当源表发生 DDL 变更（如加列、删列）时，该方法决定了如何更新本地缓存，以及是否需要将这个变更下发给下游 Sink。
    private Optional<SchemaChangeEvent> cacheChangeSchema(SchemaChangeEvent event) {
        // 获取表 ID
        TableId tableId = event.tableId();
        // 从 Map 中获取变更前该表的 Schema 信息（包括转换前后的结构）。
        PreTransformChangeInfo tableChangeInfo = preTransformChangeInfoMap.get(tableId);
        // 将收到的 DDL 事件作用于缓存中的源表结构。比如源表加了一列，这里会生成一个包含新列的完整 originalSchema。
        Schema originalSchema =
                SchemaUtils.applySchemaChangeEvent(tableChangeInfo.getSourceSchema(), event);
        // 获取当前转换后 Schema
        Schema preTransformedSchema = tableChangeInfo.getPreTransformedSchema();

        Optional<SchemaChangeEvent> schemaChangeEvent;
        // 检查该表是否属于 SELECT * 模式。如果没有匹配到任何规则，默认按 true 处理。
        if (hasAsteriskMap.getOrDefault(tableId, true)) {
            // If this TableId is asterisk-ful, we should use the latest upstream schema as
            // referenced columns to perform schema evolution, not of the original ones generated
            // when creating tables. If hasAsteriskMap has no entry for this TableId, it means that
            // this TableId has not been referenced by any transform rules, and should be regarded
            // as asterisk-ful by default.
            // 如果是 * 模式，调用工具类。参数 true 表示透传：只要源表有列变更，下游对应的结构也要跟着变。
            schemaChangeEvent =
                    SchemaUtils.transformSchemaChangeEvent(
                            true, tableChangeInfo.getSourceSchema().getColumnNames(), event);
        } else {
            // 如果用户明确指定了列（如 SELECT a, b），则进入此分支。
            // Otherwise, we will use the pre-transformed columns to determine if the given schema
            // change event should be passed to downstream, only when it is presented in the
            // pre-transformed schema.
            // 参数 false 表示按需下发：只有当变更的列出现在用户定义的投影列表中时，才会生成下发给下游的事件。如果源表加了列 c，但用户只选了 a, b，则此结果为 Optional.empty()。
            schemaChangeEvent =
                    SchemaUtils.transformSchemaChangeEvent(
                            false,
                            tableChangeInfo.getPreTransformedSchema().getColumnNames(),
                            event);
        }
        // 如果经过过滤后，该 DDL 对下游依然有效（即下游也需要改结构）。
        if (schemaChangeEvent.isPresent()) {
            // 同步更新转换后 Schema
            preTransformedSchema =
                    SchemaUtils.applySchemaChangeEvent(
                            tableChangeInfo.getPreTransformedSchema(), schemaChangeEvent.get());
        }
        // 极其重要：结构变了，列的索引和映射关系也变了。必须重新解析转换规则，并为该表生成一个新的 PreTransformProcessor 覆盖旧的。
        cachePreTransformProcessor(tableId, originalSchema);
        preTransformChangeInfoMap.put(
                tableId, PreTransformChangeInfo.of(tableId, originalSchema, preTransformedSchema));
        // 返回转换后的事件
        return schemaChangeEvent;
    }

    private void cacheTransformRuleInfo(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();
        boolean notTransformed =
                transforms.stream().noneMatch(t -> t.getSelectors().isMatch(tableId));
        if (notTransformed) {
            // If this TableId isn't presented in any transform block, it should behave like a "*"
            // projection and should be regarded as asterisk-ful.
            hasAsteriskMap.put(tableId, true);
        } else {
            boolean hasAsterisk =
                    transforms.stream()
                            .filter(t -> t.getSelectors().isMatch(tableId))
                            .anyMatch(
                                    t ->
                                            TransformParser.hasAsterisk(
                                                    t.getProjection()
                                                            .map(TransformProjection::getProjection)
                                                            .orElse(null)));

            hasAsteriskMap.put(createTableEvent.tableId(), hasAsterisk);
        }
    }
    // 处理 Schema 转换核心逻辑的方法。它决定了原始的建表事件（来自源端 MySQL 等）在经过用户的转换规则（Transform Rules）后，最终呈现给下游的是什么样的表结构。
    // 实际上执行了一个两阶段转换过程：先执行元数据转换（Metadata Transform），再执行数据投影转换（Data Transform）
    private CreateTableEvent transformCreateTableEvent(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();
        // 循环处理预定义的元数据转换规则。
        // 这些规则通常涉及表名或列名的结构化重命名。
        for (Tuple2<Selectors, SchemaMetadataTransform> transform : schemaMetadataTransformers) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId)) {
                createTableEvent =
                        new CreateTableEvent(
                                tableId,
                            // 第一阶段转换：如果匹配成功，调用 transformSchemaMetaData 修改 Schema。
                            // 这通常处理列名映射、类型更改等基础元数据逻辑，并构造出一个临时的建表事件。
                                transformSchemaMetaData(
                                        createTableEvent.getSchema(), transform.f1));
            }
        }
        // 根据最新的（可能已被元数据修改的）Schema，初始化该表的 PreTransformProcessor。这个处理器内部包含了用户定义的 SQL 投影逻辑（如 AS, UPPER() 等）
        cachePreTransformProcessor(tableId, createTableEvent.getSchema());
        if (preTransformProcessorMap.containsKey(tableId)) {
            return preTransformProcessorMap
                    .get(tableId)
                    .preTransformCreateTableEvent(createTableEvent);
        }
        return createTableEvent;
    }
    // PreTransformOperator 中非常关键的解析阶段。
    // 它的核心作用是：针对某张具体的表，计算出它到底引用了哪些原始列，并为该表准备好后续转换所需的处理器（Processor）
    private void cachePreTransformProcessor(TableId tableId, Schema tableSchema) {
        LinkedHashSet<Column> referencedColumnsSet = new LinkedHashSet<>();
        // 用于标记当前这张表是否命中了任何用户定义的 transform 规则。
        boolean hasMatchTransform = false;
        for (PreTransformer transform : transforms) {
            if (!transform.getSelectors().isMatch(tableId)) {
                continue;
            }
            // 核心逻辑调用。解析该规则中的 projection（如 id, name）。
            // 它会分析表达式，将涉及到的列加入到 referencedColumnsSet 中，并构建投影映射关系。
            processProjectionTransform(tableId, tableSchema, referencedColumnsSet, transform);
            hasMatchTransform = true;
        }
        // 如果这张表没有匹配到任何规则，系统会调用一次参数为 null 的 processProjectionTransform。
        // 这通常意味着“全量透传”，即保留原表所有列。
        if (!hasMatchTransform) {
            processProjectionTransform(tableId, tableSchema, referencedColumnsSet, null);
        }
    }
    // 最核心的逻辑解析点。
    // 它的任务是：确定最终输出的 Schema 包含哪些列，并将解析好的逻辑封装进 PreTransformProcessor
    public void processProjectionTransform(
            TableId tableId,
            Schema tableSchema,
            LinkedHashSet<Column> referencedColumnsSet,
            @Nullable PreTransformer transform) {
        // If this TableId isn't presented in any transform block, it should behave like a "*"
        // projection and should be regarded as asterisk-ful.
        // 如果该表没有任何匹配的 transform 规则。
        if (transform == null) {
            // 将原始表的所有列都加入引用集合，行为等同于 SELECT *。
            referencedColumnsSet.addAll(tableSchema.getColumns());
            hasAsteriskMap.put(tableId, true);
        } else {
            // 获取用户定义的 projection 字符串（例如 "id, name, age + 1"）。
            TransformProjection transformProjection = transform.getProjection().get();
            // 调用解析器检查规则中是否显式写了 *（如 SELECT *, UPPER(name)）。
            boolean hasAsterisk = TransformParser.hasAsterisk(transformProjection.getProjection());
            if (hasAsterisk) {
                // 如果有 *，逻辑与 transform == null 一致：引用所有列并标记为通配符模式。
                referencedColumnsSet.addAll(tableSchema.getColumns());
                hasAsteriskMap.put(tableId, true);
            } else {
                // 获取该规则对应的 filter 条件（如 age > 18）。
                TransformFilter transformFilter = transform.getFilter().orElse(null);
                // 核心算法调用。解析投影表达式和过滤表达式，计算出它们引用了原始表的哪些物理列。例如 price * count 会被解析为依赖原始表的 price 和 count 两列。
                List<Column> referencedColumns =
                        TransformParser.generateReferencedColumns(
                                transformProjection.getProjection(),
                                transformFilter != null ? transformFilter.getExpression() : null,
                                tableSchema.getColumns());
                // update referenced columns of other projections of the same tableId, if any
                referencedColumnsSet.addAll(referencedColumns);
                hasAsteriskMap.putIfAbsent(tableId, false);
            }
        }

        PreTransformChangeInfo tableChangeInfo =
                PreTransformChangeInfo.of(
                        tableId,
                        tableSchema,
                        tableSchema.copy(new ArrayList<>(referencedColumnsSet)));
        preTransformProcessorMap.put(tableId, new PreTransformProcessor(tableChangeInfo));
    }
    // 核心作用是根据用户定义的元数据规则，对表的“非列”属性（主键、分区键、表参数）进行修改或重载。
    // 接收原始 Schema 和用户定义的元数据转换规则 SchemaMetadataTransform。
    private Schema transformSchemaMetaData(
            Schema schema, SchemaMetadataTransform schemaMetadataTransform) {
        // 创建一个新的 Schema 构建器，并直接复制原始表的所有列定义。注意：此方法不处理列的增删，只处理元数据。
        Schema.Builder schemaBuilder = Schema.newBuilder().setColumns(schema.getColumns());
        // 检查用户是否在转换规则中明确指定了新的主键。
        if (!schemaMetadataTransform.getPrimaryKeys().isEmpty()) {
            // 如果用户定义了新主键，则覆盖原始表的主键。这在进行分库分表合并时非常有用，可以重新定义联合主键。
            schemaBuilder.primaryKey(schemaMetadataTransform.getPrimaryKeys());
        } else {
            // 如果用户没有定义新主键，则保留来自源数据库（如 MySQL）的原始主键定义。
            schemaBuilder.primaryKey(schema.primaryKeys());
        }
        if (!schemaMetadataTransform.getPartitionKeys().isEmpty()) {
            schemaBuilder.partitionKey(schemaMetadataTransform.getPartitionKeys());
        } else {
            schemaBuilder.partitionKey(schema.partitionKeys());
        }
        if (!schemaMetadataTransform.getOptions().isEmpty()) {
            schemaBuilder.options(schemaMetadataTransform.getOptions());
        } else {
            schemaBuilder.options(schema.options());
        }
        return schemaBuilder.build();
    }
    // 每当有一条增量数据（如 MySQL 的 Insert/Update/Delete 记录）流经算子时，该方法负责执行真正的列投影、函数计算和字段填充。
    private DataChangeEvent processDataChangeEvent(DataChangeEvent dataChangeEvent) {
        if (!transforms.isEmpty()) {
            // 获取当前这条变更数据所属的表 ID。
            TableId tableId = dataChangeEvent.tableId();
            // 从缓存 Map 中获取该表对应的 PreTransformProcessor（它是在之前的 cachePreTransformProcessor 阶段编译好的）
            PreTransformProcessor processor = preTransformProcessorMap.get(tableId);
            Preconditions.checkArgument(
                    processor != null,
                    "Transform operator receives a data change event from table %s without a full schema view. "
                            + "This might happen if source with distributed tables doesn't emit CreateTableEvent first after fail-over. "
                            + "This is likely a bug, please consider filing an issue.",
                    tableId);
            // 提取“变更前”数据
            BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
            // 提取“变更后”数据
            BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
            // 如果存在变更前的数据（比如一条更新记录的旧值）。
            if (before != null) {
                // 调用处理器的 processFillDataField 方法。这里会执行实际的 SQL 表达式计算（如将原始列投影到目标列，执行 UPPER(name) 或 price * count 等）。
                BinaryRecordData projectedBefore = processor.processFillDataField(before);
                // 使用计算后的“投影数据”创建一个新的数据变更事件，替换掉原来的 before 部分。
                dataChangeEvent = DataChangeEvent.projectBefore(dataChangeEvent, projectedBefore);
            }
            if (after != null) {
                // 同样调用处理器，根据转换规则生成新的二进制行数据。
                BinaryRecordData projectedAfter = processor.processFillDataField(after);
                dataChangeEvent = DataChangeEvent.projectAfter(dataChangeEvent, projectedAfter);
            }
        }
        // 如果用户没有配置任何转换规则，直接跳过处理逻辑，原样返回，以保证最大性能（Passthrough 模式）。
        return dataChangeEvent;
    }

    private void clearOperator() {
        this.transforms = null;
        this.preTransformProcessorMap = null;
    }
}
