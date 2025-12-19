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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.mysql.source.parser.CustomMySqlAntlrDdlParser;
import org.apache.flink.cdc.connectors.mysql.table.MySqlReadableMetadata;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.data.TimestampData;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getHistoryRecord;

/** Event deserializer for {@link MySqlDataSource}. */
// 专门针对 MySQL 数据库，将 Debezium 捕获的原始 Binlog 记录转换为 Flink CDC 通用的事件（Event）。
// DDL 解析：MySQL 的表结构变更是以 DDL 字符串形式存在的，该类通过引入 Antlr 解析器将其转化为结构化的 SchemaChangeEvent。
// 特殊类型转换：处理 MySQL 特有的空间几何类型（Geometry、Point），将其转化为通用的 JSON 字符串。
// 元数据提取：根据用户配置，从 MySQL 的 Binlog 中提取表名、库名、时间戳等信息。
// 识别逻辑：定义了在 MySQL 协议下，什么样的记录属于“数据变更”，什么样的属于“结构变更”。
@Internal
public class MySqlEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;
    // MySQL Debezium 连接器在发送 DDL 变更时，其记录的 Key Schema 名称固定为此值。
    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.mysql.SchemaChangeKey";
    // Jackson 的 JSON 解析器，用于处理空间几何类型（Geometry）到 JSON 字符串的转换。
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // 是否捕获 DDL 变更。如果为 false，所有的 DDL 记录将被忽略。
    private final boolean includeSchemaChanges;
    // MySQL 映射策略。决定是将 TINYINT(1) 视为布尔值（Boolean）还是微整型（Byte）。
    private final boolean tinyInt1isBit;
    // 解析 DDL 时是否保留注释信息。
    private final boolean includeComments;
    // Debezium 的内存表结构缓存，用于在 DDL 解析过程中维护当前表的最新状态。
    private transient Tables tables;
    // 基于 Antlr 的自定义 MySQL DDL 解析器。它能读懂 ALTER TABLE ADD COLUMN 这种 SQL 语句并告诉 Flink 结构变了。
    private transient CustomMySqlAntlrDdlParser customParser;
    // 用户请求提取的元数据列表（如表名、操作时间）。
    private List<MySqlReadableMetadata> readableMetadataList;

    public MySqlEventDeserializer(
            DebeziumChangelogMode changelogMode,
            boolean includeSchemaChanges,
            boolean tinyInt1isBit) {
        this(
                changelogMode,
                includeSchemaChanges,
                new ArrayList<>(),
                includeSchemaChanges,
                tinyInt1isBit);
    }

    public MySqlEventDeserializer(
            DebeziumChangelogMode changelogMode,
            boolean includeSchemaChanges,
            List<MySqlReadableMetadata> readableMetadataList,
            boolean includeComments,
            boolean tinyInt1isBit) {
        super(new MySqlSchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
        this.readableMetadataList = readableMetadataList;
        this.includeComments = includeComments;
        this.tinyInt1isBit = tinyInt1isBit;
    }
    // 将 MySQL 的 DDL 字符串转换为 SchemaChangeEvent 列表。
    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
        if (includeSchemaChanges) {
            if (customParser == null) {
                customParser = new CustomMySqlAntlrDdlParser(includeComments, tinyInt1isBit);
                tables = new Tables();
            }

            try {
                HistoryRecord historyRecord = getHistoryRecord(record);

                String databaseName =
                        historyRecord.document().getString(HistoryRecord.Fields.DATABASE_NAME);
                String ddl =
                        historyRecord.document().getString(HistoryRecord.Fields.DDL_STATEMENTS);
                customParser.setCurrentDatabase(databaseName);
                customParser.parse(ddl, tables);
                return customParser.getAndClearParsedEvents();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to parse the schema change : " + record, e);
            }
        }
        return Collections.emptyList();
    }
    // 判断记录是否为数据变更（INSERT/UPDATE/DELETE）
    @Override
    protected boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return value != null
                && valueSchema != null
                && valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }
    // 判断记录是否为结构变更（DDL）。
    @Override
    protected boolean isSchemaChangeRecord(SourceRecord record) {
        Schema keySchema = record.keySchema();
        return keySchema != null && SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name());
    }

    @Override
    protected TableId getTableId(SourceRecord record) {
        String[] parts = record.topic().split("\\.");
        return TableId.tableId(parts[1], parts[2]);
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        Map<String, String> metadataMap = new HashMap<>();
        readableMetadataList.forEach(
                (mySqlReadableMetadata -> {
                    Object metadata = mySqlReadableMetadata.getConverter().read(record);
                    if (mySqlReadableMetadata.equals(MySqlReadableMetadata.OP_TS)) {
                        metadataMap.put(
                                mySqlReadableMetadata.getKey(),
                                String.valueOf(((TimestampData) metadata).getMillisecond()));
                    } else {
                        metadataMap.put(mySqlReadableMetadata.getKey(), String.valueOf(metadata));
                    }
                }));
        return metadataMap;
    }
    // 重写父类方法，专门处理 MySQL 的 Geometry（几何类型）。
    @Override
    protected Object convertToString(Object dbzObj, Schema schema) {
        // the Geometry datatype in MySQL will be converted to
        // a String with Json format
        if (Point.LOGICAL_NAME.equals(schema.name())
                || Geometry.LOGICAL_NAME.equals(schema.name())) {
            try {
                Struct geometryStruct = (Struct) dbzObj;
                byte[] wkb = geometryStruct.getBytes("wkb");
                String geoJson = OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)).asGeoJson();
                JsonNode originGeoNode = OBJECT_MAPPER.readTree(geoJson);
                Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32("srid"));
                Map<String, Object> geometryInfo = new HashMap<>();
                String geometryType = originGeoNode.get("type").asText();
                geometryInfo.put("type", geometryType);
                if (geometryType.equals("GeometryCollection")) {
                    geometryInfo.put("geometries", originGeoNode.get("geometries"));
                } else {
                    geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
                }
                geometryInfo.put("srid", srid.orElse(0));
                return BinaryStringData.fromString(
                        OBJECT_MAPPER.writer().writeValueAsString(geometryInfo));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to convert %s to geometry JSON.", dbzObj), e);
            }
        } else {
            return BinaryStringData.fromString(dbzObj.toString());
        }
    }
}
