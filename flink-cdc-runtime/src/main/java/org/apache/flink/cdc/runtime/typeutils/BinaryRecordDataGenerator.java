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

package org.apache.flink.cdc.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.serializer.InternalSerializers;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryRecordDataWriter;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryWriter;

import java.util.Arrays;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/** This class is used to create {@link BinaryRecordData}. */
// 主要用于将通用的 Java 对象数组（Object[]）转换成 Flink CDC 内部优化过的二进制数据格式——BinaryRecordData
// 在 Flink CDC 处理数据时，性能的关键在于如何高效地在内存中表达一行数据。
// BinaryRecordData 是一种紧凑的、直接操作内存字节的行格式，而 BinaryRecordDataGenerator 就是生产这种格式的“工厂”：
// 数据格式化：它将零散的字段值按照指定的 DataType（数据类型）和序列化协议，写入到一块连续的内存空间中。
// 对象重用优化：为了减少垃圾回收（GC）压力，该类内部通过“重用写入器（Writer）”和“重用缓冲区”来处理数据，只在最后生成结果时才进行必要的拷贝。
// 屏蔽复杂性：它封装了复杂的二进制写入逻辑（如处理 Null 值、变长字段、对齐等），开发者只需要传入 Object[] 即可获得二进制行数据。

@PublicEvolving
public class BinaryRecordDataGenerator {
    // 存储每一列的数据类型定义（如 INT, VARCHAR, TIMESTAMP 等）
    private final DataType[] dataTypes;
    // 对应每一列的 Flink 类型序列化器。
    private final TypeSerializer[] serializers;
    // 作为二进制数据的中间容器。
    private transient BinaryRecordData reuseRecordData;
    // 具体的写入逻辑执行者。
    private transient BinaryRecordDataWriter reuseWriter;

    public BinaryRecordDataGenerator(RowType recordType) {
        this(recordType.getChildren().toArray(new DataType[0]));
    }

    public BinaryRecordDataGenerator(DataType[] dataTypes) {
        this(
                dataTypes,
                Arrays.stream(dataTypes)
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new));
    }

    public BinaryRecordDataGenerator(DataType[] dataTypes, TypeSerializer[] serializers) {
        checkArgument(
                dataTypes.length == serializers.length,
                "The types and serializers must have the same length. But types is %s and serializers is %s",
                dataTypes.length,
                serializers.length);

        this.dataTypes = dataTypes;
        this.serializers = serializers;

        this.reuseRecordData = new BinaryRecordData(dataTypes.length);
        this.reuseWriter = new BinaryRecordDataWriter(reuseRecordData);
    }

    /**
     * Creates an instance of {@link BinaryRecordData} with given field values.
     *
     * <p>Note: All fields of the record must be internal data structures. See {@link RecordData}.
     */
    public BinaryRecordData generate(Object[] rowFields) {
        checkArgument(
                dataTypes.length == rowFields.length,
                "The types and values must have the same length. But types is %s and values is %s",
                dataTypes.length,
                rowFields.length);

        reuseWriter.reset();
        for (int i = 0; i < dataTypes.length; i++) {
            if (rowFields[i] == null) {
                reuseWriter.setNullAt(i);
            } else {
                BinaryWriter.write(reuseWriter, i, rowFields[i], dataTypes[i], serializers[i]);
            }
        }
        reuseWriter.complete();
        return reuseRecordData.copy();
    }
}
