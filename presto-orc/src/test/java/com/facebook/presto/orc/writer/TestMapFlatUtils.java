
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.orc.DwrfEncryptionInfo.UNENCRYPTED;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static java.lang.Float.floatToRawIntBits;
import static org.joda.time.DateTimeZone.UTC;

public class TestMapFlatUtils
{
    private static final Integer BATCH_ROWS = 10000;

    private TestMapFlatUtils() {};

    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestFunctionAndTypeManager();

    public static Block mapBlockOf(Type keyType, Type valueType, Map<?, ?> value)
    {
        MapType mapType = mapType(keyType, valueType);
        BlockBuilder mapArrayBuilder = mapType.createBlockBuilder(null, 1);
        BlockBuilder singleMapWriter = mapArrayBuilder.beginBlockEntry();
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            appendToBlockBuilder(keyType, entry.getKey(), singleMapWriter);
            appendToBlockBuilder(valueType, entry.getValue(), singleMapWriter);
        }
        mapArrayBuilder.closeEntry();
        return mapType.getObject(mapArrayBuilder, 0);
    }

    public static MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) FUNCTION_AND_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    public static void appendToBlockBuilder(Type type, Object element, BlockBuilder blockBuilder)
    {
        Class<?> javaType = type.getJavaType();
        if (element == null) {
            blockBuilder.appendNull();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ROW) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            int field = 0;
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                field++;
            }
            blockBuilder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP) && element instanceof Map<?, ?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) element).entrySet()) {
                appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) element);
        }
        else if (javaType == long.class) {
            if (element instanceof SqlDecimal) {
                type.writeLong(blockBuilder, ((SqlDecimal) element).getUnscaledValue().longValue());
            }
            else if (REAL.equals(type)) {
                type.writeLong(blockBuilder, floatToRawIntBits(((Number) element).floatValue()));
            }
            else {
                type.writeLong(blockBuilder, ((Number) element).longValue());
            }
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, ((Number) element).doubleValue());
        }
        else if (javaType == Slice.class) {
            if (element instanceof String) {
                type.writeSlice(blockBuilder, Slices.utf8Slice(element.toString()));
            }
            else if (element instanceof byte[]) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) element));
            }
            else if (element instanceof SqlDecimal) {
                type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(((SqlDecimal) element).getUnscaledValue()));
            }
            else {
                type.writeSlice(blockBuilder, (Slice) element);
            }
        }
        else {
            type.writeObject(blockBuilder, element);
        }
    }

    public static Block[] createMapBlockWithIntegerData()
    {
        ImmutableList.Builder<Map<Integer, Integer>> values = new ImmutableList.Builder<>();
        for (int row = 0; row < 10; row++) {
            ImmutableMap.Builder<Integer, Integer> mapBuilder = new ImmutableMap.Builder<>();
            for (int i = 0; i < 10; i++) {
                mapBuilder.put(i, i);
            }
            values.add(mapBuilder.build());
        }
        MapType mapType = mapType(INTEGER, INTEGER);
        return createMapBlock(mapType, values.build(), INTEGER, INTEGER);
    }

    public static FlatMapValueWriterFactory createFlatMapValueWriterFactory(Type valueType)
    {
        List<OrcType> orcTypes = OrcType.createOrcRowType(
                0,
                ImmutableList.of("firstColumn"),
                ImmutableList.of(valueType));
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(CompressionKind.NONE)
                .build();
        return new FlatMapValueWriterFactory(
                        1,
                        orcTypes,
                        valueType,
                        columnWriterOptions,
                        DWRF,
                        UTC,
                        UNENCRYPTED,
                        DWRF.createMetadataWriter());
    }

    public static FlatMapValueColumnWriter createFlatMapValueColumnWriter(Type valueType)
    {
        FlatMapValueWriterFactory flatMapValueWriterFactory = createFlatMapValueWriterFactory(valueType);
        return flatMapValueWriterFactory.getFlatMapValueColumnWriter(1);
    }

    public static FlatMapColumnWriter createFlatMapColumnWriter(Type keyType, Type valueType)
    {
        ImmutableSet.Builder<Integer> mapFlattenColumnsList = ImmutableSet.builder();
        mapFlattenColumnsList.add(0);
        List<OrcType> orcTypes = OrcType.createOrcRowType(
                0,
                ImmutableList.of("mapColumn"),
                ImmutableList.of(mapType(keyType, valueType)));
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(CompressionKind.NONE)
                .setIntegerDictionaryEncodingEnabled(false)
                .build();
        FlatMapValueWriterFactory flatMapValueWriterFactory =
                new FlatMapValueWriterFactory(
                        3,
                        orcTypes,
                        valueType,
                        columnWriterOptions,
                        DWRF,
                        UTC,
                        UNENCRYPTED,
                        DWRF.createMetadataWriter());

        FlatMapColumnWriter writer = new FlatMapColumnWriter(
                1,
                columnWriterOptions,
                Optional.empty(),
                DWRF,
                DWRF.createMetadataWriter(),
                mapType(keyType, valueType),
                orcTypes,
                flatMapValueWriterFactory);
        return writer;
    }

    public static Block[] createMapBlock(Type type, List<?> values, Type keyType, Type valueType)
    {
        int row = 0;
        int batchId = 0;
        Block[] blocks = new Block[values.size() / BATCH_ROWS + 1];
        while (row < values.size()) {
            int end = Math.min(row + BATCH_ROWS, values.size());
            BlockBuilder blockBuilder = type.createBlockBuilder(null, BATCH_ROWS);
            while (row < end) {
                Object value = values.get(row++);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeObject(blockBuilder, mapBlockOf(keyType, valueType, (Map<?, ?>) value));
                }
            }
            blocks[batchId] = blockBuilder.build();
            batchId++;
        }
        return blocks;
    }

    public static Block[] createNumericDataBlock(Type type, List<Long> values)
    {
        int row = 0;
        int batchId = 0;
        Block[] blocks = new Block[values.size() / BATCH_ROWS + 1];
        while (row < values.size()) {
            int end = Math.min(row + BATCH_ROWS, values.size());
            BlockBuilder blockBuilder = type.createBlockBuilder(null, BATCH_ROWS);
            while (row < end) {
                Long value = values.get(row++);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeLong(blockBuilder, value.longValue());
                }
            }
            blocks[batchId] = blockBuilder.build();
            batchId++;
        }
        return blocks;
    }

    public static Block[] createStringDataBlock(Type type, List<String> values)
    {
        int row = 0;
        int batchId = 0;
        Block[] blocks = new Block[values.size() / BATCH_ROWS + 1];
        while (row < values.size()) {
            int end = Math.min(row + BATCH_ROWS, values.size());
            BlockBuilder blockBuilder = type.createBlockBuilder(null, BATCH_ROWS);
            while (row < end) {
                String value = values.get(row++);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    VARCHAR.writeString(blockBuilder, value);
                }
            }
            blocks[batchId] = blockBuilder.build();
            batchId++;
        }
        return blocks;
    }

    public static Block[] createIntegerBlock()
    {
        ImmutableList.Builder<Long> values = new ImmutableList.Builder<>();
        for (long row = 0; row < 10; row++) {
            values.add(row);
        }
        return createNumericDataBlock(INTEGER, values.build());
    }

    public static Block[] createStringBlock()
    {
        ImmutableList.Builder<String> values = new ImmutableList.Builder<>();
        for (long row = 0; row < 10; row++) {
            values.add(String.valueOf(row));
        }
        return createStringDataBlock(VARCHAR, values.build());
    }
}
