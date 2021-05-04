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
package com.facebook.presto.orc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.facebook.presto.orc.writer.FlatMapColumnWriter;
import com.facebook.presto.orc.writer.FlatMapValueWriterFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.orc.DwrfEncryptionInfo.UNENCRYPTED;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.facebook.presto.orc.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static java.util.Locale.ENGLISH;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMapColumnWriter
{
    private static final int COLUMN_ID = 1;
    private static final int BATCH_ROWS = 1_000;
    private static final int STRIPE_MAX_ROWS = 15_000;

    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestFunctionAndTypeManager();

    public static MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) FUNCTION_AND_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    public FlatMapColumnWriter createFlatMapColumnWriter(Type keyType, Type valueType)
    {
        ImmutableSet.Builder<Integer> mapFlattenColumnsList = ImmutableSet.builder();
        mapFlattenColumnsList.add(0);
        List<OrcType> orcTypes = OrcType.createOrcRowType(
                0,
                ImmutableList.of("mapColumn"),
                ImmutableList.of(mapType(keyType, valueType)));
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(CompressionKind.NONE)
                .setFlatMapColumns(mapFlattenColumnsList.build())
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

    public Block[] createMapBlock(Type type, List<?> values, Type keyType, Type valueType)
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

    public Block[] createMapBlockWithIntegerData()
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

    @Test
    public void testFlatMapColumnWriterCreation()
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);
        writer.beginRowGroup();
        writer.finishRowGroup();
        assertTrue(writer.getBufferedBytes() == 0, "Bytes not buffered");
    }

    @Test
    public void testFlatMapWriteBlock()
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);

        Block[] blocks = createMapBlockWithIntegerData();
        writer.beginRowGroup();
        for (Block block : blocks) {
            writer.writeBlock(block);
        }

        /* Verify ColumnStatistics */
        Map<Integer, ColumnStatistics> columnStats = writer.finishRowGroup();
        assertEquals(columnStats.size(), 3);
        assertEquals(columnStats.get(1).getNumberOfValues(), 10);
        assertEquals(columnStats.get(2).getNumberOfValues(), 100);
        assertEquals(columnStats.get(3).getNumberOfValues(), 100);

        writer.close();
    }

    @Test
    public void testFlatMapIndexStreams()
            throws IOException
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);

        Block[] blocks = createMapBlockWithIntegerData();
        writer.beginRowGroup();
        for (Block block : blocks) {
            writer.writeBlock(block);
        }

        writer.finishRowGroup();
        writer.close();

        /* Verify Index Streams */
        List<StreamDataOutput> indexStreams = writer.getIndexStreams(Optional.empty());
        /* 11 index streams one for each unique value plus one for keyNode */
        assertEquals(indexStreams.size(), 11);
        assertEquals(indexStreams.get(0).getStream().getColumn(), 1);
        assertEquals(indexStreams.get(0).getStream().getStreamKind(), Stream.StreamKind.ROW_INDEX);
        for (int streamNum = 1; streamNum < 11; streamNum++) {
            assertEquals(indexStreams.get(streamNum).getStream().getColumn(), 3);
            assertEquals(indexStreams.get(streamNum).getStream().getStreamKind(), Stream.StreamKind.ROW_INDEX);
        }
    }

    @Test
    public void testFlatMapColumnEncodings()
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);

        Block[] blocks = createMapBlockWithIntegerData();
        writer.beginRowGroup();
        for (Block block : blocks) {
            writer.writeBlock(block);
        }

        writer.finishRowGroup();
        writer.close();

        /* Verify the generated column encodings */
        Map<Integer, ColumnEncoding> columnEncodings = writer.getColumnEncodings();
        assertEquals(columnEncodings.size(), 2);
        assertEquals(columnEncodings.get(1).getColumnEncodingKind(), ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT);
        assertEquals(columnEncodings.get(3).getColumnEncodingKind(), ColumnEncoding.ColumnEncodingKind.DIRECT);
        assertEquals(columnEncodings.get(1).getAdditionalSequenceEncodings().isPresent(), false);
        assertEquals(columnEncodings.get(3).getAdditionalSequenceEncodings().get().size(), 10);
    }

    @Test
    public void testFlatMapStripeStatistics()
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);

        Block[] blocks = createMapBlockWithIntegerData();
        writer.beginRowGroup();
        for (Block block : blocks) {
            writer.writeBlock(block);
        }

        writer.finishRowGroup();
        writer.close();
        /* Verify the generated column stripe statistics */
        Map<Integer, ColumnStatistics> columnStripeStats = writer.getColumnStripeStatistics();
        assertEquals(columnStripeStats.size(), 3);
        assertEquals(columnStripeStats.size(), 3);
        assertEquals(columnStripeStats.get(1).getNumberOfValues(), 10);
        assertEquals(columnStripeStats.get(2).getNumberOfValues(), 100);
        assertEquals(columnStripeStats.get(3).getNumberOfValues(), 100);
    }

    @Test
    public void testFlatMapDataStreams()
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);

        Block[] blocks = createMapBlockWithIntegerData();
        writer.beginRowGroup();
        for (Block block : blocks) {
            writer.writeBlock(block);
        }

        writer.finishRowGroup();
        writer.close();
        /* Verify the generated data streams */
        List<StreamDataOutput> dataStreams = writer.getDataStreams();
        assertEquals(dataStreams.size(), 20);
        for (int streamNum = 0; streamNum < 10; streamNum++) {
            assertEquals(dataStreams.get(streamNum * 2).getStream().getStreamKind(), Stream.StreamKind.IN_MAP);
            assertEquals(dataStreams.get(streamNum * 2 + 1).getStream().getStreamKind(), Stream.StreamKind.DATA);
        }
        writer.reset();
    }

    @Test
    public void testMapwithIntegerKey()
            throws IOException
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
        testMapWrite(mapType, DWRF, values.build(), INTEGER, INTEGER);
    }

    @Test
    public void testMapwithIntegerKeyMultiStrides()
            throws IOException
    {
        ImmutableList.Builder<Map<Integer, Integer>> values = new ImmutableList.Builder<>();
        for (int row = 0; row < 19980; row++) {
            ImmutableMap.Builder<Integer, Integer> mapBuilder = new ImmutableMap.Builder<>();
            for (int i = 0; i < 10; i++) {
                mapBuilder.put(i, i);
            }
            values.add(mapBuilder.build());
        }
        MapType mapType = mapType(INTEGER, INTEGER);
        testMapWrite(mapType, DWRF, values.build(), INTEGER, INTEGER);
    }

    @Test
    public void testMapwithIntegerKeyMultiKeys()
            throws IOException
    {
        ImmutableList.Builder<Map<Integer, Integer>> values = new ImmutableList.Builder<>();
        for (int row = 0; row < 31_000; row++) {
            ImmutableMap.Builder<Integer, Integer> mapBuilder = new ImmutableMap.Builder<>();
            for (int i = 0; i < 10; i++) {
                mapBuilder.put(i, i);
            }
            values.add(mapBuilder.build());
        }
        MapType mapType = mapType(INTEGER, INTEGER);
        testMapWrite(mapType, DWRF, values.build(), INTEGER, INTEGER);
    }

    @Test
    public void testMapwithIntegerKeyRandomValues()
            throws IOException
    {
        ImmutableList.Builder<Map<Integer, Integer>> values = new ImmutableList.Builder<>();
        for (int row = 0; row < 10000; row++) {
            ImmutableMap.Builder<Integer, Integer> mapBuilder = new ImmutableMap.Builder<>();
            TreeSet<Integer> uniqueRandomKeys = new TreeSet<Integer>();
            while (uniqueRandomKeys.size() < 10) {
                uniqueRandomKeys.add(ThreadLocalRandom.current().nextInt(1, 10001));
            }
            for (Integer entry : uniqueRandomKeys) {
                mapBuilder.put(entry, entry);
            }
            values.add(mapBuilder.build());
        }
        MapType mapType = mapType(INTEGER, INTEGER);
        testMapWrite(mapType, DWRF, values.build(), INTEGER, INTEGER);
    }

    @Test
    public void testMapwithIntegerKeyNoRows()
            throws IOException
    {
        ImmutableList.Builder<Map<Integer, Integer>> values = new ImmutableList.Builder<>();
        MapType mapType = mapType(INTEGER, INTEGER);
        testMapWrite(mapType, DWRF, values.build(), INTEGER, INTEGER);
    }

    @Test
    public void testMapwithIntegerKeyNullMap()
            throws IOException
    {
        ImmutableList.Builder<Map<Integer, Integer>> values = new ImmutableList.Builder<>();
        for (int row = 0; row < 10; row++) {
            ImmutableMap.Builder<Integer, Integer> mapBuilder = new ImmutableMap.Builder<>();
            values.add(mapBuilder.build());
        }
        MapType mapType = mapType(INTEGER, INTEGER);
        testMapWrite(mapType, DWRF, values.build(), INTEGER, INTEGER);
    }

    @Test
    public void testMapwithIntegerKeyMapValue()
            throws IOException
    {
        ImmutableList.Builder<Map<Integer, Map<Integer, Integer>>> values = new ImmutableList.Builder<>();
        for (int row = 0; row < 10; row++) {
            ImmutableMap.Builder<Integer, Map<Integer, Integer>> mapBuilder = new ImmutableMap.Builder<>();
            for (int i = 0; i < 10; i++) {
                mapBuilder.put(i, ImmutableMap.of(i, i));
            }
            values.add(mapBuilder.build());
        }
        MapType mapType = mapType(INTEGER, mapType(INTEGER, INTEGER));
        testMapWrite(mapType, DWRF, values.build(), INTEGER, mapType(INTEGER, INTEGER));
    }

    @Test
    public void testMapwithVarcharKey()
            throws IOException
    {
        ImmutableList.Builder<Map<String, String>> values = new ImmutableList.Builder<>();
        for (int row = 0; row < 10; row++) {
            ImmutableMap.Builder<String, String> mapBuilder = new ImmutableMap.Builder<>();
            for (int i = 0; i < 10; i++) {
                mapBuilder.put(String.valueOf(i), String.valueOf(i));
            }
            values.add(mapBuilder.build());
        }
        MapType mapType = mapType(VARCHAR, VARCHAR);
        testMapWrite(mapType, DWRF, values.build(), VARCHAR, VARCHAR);
    }

    private List<StripeFooter> testMapWrite(Type type, OrcEncoding encoding, List<?> values, Type keyType, Type valueType)
            throws IOException
    {
        List<Type> types = ImmutableList.of(type);
        OrcWriterOptions orcWriterOptions = new OrcWriterOptions().withStripeMaxRowCount(STRIPE_MAX_ROWS);
        //.withRowGroupMaxRowCount(12_998);
        try (TempFile tempFile = new TempFile()) {
            OrcWriter writer = createOrcWriter(tempFile.getFile(), encoding, ZSTD, Optional.empty(), types, orcWriterOptions, new OrcWriterStats());

            int row = 0;
            int batchId = 0;
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
                Block[] blocks = new Block[] {blockBuilder.build()};
                writer.write(new Page(blocks));
                batchId++;
            }
            writer.close();
            System.out.println(tempFile.getFile().getAbsolutePath());
            /*
            writer.validate(new FileOrcDataSource(
                    tempFile.getFile(),
                    new DataSize(1, MEGABYTE),
                    new DataSize(1, MEGABYTE),
                    new DataSize(1, MEGABYTE),
                    true));

             */

            try (OrcSelectiveRecordReader reader = OrcTester.createCustomOrcSelectiveRecordReader(
                    tempFile,
                    encoding,
                    OrcPredicate.TRUE,
                    type,
                    INITIAL_BATCH_SIZE,
                    true)) {
                row = 0;
                SqlFunctionProperties properties = SqlFunctionProperties
                        .builder()
                        .setTimeZoneKey(UTC_KEY)
                        .setLegacyTimestamp(true)
                        .setSessionStartTime(0)
                        .setSessionLocale(ENGLISH)
                        .setSessionUser("user")
                        .build();
                while (row < values.size()) {
                    Page page = reader.getNextPage();
                    if (page == null) {
                        break;
                    }

                    Block block = page.getBlock(0).getLoadedBlock();
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        Object value = values.get(row++);
                        assertEquals(block.isNull(i), value == null);
                        if (value != null) {
                            Map<?, ?> expectedMapRowValue = (Map<?, ?>) value;
                            Block actualMapRowValue = block.getSingleValueBlock(i);
                            ColumnarMap columnarMap = toColumnarMap(actualMapRowValue);
                            Block keysBlock = columnarMap.getKeysBlock();
                            Block valuesBlock = columnarMap.getValuesBlock();

                            if (keysBlock.getPositionCount() != expectedMapRowValue.size()) {
                                assertEquals(keysBlock.getPositionCount(), expectedMapRowValue.size(), "Map row invalid count");
                            }

                            for (int position = 0; position < keysBlock.getPositionCount(); position++) {
                                Object mapKeyEntry = keyType.getObjectValue(properties, keysBlock, position);
                                Object mapValueEntry = valueType.getObjectValue(properties, valuesBlock, position);
                                assertEquals(expectedMapRowValue.get(mapKeyEntry), mapValueEntry, "Map row value mismatch");
                            }
                        }
                    }
                }
                assertEquals(row, values.size());
            }

            return OrcTester.getStripes(tempFile.getFile(), encoding);
        }
    }
}
