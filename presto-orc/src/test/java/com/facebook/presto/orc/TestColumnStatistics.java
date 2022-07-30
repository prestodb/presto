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
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;
import com.facebook.presto.orc.metadata.statistics.MapColumnStatisticsBuilder;
import com.facebook.presto.orc.proto.DwrfProto.KeyInfo;
import com.facebook.presto.orc.protobuf.ByteString;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.facebook.presto.orc.OrcTester.mapType;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestColumnStatistics
{
    private static final int MAP_STATS_TEST_STRIPE_MAX_ROW_COUNT = 4;
    private static final int MAP_STATS_TEST_ROW_GROUP_MAX_ROW_COUNT = 2;
    public static final int COLUMN = 1;

    @DataProvider
    public static Object[][] mapKeyTypeProvider()
    {
        return new Object[][] {
                {BIGINT},
                {VARCHAR}
        };
    }

    @Test(dataProvider = "mapKeyTypeProvider")
    public void testEmptyMapStatistics(Type mapKeyType)
            throws Exception
    {
        MapType mapType = (MapType) mapType(mapKeyType, BIGINT);

        Page emptyMaps = createMapPage(mapType, Arrays.asList(
                mapOf(),
                mapOf(),
                mapOf(),
                mapOf(),
                mapOf(),
                mapOf()));

        Page emptyMapsWithNulls = createMapPage(mapType, Arrays.asList(
                null,
                mapOf(),
                null,
                null,
                null,
                mapOf()));

        Page emptyMapsWithAllNulls = createMapPage(mapType, Arrays.asList(
                null,
                null,
                null,
                null,
                null,
                null));

        doTestFlatMapStatistics(mapType, emptyMaps,
                new ColumnStatistics(6L),
                new ColumnStatistics(2L),
                new ColumnStatistics(2L),
                new ColumnStatistics(2L));

        doTestFlatMapStatistics(mapType, emptyMapsWithNulls,
                new ColumnStatistics(2L),
                new ColumnStatistics(1L),
                new ColumnStatistics(0L),
                new ColumnStatistics(1L));

        doTestFlatMapStatistics(mapType, emptyMapsWithAllNulls,
                new ColumnStatistics(0L),
                new ColumnStatistics(0L),
                new ColumnStatistics(0L),
                new ColumnStatistics(0L));
    }

    @Test(dataProvider = "mapKeyTypeProvider")
    public void testMapStatistics(Type mapKeyType)
            throws Exception
    {
        MapType mapType = (MapType) mapType(mapKeyType, BIGINT);
        Object[] keys = mapKeyType == BIGINT ? new Object[] {0L, 1L, 2L, 3L, 4L, 5L} : new Object[] {"k0", "k1", "k2", "k3", "k4", "k5"};
        Object everyRowKey = keys[0];  // [1, 2, 3, 4, 5, 6]
        Object firstRowGroupKey = keys[1]; // [11]
        Object secondRowGroupKey = keys[2]; // [22]
        Object thirdRowGroupKey = keys[3]; // [33]
        Object firstStripeKey = keys[4]; // [100]
        Object secondStripeKey = keys[5]; // [1000]

        Page maps = createMapPage(mapType, Arrays.asList(
                // stripe 1, row group 1
                mapOf(everyRowKey, 1L, firstRowGroupKey, 11L),
                mapOf(everyRowKey, 2L, firstStripeKey, 100L),

                // stripe 1, row group 2
                mapOf(everyRowKey, 3L, firstStripeKey, 100L),
                mapOf(everyRowKey, 4L, secondRowGroupKey, 22L),

                // stripe 2, row group 1
                mapOf(everyRowKey, 5L, thirdRowGroupKey, 33L, secondStripeKey, 1000L),
                mapOf(everyRowKey, 6L)));

        MapColumnStatisticsBuilder fileStatistics = new MapColumnStatisticsBuilder(true);
        fileStatistics.increaseValueCount(6);
        addIntStats(fileStatistics, everyRowKey, 6L, 1L, 6L, 21L);
        addIntStats(fileStatistics, firstRowGroupKey, 1L, 11L, 11L, 11L);
        addIntStats(fileStatistics, firstStripeKey, 2L, 100L, 100L, 200L);
        addIntStats(fileStatistics, secondRowGroupKey, 1L, 22L, 22L, 22L);
        addIntStats(fileStatistics, thirdRowGroupKey, 1L, 33L, 33L, 33L);
        addIntStats(fileStatistics, secondStripeKey, 1L, 1000L, 1000L, 1000L);

        MapColumnStatisticsBuilder rowGroupStats1 = new MapColumnStatisticsBuilder(true);
        rowGroupStats1.increaseValueCount(2);
        addIntStats(rowGroupStats1, everyRowKey, 2L, 1L, 2L, 3L);
        addIntStats(rowGroupStats1, firstRowGroupKey, 1L, 11L, 11L, 11L);
        addIntStats(rowGroupStats1, firstStripeKey, 1L, 100L, 100L, 100L);

        // keys from rowGroup1 are carried over as generic ColumnStatistics with 0 number of values
        MapColumnStatisticsBuilder rowGroupStats2 = new MapColumnStatisticsBuilder(true);
        rowGroupStats2.increaseValueCount(2);
        addIntStats(rowGroupStats2, everyRowKey, 2L, 3L, 4L, 7L);
        rowGroupStats2.addMapStatistics(keyInfo(firstRowGroupKey), new ColumnStatistics(0L));
        addIntStats(rowGroupStats2, firstStripeKey, 1L, 100L, 100L, 100L);
        addIntStats(rowGroupStats2, secondRowGroupKey, 1L, 22L, 22L, 22L);

        MapColumnStatisticsBuilder rowGroupStats3 = new MapColumnStatisticsBuilder(true);
        rowGroupStats3.increaseValueCount(2);
        addIntStats(rowGroupStats3, everyRowKey, 2L, 5L, 6L, 11L);
        addIntStats(rowGroupStats3, thirdRowGroupKey, 1L, 33L, 33L, 33L);
        addIntStats(rowGroupStats3, secondStripeKey, 1L, 1000L, 1000L, 1000L);

        doTestFlatMapStatistics(mapType, maps,
                fileStatistics.buildColumnStatistics(),
                rowGroupStats1.buildColumnStatistics(),
                rowGroupStats2.buildColumnStatistics(),
                rowGroupStats3.buildColumnStatistics());
    }

    @Test(dataProvider = "mapKeyTypeProvider")
    public void testMapStatisticsWithNulls(Type mapKeyType)
            throws Exception
    {
        MapType mapType = (MapType) mapType(mapKeyType, BIGINT);
        Object key = mapKeyType == BIGINT ? 0L : "k0";

        Page maps = createMapPage(mapType, Arrays.asList(
                // stripe 1, row group 1
                mapOf(key, 1L),
                mapOf(),

                // stripe 1, row group 2
                null,
                mapOf(key, null),

                // stripe 2, row group 1
                mapOf(key, null),
                mapOf(key, 2L)));

        MapColumnStatisticsBuilder fileStatistics = new MapColumnStatisticsBuilder(true);
        fileStatistics.increaseValueCount(5);
        addIntStats(fileStatistics, key, 2L, 1L, 2L, 3L);

        MapColumnStatisticsBuilder rowGroupStats1 = new MapColumnStatisticsBuilder(true);
        rowGroupStats1.increaseValueCount(2);
        addIntStats(rowGroupStats1, key, 1L, 1L, 1L, 1L);

        MapColumnStatisticsBuilder rowGroupStats2 = new MapColumnStatisticsBuilder(true);
        rowGroupStats2.increaseValueCount(1);
        rowGroupStats2.addMapStatistics(keyInfo(key), new ColumnStatistics(0L));

        MapColumnStatisticsBuilder rowGroupStats3 = new MapColumnStatisticsBuilder(true);
        rowGroupStats3.increaseValueCount(2);
        addIntStats(rowGroupStats3, key, 1L, 2L, 2L, 2L);

        doTestFlatMapStatistics(mapType, maps,
                fileStatistics.buildColumnStatistics(),
                rowGroupStats1.buildColumnStatistics(),
                rowGroupStats2.buildColumnStatistics(),
                rowGroupStats3.buildColumnStatistics());
    }

    // test a case when first row group has values, but second row group is all nulls,
    // thus forcing the keys from first row group to be carried over to the second row group
    // and use 0 value count
    @Test(dataProvider = "mapKeyTypeProvider")
    public void testMapStatisticsWithNullsFullRowGroup(Type mapKeyType)
            throws Exception
    {
        MapType mapType = (MapType) mapType(mapKeyType, BIGINT);
        Object key = mapKeyType == BIGINT ? 0L : "k0";

        Page maps = createMapPage(mapType, Arrays.asList(
                // stripe 1, row group 1
                mapOf(key, 1L),
                mapOf(),

                // stripe 1, row group 2 - make all values null to have make
                // nonNullRowGroupValueCount=0 in MapFlatColumnWriter
                null,
                null,

                // stripe 2, row group 1
                mapOf(key, null),
                mapOf(key, 2L)));

        MapColumnStatisticsBuilder fileStatistics = new MapColumnStatisticsBuilder(true);
        fileStatistics.increaseValueCount(4);
        addIntStats(fileStatistics, key, 2L, 1L, 2L, 3L);

        MapColumnStatisticsBuilder rowGroupStats1 = new MapColumnStatisticsBuilder(true);
        rowGroupStats1.increaseValueCount(2);
        addIntStats(rowGroupStats1, key, 1L, 1L, 1L, 1L);

        MapColumnStatisticsBuilder rowGroupStats2 = new MapColumnStatisticsBuilder(true);
        rowGroupStats2.increaseValueCount(0);
        rowGroupStats2.addMapStatistics(keyInfo(key), new ColumnStatistics(0L));

        MapColumnStatisticsBuilder rowGroupStats3 = new MapColumnStatisticsBuilder(true);
        rowGroupStats3.increaseValueCount(2);
        addIntStats(rowGroupStats3, key, 1L, 2L, 2L, 2L);

        doTestFlatMapStatistics(mapType, maps,
                fileStatistics.buildColumnStatistics(),
                rowGroupStats1.buildColumnStatistics(),
                rowGroupStats2.buildColumnStatistics(),
                rowGroupStats3.buildColumnStatistics());
    }

    private void doTestFlatMapStatistics(
            Type type,
            Page page,
            ColumnStatistics expectedFileStats,
            ColumnStatistics expectedRowGroupStats1,
            ColumnStatistics expectedRowGroupStats2,
            ColumnStatistics expectedRowGroupStats3)
            throws Exception
    {
        // this test case is geared toward pages with 6 rows to create two stripes with 4 and 2 rows each
        // row group size is 2 rows (MAP_STATS_TEST_ROW_GROUP_MAX_ROW_COUNT)
        assertEquals(page.getPositionCount(), 6);

        CapturingOrcFileIntrospector introspector;

        try (TempFile tempFile = new TempFile()) {
            writeFileForMapStatistics(type, page, tempFile);
            introspector = introspectOrcFile(type, tempFile);
        }

        // validate file statistics
        List<ColumnStatistics> allFileStatistics = introspector.getFileFooter().getFileStats();
        ColumnStatistics actualFileStatistics = allFileStatistics.get(COLUMN);
        assertEquals(actualFileStatistics, expectedFileStats);

        // prepare to validate row group statistics
        assertEquals(introspector.getStripes().size(), 2);
        assertEquals(introspector.getStripeInformations().size(), 2);
        assertEquals(introspector.getRowGroupIndexesByStripeOffset().size(), 2);
        Stripe stripe1 = introspector.getStripes().get(0);
        Stripe stripe2 = introspector.getStripes().get(1);
        assertEquals(stripe1.getRowCount(), 4);
        assertEquals(stripe2.getRowCount(), 2);

        long stripeOffset1 = introspector.getStripeInformations().get(0).getOffset();
        long stripeOffset2 = introspector.getStripeInformations().get(1).getOffset();

        Map<StreamId, List<RowGroupIndex>> stripeRowGroupIndexes1 = introspector.getRowGroupIndexesByStripeOffset().get(stripeOffset1);
        Map<StreamId, List<RowGroupIndex>> stripeRowGroupIndexes2 = introspector.getRowGroupIndexesByStripeOffset().get(stripeOffset2);
        assertNumberOfRowGroupStatistics(stripeRowGroupIndexes1, 2);
        assertNumberOfRowGroupStatistics(stripeRowGroupIndexes2, 1);

        // validate row group statistics
        ColumnStatistics actualRowGroupStats1 = getColumnRowGroupStats(stripeRowGroupIndexes1, 0); // stripe 1, row group 1
        ColumnStatistics actualRowGroupStats2 = getColumnRowGroupStats(stripeRowGroupIndexes1, 1); // stripe 1, row group 2
        ColumnStatistics actualRowGroupStats3 = getColumnRowGroupStats(stripeRowGroupIndexes2, 0); // stripe 2, row group 1
        assertEquals(actualRowGroupStats1, expectedRowGroupStats1);
        assertEquals(actualRowGroupStats2, expectedRowGroupStats2);
        assertEquals(actualRowGroupStats3, expectedRowGroupStats3);
    }

    // make sure the number of row group stats is as expected
    private void assertNumberOfRowGroupStatistics(Map<StreamId, List<RowGroupIndex>> rowGroupIndexes, int size)
    {
        for (Map.Entry<StreamId, List<RowGroupIndex>> e : rowGroupIndexes.entrySet()) {
            assertEquals(e.getValue().size(), size);
        }
    }

    // extract column statistics for a specific column and row group
    private ColumnStatistics getColumnRowGroupStats(Map<StreamId, List<RowGroupIndex>> rowGroupIndexes, int rowGroupIndex)
    {
        ColumnStatistics columnStatistics = null;
        for (Map.Entry<StreamId, List<RowGroupIndex>> e : rowGroupIndexes.entrySet()) {
            if (e.getKey().getColumn() == COLUMN) {
                // keep iterating to make sure there is only one StreamId for this column
                assertNull(columnStatistics);
                columnStatistics = e.getValue().get(rowGroupIndex).getColumnStatistics();
            }
        }

        assertNotNull(columnStatistics);
        return columnStatistics;
    }

    private static Page createMapPage(MapType type, List<Map<Object, Object>> maps)
    {
        Type keyType = type.getKeyType();
        Type valueType = type.getValueType();
        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) type.createBlockBuilder(null, maps.size());
        BlockBuilder mapKeyBuilder = mapBlockBuilder.getKeyBlockBuilder();
        BlockBuilder mapValueBuilder = mapBlockBuilder.getValueBlockBuilder();

        for (Map<Object, Object> map : maps) {
            if (map == null) {
                mapBlockBuilder.appendNull();
                continue;
            }

            mapBlockBuilder.beginDirectEntry();
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                writeNativeValue(keyType, mapKeyBuilder, entry.getKey());
                writeNativeValue(valueType, mapValueBuilder, entry.getValue());
            }
            mapBlockBuilder.closeEntry();
        }

        return new Page(mapBlockBuilder.build());
    }

    private void writeFileForMapStatistics(Type type, Page page, TempFile file)
            throws IOException
    {
        DefaultOrcWriterFlushPolicy flushPolicy = DefaultOrcWriterFlushPolicy.builder()
                .withStripeMaxRowCount(MAP_STATS_TEST_STRIPE_MAX_ROW_COUNT)
                .build();

        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withFlushPolicy(flushPolicy)
                .withRowGroupMaxRowCount(MAP_STATS_TEST_ROW_GROUP_MAX_ROW_COUNT)
                .withFlattenedColumns(ImmutableSet.of(0))
                .withMapStatisticsEnabled(true)
                .build();

        try (OrcWriter orcWriter = createOrcWriter(
                file.getFile(),
                DWRF,
                ZLIB,
                Optional.empty(),
                ImmutableList.of(type),
                writerOptions,
                NOOP_WRITER_STATS)) {
            orcWriter.write(page);
        }
    }

    private static CapturingOrcFileIntrospector introspectOrcFile(Type type, TempFile file)
            throws IOException
    {
        OrcDataSource dataSource = new FileOrcDataSource(file.getFile(),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                true);

        OrcReaderOptions readerOptions = OrcReaderOptions.builder()
                .withMaxMergeDistance(new DataSize(1, MEGABYTE))
                .withTinyStripeThreshold(new DataSize(1, MEGABYTE))
                .withMaxBlockSize(new DataSize(1, MEGABYTE))
                .withReadMapStatistics(true)
                .build();

        CapturingOrcFileIntrospector introspector = new CapturingOrcFileIntrospector();

        OrcReader reader = new OrcReader(
                dataSource,
                DWRF,
                new StorageOrcFileTailSource(),
                StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()),
                Optional.empty(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                readerOptions,
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats(),
                Optional.of(introspector));

        OrcSelectiveRecordReader recordReader = reader.createSelectiveRecordReader(
                ImmutableMap.of(0, type),
                ImmutableList.of(0),
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                OrcPredicate.TRUE,
                0,
                dataSource.getSize(),
                DateTimeZone.UTC,
                false,
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                Optional.empty(),
                1000);

        while (recordReader.getNextPage() != null) {
            // read all to load all row groups into the introspector
        }

        recordReader.close();
        dataSource.close();

        return introspector;
    }

    // create a KeyInfo object for a given key
    private static KeyInfo keyInfo(Object key)
    {
        if (key instanceof Long) {
            return KeyInfo.newBuilder().setIntKey((Long) key).build();
        }
        else {
            return KeyInfo.newBuilder().setBytesKey(ByteString.copyFromUtf8((String) key)).build();
        }
    }

    private static void addIntStats(MapColumnStatisticsBuilder stats, Object key, long numberOfValues, long min, long max, long sum)
    {
        stats.addMapStatistics(keyInfo(key), new IntegerColumnStatistics(numberOfValues, null, new IntegerStatistics(min, max, sum)));
    }

    private static Map<Object, Object> mapOf(Object... values)
    {
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(values[i], values[i + 1]);
        }
        return map;
    }
}
