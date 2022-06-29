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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcTester.arrayType;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.facebook.presto.orc.OrcTester.mapType;
import static com.facebook.presto.orc.OrcTester.rowType;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestFlatMapWriter
{
    // Number of rows should be larger than DEFAULT_ROW_GROUP_MAX_ROW_COUNT=10_000 to write multiple row groups
    private static final int ROWS = 50_000;
    private static final Map<?, ?> EMPTY_MAP = ImmutableMap.of();

    @Test
    public void testMapValueType()
            throws Exception
    {
        runTest(mapType(INTEGER, mapType(INTEGER, VARCHAR)),
                1, 2, 3,
                ImmutableMap.of(10, "20"), ImmutableMap.of(11, "22"), ImmutableMap.of(12, "23"), ImmutableMap.of(13, "24"), ImmutableMap.of(14, "25"));
    }

    @Test
    public void testArrayValueType()
            throws Exception
    {
        runTest(mapType(INTEGER, arrayType(INTEGER)),
                1, 2, 3,
                ImmutableList.of(1, 2), ImmutableList.of(3), ImmutableList.of(4, 5), ImmutableList.of(6), ImmutableList.of(9, 10));
    }

    @Test
    public void testRowValueType()
            throws Exception
    {
        runTest(mapType(INTEGER, rowType(INTEGER, VARCHAR)),
                1, 2, 3,
                ImmutableList.of(10, "20"), ImmutableList.of(11, "22"), ImmutableList.of(12, "23"), ImmutableList.of(13, "24"), ImmutableList.of(14, "25"));
    }

    @Test
    public void testDeeplyNestedValueType()
            throws Exception
    {
        // do one deeply nested value type
        runTest(mapType(INTEGER, arrayType(arrayType(INTEGER))),
                1, 2, 3,
                ImmutableList.of(ImmutableList.of(1, 2)),
                ImmutableList.of(ImmutableList.of(3), ImmutableList.of(33)),
                ImmutableList.of(ImmutableList.of(4, 5), ImmutableList.of(44, 55)),
                ImmutableList.of(ImmutableList.of(6)),
                ImmutableList.of(ImmutableList.of(9, 10)));
    }

    @Test
    public void testByteKey()
            throws Exception
    {
        runTest(mapType(TINYINT, INTEGER),
                (byte) 1, (byte) 2, (byte) 3,
                10, 11, 12, 13, 14);
    }

    @Test
    public void testShortKey()
            throws Exception
    {
        runTest(mapType(SMALLINT, INTEGER),
                (short) 1, (short) 2, (short) 3,
                10, 11, 12, 13, 14);
    }

    @Test
    public void testIntKey()
            throws Exception
    {
        runTest(mapType(INTEGER, INTEGER),
                1, 2, 3,
                10, 11, 12, 13, 14);
    }

    @Test
    public void testLongKey()
            throws Exception
    {
        runTest(mapType(BIGINT, INTEGER),
                1L, 2L, 3L,
                10, 11, 12, 13, 14);
    }

    @Test
    public void testStringKey()
            throws Exception
    {
        runTest(mapType(VARCHAR, INTEGER),
                "k1", "k2", "3",
                10, 11, 12, 13, 14);
    }

    @Test
    public void testVarbinaryKey()
            throws Exception
    {
        runTest(mapType(VARBINARY, VARCHAR),
                new SqlVarbinary("k1".getBytes(UTF_8)), new SqlVarbinary("k2".getBytes(UTF_8)), new SqlVarbinary("k3".getBytes(UTF_8)),
                "10", "11", "12", "13", "14");
    }

    @Test
    public void testNullKeyUnsupported()
    {
        OrcTester tester = OrcTester.quickDwrfFlatMapTester();
        Map<?, ?> nullKeyMap = new HashMap<>();
        nullKeyMap.put(null, null);

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> tester.testRoundTrip(mapType(INTEGER, INTEGER), newArrayList(nullKeyMap)));
        assertEquals(ex.getMessage(), "Map key is null at position: 0");
    }

    @Test
    public void testUnsupportedKeyType()
    {
        OrcTester tester = OrcTester.quickDwrfFlatMapTester();
        for (Type keyType : ImmutableList.of(BOOLEAN, REAL, DOUBLE, mapType(INTEGER, INTEGER), arrayType(INTEGER), rowType(INTEGER))) {
            expectThrows(IllegalArgumentException.class, () -> tester.testRoundTrip(mapType(keyType, INTEGER), newArrayList(EMPTY_MAP)));
        }
    }

    @Test
    public void testMixedNullMapAndEmptyMap()
            throws Exception
    {
        Type mapType = mapType(INTEGER, DOUBLE);
        OrcTester tester = OrcTester.quickDwrfFlatMapTester();

        tester.testRoundTrip(mapType, newArrayList(limit(cycle((Map) null), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle((Map) null, EMPTY_MAP), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(EMPTY_MAP, (Map) null), ROWS)));
    }

    private static <K, V> void runTest(Type mapType, K key1, K key2, K key3, V value1, V value2, V value3, V value4, V value5)
            throws Exception
    {
        OrcTester tester = OrcTester.quickDwrfFlatMapTester();
        Map<K, ?> nullValue = new HashMap<>();
        nullValue.put(key1, null);

        Map<K, V> map1 = ImmutableMap.of(key1, value1);
        Map<K, V> map2 = ImmutableMap.of(key1, value2);
        Map<K, V> map3 = ImmutableMap.of(key2, value3);
        Map<K, V> map4 = ImmutableMap.of(key3, value4);
        Map<K, V> map5 = ImmutableMap.of(key1, value5, key2, value4, key3, value2);

        // empty and null value maps
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(EMPTY_MAP), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(nullValue), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(nullValue, EMPTY_MAP), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(EMPTY_MAP, nullValue), ROWS)));

        // same keys
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(map1, map2), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(map1, map2, EMPTY_MAP), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(map5, EMPTY_MAP), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(map1, map2, nullValue), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(map1, map2, nullValue, EMPTY_MAP), ROWS)));

        // zig-zag keys
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(map1, map3, map4, map5), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(map1, map3, map4, map5, EMPTY_MAP), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(map1, map3, map4, map5, nullValue), ROWS)));
        tester.testRoundTrip(mapType, newArrayList(limit(cycle(map1, map3, map4, map5, nullValue, EMPTY_MAP), ROWS)));

        // randomize keys
        tester.testRoundTrip(mapType, newArrayList(limit(random(map1, map2, map3, map4, map5, nullValue, EMPTY_MAP), ROWS)));
    }

    @Test
    public void testMaxKeyLimit()
            throws Exception
    {
        MapType mapType = (MapType) mapType(INTEGER, INTEGER);
        int maxFlattenedMapKeyCount = 3;

        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withFlattenedColumns(ImmutableSet.of(0))
                .withMaxFlattenedMapKeyCount(maxFlattenedMapKeyCount)
                .build();

        try (TempFile tempFile = new TempFile()) {
            try (OrcWriter orcWriter = createOrcWriter(
                    tempFile.getFile(),
                    OrcEncoding.DWRF,
                    CompressionKind.ZLIB,
                    Optional.empty(),
                    ImmutableList.of(mapType),
                    writerOptions,
                    new NoOpOrcWriterStats())) {
                // write a block with 2 keys
                orcWriter.write(createMapPageForKeyLimitTest(mapType, maxFlattenedMapKeyCount - 1));

                // write a block with 3 keys, which is the max allowed number of keys
                expectThrows(IllegalStateException.class, () -> orcWriter.write(createMapPageForKeyLimitTest(mapType, maxFlattenedMapKeyCount)));
            }
        }
    }

    private static Page createMapPageForKeyLimitTest(MapType type, int keyCount)
    {
        Type keyType = type.getKeyType();
        Type valueType = type.getValueType();
        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) type.createBlockBuilder(null, 10);
        BlockBuilder mapKeyBuilder = mapBlockBuilder.getKeyBlockBuilder();
        BlockBuilder mapValueBuilder = mapBlockBuilder.getValueBlockBuilder();

        mapBlockBuilder.beginDirectEntry();
        for (int k = 0; k < keyCount; k++) {
            keyType.writeLong(mapKeyBuilder, k);
            valueType.writeLong(mapValueBuilder, k);
        }
        mapBlockBuilder.closeEntry();

        return new Page(mapBlockBuilder.build());
    }

    private static <T> Iterable<T> random(T... elements)
    {
        Random rnd = new Random(LocalDate.now().toEpochDay());
        Iterator<T> iterator = new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return true;
            }

            @Override
            public T next()
            {
                return elements[rnd.nextInt(elements.length)];
            }
        };
        return () -> iterator;
    }
}
