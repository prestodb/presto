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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.facebook.presto.orc.writer.ColumnSizeLayout;
import com.facebook.presto.orc.writer.StreamLayout.ByStreamSize;
import com.facebook.presto.orc.writer.StreamLayoutFactory;
import com.facebook.presto.orc.writer.StreamOrderingLayout;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.SortedMap;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcTester.arrayType;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.facebook.presto.orc.OrcTester.mapType;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_MAP;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.shuffle;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestStreamLayout
{
    private static StreamDataOutput createStream(int nodeId, int seqId, StreamKind streamKind, int length)
    {
        Stream stream = new Stream(nodeId, seqId, streamKind, length, true);
        return new StreamDataOutput(Slices.allocate(1024), stream);
    }

    private static StreamDataOutput createStream(int nodeId, StreamKind streamKind, int length)
    {
        Stream stream = new Stream(nodeId, DEFAULT_SEQUENCE_ID, streamKind, length, true);
        return new StreamDataOutput(Slices.allocate(1024), stream);
    }

    private static void verifyStream(Stream stream, int nodeId, StreamKind streamKind, int length)
    {
        assertEquals(stream.getColumn(), nodeId);
        assertEquals(stream.getLength(), length);
        assertEquals(stream.getStreamKind(), streamKind);
    }

    private static void verifyStream(Stream actual, int nodeId, int seqId, StreamKind streamKind, int length)
    {
        Stream expected = new Stream(nodeId, seqId, streamKind, length, true);
        assertEquals(actual, expected);
    }

    @Test
    public void testByStreamSize()
    {
        List<StreamDataOutput> streams = new ArrayList<>();
        int length = 10_000;
        for (int i = 0; i < 10; i++) {
            streams.add(createStream(i, PRESENT, length - i));
            streams.add(createStream(i, DATA, length - 100 - i));
        }

        shuffle(streams);

        new ByStreamSize().reorder(streams);

        assertEquals(streams.size(), 20);
        Iterator<StreamDataOutput> iterator = streams.iterator();
        for (int i = 9; i >= 0; i--) {
            verifyStream(iterator.next().getStream(), i, DATA, length - 100 - i);
        }

        for (int i = 9; i >= 0; i--) {
            verifyStream(iterator.next().getStream(), i, PRESENT, length - i);
        }
        assertFalse(iterator.hasNext());
    }

    @DataProvider
    public static Object[][] testByColumnSizeDataProvider()
    {
        // Assume the following schema:
        // Node 1, Column 0,  Type: MAP (map1)
        //   Node 2, Column 0, Type: INT
        //   Node 3, Column 0, Type: LIST
        //     Node 4, Column 0, Type: INT

        // Node 5, Column 1, Type: MAP (FLAT flatMap1)
        //   Node 6, Column 1, Type: INT (absent in flat maps)
        //   Node 7, Column 1,  Type: LIST
        //     Node 8, Column 1, Type: INT

        // Node 9, Column 2, Type: LIST (list1)
        //   Node 10, Column 2, Type: INT

        // Node 11, Column 3, Name: m2, Type: MAP (FLAT flatMap2)
        //   Node 12, Column 3, Name: key, Type: INT (absent in flat maps)
        //   Node 13, Column 3, Name: item, Type: INT

        // Node 14, Column 4, Type: INT (regular1)
        // Node 15, Column 5, Type: INT (regular2)
        // Node 16, Column 6, Type: INT (regular3)
        final int map1 = 1;
        final int map1Key = 2;
        final int map1Val = 3;
        final int map1ValElem = 4;

        final int flatMap1 = 5;
        final int flatMap1Val = 7;
        final int flatMap1ValElem = 8;

        final int list1 = 9;
        final int list1Elem = 10;

        final int flatMap2 = 11;
        final int flatMap2Val = 13;

        final int regular1 = 14;
        final int regular2 = 15;
        final int regular3 = 16;

        // supply streams in the expected order, test will perform several reorder
        // iterations with shuffling
        return new Object[][] {
                {
                        "split non-flatmap and flatmap columns into separate groups",
                        new StreamDataOutput[] {
                                createStream(map1, PRESENT, 0),
                                createStream(map1Key, PRESENT, 0),
                                createStream(list1, PRESENT, 0),
                                createStream(regular1, PRESENT, 0),
                                createStream(regular2, PRESENT, 0),

                                createStream(flatMap1, PRESENT, 0),
                                createStream(flatMap1Val, PRESENT, 0),
                                createStream(flatMap1ValElem, 1, PRESENT, 0),
                                createStream(flatMap1ValElem, 2, PRESENT, 0),
                                createStream(flatMap1ValElem, 3, PRESENT, 0),
                                createStream(flatMap2, PRESENT, 0),
                                createStream(flatMap2Val, 1, PRESENT, 0),
                                createStream(flatMap2Val, 2, PRESENT, 0)
                        }
                },
                {
                        "order columns by total column size in desc order",
                        new StreamDataOutput[] {
                                createStream(regular1, PRESENT, 5_000_000),
                                createStream(regular2, PRESENT, 4_000_000),

                                createStream(list1, PRESENT, 3_000_000),
                                createStream(list1Elem, PRESENT, 200),

                                createStream(map1, PRESENT, 10),
                                createStream(map1Key, PRESENT, 3_000_000),

                                createStream(flatMap2, PRESENT, 1),
                                createStream(flatMap2Val, 1, PRESENT, 5),
                                createStream(flatMap2Val, 1, DATA, 5),
                                createStream(flatMap2Val, 2, DATA, 5),
                                createStream(flatMap2Val, 3, PRESENT, 5),

                                createStream(flatMap1, PRESENT, 1),
                                createStream(flatMap1Val, PRESENT, 1),
                                createStream(flatMap1ValElem, 1, PRESENT, 1),
                                createStream(flatMap1ValElem, 1, DATA, 1),
                                createStream(flatMap1ValElem, 1, LENGTH, 1),
                        }
                },

                {
                        "group by sequence",
                        new StreamDataOutput[] {
                                createStream(flatMap1, PRESENT, 1),
                                createStream(flatMap1Val, 1, PRESENT, 1),
                                createStream(flatMap1ValElem, 1, PRESENT, 0),
                                createStream(flatMap1ValElem, 1, DATA, 0),
                                createStream(flatMap1ValElem, 1, LENGTH, 0),
                                createStream(flatMap1Val, 2, PRESENT, 1),
                                createStream(flatMap1ValElem, 2, PRESENT, 0),
                                createStream(flatMap1ValElem, 2, DATA, 0),
                                createStream(flatMap1ValElem, 2, LENGTH, 0),

                                createStream(flatMap2, PRESENT, 0),
                                createStream(flatMap2Val, 1, PRESENT, 0),
                                createStream(flatMap2Val, 1, DATA, 0),
                                createStream(flatMap2Val, 1, LENGTH, 0),
                                createStream(flatMap2Val, 2, PRESENT, 0),
                                createStream(flatMap2Val, 2, DATA, 0),
                                createStream(flatMap2Val, 2, LENGTH, 0),
                        }
                },
                {
                        "order sequence streams by column+sequence size in desc order",
                        new StreamDataOutput[] {
                                createStream(flatMap1, PRESENT, 1000),
                                // seq 2
                                createStream(flatMap1Val, 2, PRESENT, 20),
                                createStream(flatMap1ValElem, 2, PRESENT, 20),
                                createStream(flatMap1ValElem, 2, DATA, 20),
                                createStream(flatMap1ValElem, 2, LENGTH, 20),
                                // seq 1
                                createStream(flatMap1Val, 1, PRESENT, 10),
                                createStream(flatMap1ValElem, 1, PRESENT, 10),
                                createStream(flatMap1ValElem, 1, DATA, 10),
                                createStream(flatMap1ValElem, 1, LENGTH, 10),
//
                                createStream(flatMap2, PRESENT, 10),
                                // seq 1
                                createStream(flatMap2Val, 1, PRESENT, 30),
                                createStream(flatMap2Val, 1, DATA, 30),
                                createStream(flatMap2Val, 1, LENGTH, 30),
                                // seq 2
                                createStream(flatMap2Val, 2, PRESENT, 10),
                                createStream(flatMap2Val, 2, DATA, 10),
                                createStream(flatMap2Val, 2, LENGTH, 10),
                        }
                },
                {
                        "order by the node in asc order",
                        new StreamDataOutput[] {
                                createStream(list1, PRESENT, 5),
                                createStream(list1Elem, PRESENT, 5),
                                createStream(regular1, PRESENT, 10),
                                createStream(regular2, DATA, 10),

                                createStream(flatMap1, PRESENT, 5),
                                createStream(flatMap1Val, 1, DATA, 5),
                                createStream(flatMap2, PRESENT, 5),
                                createStream(flatMap2Val, 1, DATA, 5),
                        }
                },
                {
                        "order by stream kind",
                        new StreamDataOutput[] {
                                createStream(list1, PRESENT, 0),
                                createStream(list1Elem, PRESENT, 0),
                                createStream(list1Elem, DATA, 0),
                                createStream(list1Elem, LENGTH, 0),
                        }
                },
        };
    }

    @Test(dataProvider = "testByColumnSizeDataProvider")
    public void testByColumnSize(String testName, StreamDataOutput[] streams)
    {
        List<StreamDataOutput> expectedStreams = ImmutableList.copyOf(streams);
        List<StreamDataOutput> testStreams = new ArrayList<>(ImmutableList.copyOf(streams));

        Map<Integer, Integer> nodeToColumn = ImmutableMap.<Integer, Integer>builder()
                .put(1, 0)
                .put(2, 0)
                .put(3, 0)
                .put(4, 0)
                .put(5, 1)
                .put(6, 1)
                .put(7, 1)
                .put(8, 1)
                .put(9, 2)
                .put(10, 2)
                .put(11, 3)
                .put(12, 3)
                .put(13, 3)
                .put(14, 4)
                .put(15, 5)
                .put(16, 6)
                .build();

        Map<Integer, ColumnEncoding> nodeIdToColumnEncodings = ImmutableMap.<Integer, ColumnEncoding>builder()
                .put(5, new ColumnEncoding(DWRF_MAP_FLAT, 0))
                .put(11, new ColumnEncoding(DWRF_MAP_FLAT, 0))
                .build();

        ColumnSizeLayout layout = new ColumnSizeLayout();

        int seed = LocalDate.now(ZoneId.of("America/Los_Angeles")).getDayOfYear();
        Random rnd = new Random(seed);

        for (int i = 0; i < 25; i++) {
            shuffle(testStreams, rnd);
            layout.reorder(testStreams, nodeToColumn, nodeIdToColumnEncodings);
            assertEquals(testStreams, expectedStreams);
        }
    }

    @DataProvider(name = "testParams")
    public static Object[][] testParams()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "testParams")
    public void testByStreamSizeStreamOrdering(boolean isEmptyMap)
    {
        List<StreamDataOutput> streams = createStreams(isEmptyMap);
        ByStreamSize streamLayout = new ByStreamSize();
        StreamOrderingLayout streamOrderingLayout = new StreamOrderingLayout(createStreamReorderingInput(), streamLayout);
        streamOrderingLayout.reorder(streams, createNodeIdToColumnId(), createColumnEncodings(isEmptyMap));

        Iterator<StreamDataOutput> iterator = streams.iterator();
        if (!isEmptyMap) {
            verifyFlatMapColumns(iterator);
        }
        // non flat map columns
        verifyStream(iterator.next().getStream(), 5, 0, DATA, 1);
        verifyStream(iterator.next().getStream(), 2, 0, LENGTH, 2);
        verifyStream(iterator.next().getStream(), 1, 0, DATA, 3);
        verifyStream(iterator.next().getStream(), 4, 0, LENGTH, 5);
        verifyStream(iterator.next().getStream(), 3, 0, DATA, 8);
        verifyStream(iterator.next().getStream(), 1, 0, PRESENT, 12);
        if (!isEmptyMap) {
            // flat map stream not reordered
            verifyStream(iterator.next().getStream(), 11, 5, IN_MAP, 13);
            verifyStream(iterator.next().getStream(), 11, 5, LENGTH, 14);
            verifyStream(iterator.next().getStream(), 12, 5, DATA, 15);
        }
        assertFalse(iterator.hasNext());
    }

    @Test(dataProvider = "testParams")
    public void testByColumnSizeStreamOrdering(boolean isEmptyMap)
    {
        List<StreamDataOutput> streams = createStreams(isEmptyMap);
        ColumnSizeLayout layout = new ColumnSizeLayout();
        StreamOrderingLayout streamOrderingLayout = new StreamOrderingLayout(createStreamReorderingInput(), layout);
        streamOrderingLayout.reorder(streams, createNodeIdToColumnId(), createColumnEncodings(isEmptyMap));

        Iterator<StreamDataOutput> iterator = streams.iterator();
        if (!isEmptyMap) {
            verifyFlatMapColumns(iterator);
        }

        // regular columns
        // column 1 with total size 16, ordered by nodes
        verifyStream(iterator.next().getStream(), 2, 0, LENGTH, 2);
        verifyStream(iterator.next().getStream(), 3, 0, DATA, 8);
        verifyStream(iterator.next().getStream(), 4, 0, LENGTH, 5);
        verifyStream(iterator.next().getStream(), 5, 0, DATA, 1);

        // column 0 with total size 15, ordered by stream kind
        verifyStream(iterator.next().getStream(), 1, 0, PRESENT, 12);
        verifyStream(iterator.next().getStream(), 1, 0, DATA, 3);

        if (!isEmptyMap) {
            // flat map stream are also ordered by node and kind
            verifyStream(iterator.next().getStream(), 11, 5, LENGTH, 14);
            verifyStream(iterator.next().getStream(), 11, 5, IN_MAP, 13);
            verifyStream(iterator.next().getStream(), 12, 5, DATA, 15);
        }
        assertFalse(iterator.hasNext());
    }

    private static Map<Integer, ColumnEncoding> createColumnEncodings(boolean isEmptyMap)
    {
        SortedMap<Integer, DwrfSequenceEncoding> seqIdToEncodings1 = ImmutableSortedMap.of(
                1, new DwrfSequenceEncoding(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(100).build(),
                        new ColumnEncoding(DIRECT, 0)),
                2, new DwrfSequenceEncoding(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(200).build(),
                        new ColumnEncoding(DIRECT, 0)),
                3, new DwrfSequenceEncoding(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(300).build(),
                        new ColumnEncoding(DIRECT, 0)));

        SortedMap<Integer, DwrfSequenceEncoding> seqIdToEncodings2 = ImmutableSortedMap.of(
                1, new DwrfSequenceEncoding(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(100).build(),
                        new ColumnEncoding(DIRECT, 0)),
                2, new DwrfSequenceEncoding(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(200).build(),
                        new ColumnEncoding(DIRECT, 0)),
                3, new DwrfSequenceEncoding(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(300).build(),
                        new ColumnEncoding(DIRECT, 0)),
                4, new DwrfSequenceEncoding(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(400).build(),
                        new ColumnEncoding(DIRECT, 0)),
                5, new DwrfSequenceEncoding(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(500).build(),
                        new ColumnEncoding(DIRECT, 0)));

        return ImmutableMap.<Integer, ColumnEncoding>builder()
                .put(1, new ColumnEncoding(DIRECT, 0))
                .put(2, new ColumnEncoding(DIRECT, 0))
                .put(3, new ColumnEncoding(DIRECT, 0))
                .put(4, new ColumnEncoding(DIRECT, 0))
                .put(5, new ColumnEncoding(DIRECT, 0))
                .put(6, new ColumnEncoding(DWRF_MAP_FLAT, 0))
                .put(8, new ColumnEncoding(DIRECT, 0, isEmptyMap ? Optional.empty() : Optional.of(seqIdToEncodings1)))
                .put(9, new ColumnEncoding(DWRF_MAP_FLAT, 0))
                .put(11, new ColumnEncoding(DIRECT, 0, isEmptyMap ? Optional.empty() : Optional.of(seqIdToEncodings2)))
                .put(12, new ColumnEncoding(DIRECT, 0, isEmptyMap ? Optional.empty() : Optional.of(seqIdToEncodings2)))
                .build();
    }

    private static Map<Integer, Integer> createNodeIdToColumnId()
    {
        // col Id 0, 1, 2, 3
        // node Id 0 to 12
        return ImmutableMap.<Integer, Integer>builder()
                .put(1, 0)
                .put(2, 1)
                .put(3, 1)
                .put(4, 1)
                .put(5, 1)
                .put(6, 2)
                .put(7, 2)
                .put(8, 2)
                .put(9, 3)
                .put(10, 3)
                .put(11, 3)
                .put(12, 3)
                .build();
    }

    private static DwrfStreamOrderingConfig createStreamReorderingInput()
    {
        Map<Integer, List<DwrfProto.KeyInfo>> columnIdToFlatMapKeyIds = new HashMap<>();
        columnIdToFlatMapKeyIds.put(
                // column exists
                //  complete overlap between column encodings and key ordering
                2, ImmutableList.of(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(300).build(),
                        DwrfProto.KeyInfo.newBuilder().setIntKey(200).build(),
                        DwrfProto.KeyInfo.newBuilder().setIntKey(100).build()));
        columnIdToFlatMapKeyIds.put(
                // column exists,
                // key 600 doesn't exist in the column encodings
                // key 500 exists in the column encodings not present in the order list
                3, ImmutableList.of(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(100).build(),
                        DwrfProto.KeyInfo.newBuilder().setIntKey(200).build(),
                        DwrfProto.KeyInfo.newBuilder().setIntKey(400).build(),
                        DwrfProto.KeyInfo.newBuilder().setIntKey(300).build(),
                        DwrfProto.KeyInfo.newBuilder().setIntKey(600).build()));
        columnIdToFlatMapKeyIds.put(// column does not exist in the input schema
                4, ImmutableList.of(
                        DwrfProto.KeyInfo.newBuilder().setIntKey(100).build(),
                        DwrfProto.KeyInfo.newBuilder().setIntKey(200).build()));
        return new DwrfStreamOrderingConfig(columnIdToFlatMapKeyIds);
    }

    private static void verifyFlatMapColumns(Iterator<StreamDataOutput> iterator)
    {
        // flat map stream are ordered by node and kind
        // Kind order: DATA:1, LENGTH:2, IN_MAP:12
        // column 2
        verifyStream(iterator.next().getStream(), 8, 3, DATA, 7);
        verifyStream(iterator.next().getStream(), 8, 3, IN_MAP, 6);
        verifyStream(iterator.next().getStream(), 8, 2, DATA, 5);
        verifyStream(iterator.next().getStream(), 8, 2, IN_MAP, 4);
        verifyStream(iterator.next().getStream(), 8, 1, DATA, 3);
        verifyStream(iterator.next().getStream(), 8, 1, IN_MAP, 2);

        // column 3
        verifyStream(iterator.next().getStream(), 11, 1, LENGTH, 2);
        verifyStream(iterator.next().getStream(), 11, 1, IN_MAP, 1);
        verifyStream(iterator.next().getStream(), 12, 1, DATA, 3);

        verifyStream(iterator.next().getStream(), 11, 2, LENGTH, 5);
        verifyStream(iterator.next().getStream(), 11, 2, IN_MAP, 4);
        verifyStream(iterator.next().getStream(), 12, 2, DATA, 6);

        verifyStream(iterator.next().getStream(), 11, 4, LENGTH, 11);
        verifyStream(iterator.next().getStream(), 11, 4, IN_MAP, 10);
        verifyStream(iterator.next().getStream(), 12, 4, DATA, 12);

        verifyStream(iterator.next().getStream(), 11, 3, LENGTH, 8);
        verifyStream(iterator.next().getStream(), 11, 3, IN_MAP, 7);
        verifyStream(iterator.next().getStream(), 12, 3, DATA, 9);
    }

    private static List<StreamDataOutput> createStreams(boolean isEmptyMap)
    {
        // Assume the file has the following schema:
        // column 0: 1INT
        // column 1: 2MAP<3INT, 4LIST<5INT>> // non flat map
        // column 2: 6MAP<7INT, 8FLOAT> // flat map
        // column 3: 9MAP<10INT, 11LIST<12INT> // flat map

        List<StreamDataOutput> streams = new ArrayList<>();
        // column 0
        streams.add(createStream(1, DATA, 3));
        streams.add(createStream(1, PRESENT, 12));

        // column 1 MAP<INT, LIST<INT>> <2, <3, 4<5>>>>
        streams.add(createStream(2, LENGTH, 2)); // MAP
        streams.add(createStream(3, DATA, 8)); // INT
        streams.add(createStream(4, LENGTH, 5)); // LIST<INT>
        streams.add(createStream(5, DATA, 1)); // INT

        if (!isEmptyMap) {
            // column 2 MAP<INT, FLOAT> <6 <7, 8>>
            streams.add(createStream(8, 1, IN_MAP, 2));
            streams.add(createStream(8, 1, DATA, 3));
            streams.add(createStream(8, 2, IN_MAP, 4));
            streams.add(createStream(8, 2, DATA, 5));
            streams.add(createStream(8, 3, IN_MAP, 6));
            streams.add(createStream(8, 3, DATA, 7));

            // column 3 MAP<INT, LIST<INT> <9 <10, 11<12>>>
            streams.add(createStream(11, 1, IN_MAP, 1));
            streams.add(createStream(11, 1, LENGTH, 2));
            streams.add(createStream(12, 1, DATA, 3));

            streams.add(createStream(11, 2, IN_MAP, 4));
            streams.add(createStream(11, 2, LENGTH, 5));
            streams.add(createStream(12, 2, DATA, 6));

            streams.add(createStream(11, 3, IN_MAP, 7));
            streams.add(createStream(11, 3, LENGTH, 8));
            streams.add(createStream(12, 3, DATA, 9));

            streams.add(createStream(11, 4, IN_MAP, 10));
            streams.add(createStream(11, 4, LENGTH, 11));
            streams.add(createStream(12, 4, DATA, 12));

            streams.add(createStream(11, 5, IN_MAP, 13));
            streams.add(createStream(11, 5, LENGTH, 14));
            streams.add(createStream(12, 5, DATA, 15));
        }
        return streams;
    }

    @DataProvider
    public static Object[][] streamOrderingLayoutProvider()
    {
        // stream ordering test writes the following keys: 0, 1, 2
        return new Object[][] {
                {2L, 0L, 1L}, // full match
                {new Long[0]}, // no keys
                {0L}, // partially matching keys

                {10L, 20L}, // full mismatch
                {2L, 10L} // partial match + mismatch
        };
    }

    @Test(dataProvider = "streamOrderingLayoutProvider")
    public void testStreamOrderingLayoutEndToEndPrimitive(Object[] orderedKeys)
            throws Exception
    {
        MapType mapType = (MapType) mapType(INTEGER, INTEGER);

        // create a map with a single row and three keys 0, 1, 2
        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, 10);
        BlockBuilder mapKeyBuilder = mapBlockBuilder.getKeyBlockBuilder();
        BlockBuilder mapValueBuilder = mapBlockBuilder.getValueBlockBuilder();
        mapBlockBuilder.beginDirectEntry();
        for (int k = 0; k < 3; k++) {
            INTEGER.writeLong(mapKeyBuilder, k);
            INTEGER.writeLong(mapValueBuilder, k);
        }
        mapBlockBuilder.closeEntry();
        Page page = new Page(mapBlockBuilder.build());

        // inject a custom layout factory
        StreamLayoutFactory streamLayoutFactory = createLongKeysStreamLayoutFactory(0, orderedKeys);
        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withFlattenedColumns(ImmutableSet.of(0))
                .withStreamLayoutFactory(streamLayoutFactory)
                .build();

        try (TempFile tempFile = new TempFile()) {
            try (OrcWriter orcWriter = createOrcWriter(
                    tempFile.getFile(),
                    OrcEncoding.DWRF,
                    CompressionKind.ZLIB,
                    Optional.empty(),
                    ImmutableList.of(mapType),
                    writerOptions,
                    NOOP_WRITER_STATS)) {
                orcWriter.write(page);
            }

            assertFileStreamsOrder(orderedKeys, mapType, tempFile);
        }
    }

    @Test(dataProvider = "streamOrderingLayoutProvider")
    public void testStreamOrderingLayoutEndToEndComplexEmpty(Object[] orderedKeys)
            throws Exception
    {
        MapType mapType = (MapType) mapType(INTEGER, arrayType(INTEGER));

        // create an empty map
        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, 10);
        mapBlockBuilder.beginDirectEntry();
        mapBlockBuilder.closeEntry();
        Page page = new Page(mapBlockBuilder.build());

        // inject a custom layout factory
        StreamLayoutFactory streamLayoutFactory = createLongKeysStreamLayoutFactory(0, orderedKeys);
        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withFlattenedColumns(ImmutableSet.of(0))
                .withStreamLayoutFactory(streamLayoutFactory)
                .build();

        try (TempFile tempFile = new TempFile()) {
            try (OrcWriter orcWriter = createOrcWriter(
                    tempFile.getFile(),
                    OrcEncoding.DWRF,
                    CompressionKind.ZLIB,
                    Optional.empty(),
                    ImmutableList.of(mapType),
                    writerOptions,
                    NOOP_WRITER_STATS)) {
                orcWriter.write(page);
                // no need to verify empty map
            }
        }
    }

    private static void assertFileStreamsOrder(Object[] orderedKeys, MapType mapType, TempFile tempFile)
            throws IOException
    {
        // introspect the file to get access to the file meta information
        CapturingOrcFileIntrospector introspector = new CapturingOrcFileIntrospector();
        readFileWithIntrospector(mapType, introspector, tempFile);
        assertEquals(introspector.getStripeFooterByStripeOffset().size(), 1);

        StripeFooter stripeFooter = introspector.getStripeFooterByStripeOffset().values().iterator().next();

        // get data streams, because only data streams are ordered
        List<Stream> dataStreams = stripeFooter.getStreams().stream()
                .filter(s -> s.getStreamKind() != StreamKind.ROW_INDEX)
                .collect(toImmutableList());

        // get sequence to key mapping for flat map value node
        int node = 3; // map value node
        ColumnEncoding columnEncoding = stripeFooter.getColumnEncodings().get(node);
        SortedMap<Integer, DwrfSequenceEncoding> nodeSequences = columnEncoding.getAdditionalSequenceEncodings().get();
        ImmutableMap.Builder<Long, Integer> keyToSequenceBuilder = ImmutableMap.builder();
        for (Map.Entry<Integer, DwrfSequenceEncoding> entry : nodeSequences.entrySet()) {
            long key = entry.getValue().getKey().getIntKey();
            int sequence = entry.getKey();
            keyToSequenceBuilder.put(key, sequence);
        }
        Map<Long, Integer> keyToSequence = keyToSequenceBuilder.build();

        // remove mismatching keys that are not written by the writer
        List<Long> filteredKeys = Arrays.stream(orderedKeys)
                .map(k -> (Long) k)
                .filter(k -> k >= 0 && k < 3)
                .collect(toImmutableList());

        // assert that the streams are indeed ordered
        // ordered streams come at the head of the all data streams
        Iterator<Stream> dataStreamsIterator = dataStreams.iterator();
        for (Long key : filteredKeys) {
            // there should be IN_MAP + DATA streams for each sequence, we don't care about what stream kind comes first
            int sequence = keyToSequence.get(key);
            assertEquals(dataStreamsIterator.next().getSequence(), sequence);
            assertEquals(dataStreamsIterator.next().getSequence(), sequence);
        }
    }

    private static void readFileWithIntrospector(Type type, CapturingOrcFileIntrospector introspector, TempFile tempFile)
            throws IOException
    {
        OrcDataSource dataSource = new FileOrcDataSource(tempFile.getFile(),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                true);

        OrcReaderOptions readerOptions = OrcReaderOptions.builder()
                .withMaxMergeDistance(new DataSize(1, MEGABYTE))
                .withTinyStripeThreshold(new DataSize(1, MEGABYTE))
                .withMaxBlockSize(new DataSize(1, MEGABYTE))
                .build();

        OrcReader reader = new OrcReader(
                dataSource,
                OrcEncoding.DWRF,
                new StorageOrcFileTailSource(),
                StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()),
                Optional.empty(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                readerOptions,
                false,
                DwrfEncryptionProvider.NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats(),
                Optional.of(introspector),
                tempFile.getFile().lastModified());

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
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                Optional.empty(),
                1000);
        while (recordReader.getNextPage() != null) {
            // ignore
        }
        recordReader.close();
    }

    private static StreamLayoutFactory createLongKeysStreamLayoutFactory(int column, Object[] orderedKeys)
    {
        List<DwrfProto.KeyInfo> keyInfos = Arrays.stream(orderedKeys)
                .map(key -> DwrfProto.KeyInfo.newBuilder()
                        .setIntKey((Long) key)
                        .build())
                .collect(toImmutableList());
        Map<Integer, List<DwrfProto.KeyInfo>> columnToKeys = ImmutableMap.of(column, keyInfos);

        DwrfStreamOrderingConfig streamOrderingConfig = new DwrfStreamOrderingConfig(columnToKeys);
        StreamLayoutFactory baseStreamLayoutFactory = new StreamLayoutFactory.ColumnSizeLayoutFactory();
        return () -> new StreamOrderingLayout(streamOrderingConfig, baseStreamLayoutFactory.create());
    }
}
