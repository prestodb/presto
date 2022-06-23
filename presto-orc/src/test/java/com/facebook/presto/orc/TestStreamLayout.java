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

import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.facebook.presto.orc.writer.StreamLayout.ByColumnSize;
import com.facebook.presto.orc.writer.StreamLayout.ByStreamSize;
import com.facebook.presto.orc.writer.StreamOrderingLayout;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import io.airlift.slice.Slices;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
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

    private static void verifyStream(Stream stream, int nodeId, int seqId, StreamKind streamKind, int length)
    {
        assertEquals(stream.getColumn(), nodeId);
        assertEquals(stream.getSequence(), seqId);
        assertEquals(stream.getLength(), length);
        assertEquals(stream.getStreamKind(), streamKind);
    }

    @Test
    public void testByStreamSize()
    {
        List<StreamDataOutput> streams = new ArrayList<>();
        int length = 10_000;
        for (int i = 0; i < 10; i++) {
            streams.add(createStream(i, StreamKind.PRESENT, length - i));
            streams.add(createStream(i, StreamKind.DATA, length - 100 - i));
        }

        Collections.shuffle(streams);

        new ByStreamSize().reorder(streams);

        assertEquals(streams.size(), 20);
        Iterator<StreamDataOutput> iterator = streams.iterator();
        for (int i = 9; i >= 0; i--) {
            verifyStream(iterator.next().getStream(), i, StreamKind.DATA, length - 100 - i);
        }

        for (int i = 9; i >= 0; i--) {
            verifyStream(iterator.next().getStream(), i, StreamKind.PRESENT, length - i);
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testByColumnSize()
    {
        // Assume the file has 3 streams
        // 1st Column( 1010), Data(1000), Present(10)
        // 2nd column (1010), Dictionary (300), Present (10), Data(600), Length(100)
        // 3rd Column > 2GB

        List<StreamDataOutput> streams = new ArrayList<>();
        streams.add(createStream(1, StreamKind.DATA, 1_000));
        streams.add(createStream(1, StreamKind.PRESENT, 10));

        streams.add(createStream(2, StreamKind.DICTIONARY_DATA, 300));
        streams.add(createStream(2, StreamKind.PRESENT, 10));
        streams.add(createStream(2, StreamKind.DATA, 600));
        streams.add(createStream(2, StreamKind.LENGTH, 100));

        streams.add(createStream(3, StreamKind.DATA, Integer.MAX_VALUE));
        streams.add(createStream(3, StreamKind.PRESENT, Integer.MAX_VALUE));

        Collections.shuffle(streams);
        new ByColumnSize().reorder(streams);

        Iterator<StreamDataOutput> iterator = streams.iterator();
        verifyStream(iterator.next().getStream(), 1, StreamKind.PRESENT, 10);
        verifyStream(iterator.next().getStream(), 1, StreamKind.DATA, 1000);

        verifyStream(iterator.next().getStream(), 2, StreamKind.PRESENT, 10);
        verifyStream(iterator.next().getStream(), 2, StreamKind.LENGTH, 100);
        verifyStream(iterator.next().getStream(), 2, StreamKind.DICTIONARY_DATA, 300);
        verifyStream(iterator.next().getStream(), 2, StreamKind.DATA, 600);

        verifyStream(iterator.next().getStream(), 3, StreamKind.PRESENT, Integer.MAX_VALUE);
        verifyStream(iterator.next().getStream(), 3, StreamKind.DATA, Integer.MAX_VALUE);

        assertFalse(iterator.hasNext());
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
        verifyStream(iterator.next().getStream(), 5, 0, StreamKind.DATA, 1);
        verifyStream(iterator.next().getStream(), 2, 0, StreamKind.LENGTH, 2);
        verifyStream(iterator.next().getStream(), 1, 0, StreamKind.DATA, 3);
        verifyStream(iterator.next().getStream(), 4, 0, StreamKind.LENGTH, 5);
        verifyStream(iterator.next().getStream(), 3, 0, StreamKind.DATA, 8);
        verifyStream(iterator.next().getStream(), 1, 0, StreamKind.PRESENT, 12);
        if (!isEmptyMap) {
            // flat map stream not reordered
            verifyStream(iterator.next().getStream(), 11, 5, StreamKind.IN_MAP, 13);
            verifyStream(iterator.next().getStream(), 11, 5, StreamKind.LENGTH, 14);
            verifyStream(iterator.next().getStream(), 12, 5, StreamKind.DATA, 15);
        }
        assertFalse(iterator.hasNext());
    }

    @Test(dataProvider = "testParams")
    public void testByColumnSizeStreamOrdering(boolean isEmptyMap)
    {
        List<StreamDataOutput> streams = createStreams(isEmptyMap);
        ByColumnSize streamLayout = new ByColumnSize();
        StreamOrderingLayout streamOrderingLayout = new StreamOrderingLayout(createStreamReorderingInput(), streamLayout);
        streamOrderingLayout.reorder(streams, createNodeIdToColumnId(), createColumnEncodings(isEmptyMap));

        Iterator<StreamDataOutput> iterator = streams.iterator();
        if (!isEmptyMap) {
            verifyFlatMapColumns(iterator);
        }
        // non flat map columns
        verifyStream(iterator.next().getStream(), 5, 0, StreamKind.DATA, 1);
        verifyStream(iterator.next().getStream(), 2, 0, StreamKind.LENGTH, 2);
        verifyStream(iterator.next().getStream(), 4, 0, StreamKind.LENGTH, 5);
        verifyStream(iterator.next().getStream(), 3, 0, StreamKind.DATA, 8);
        verifyStream(iterator.next().getStream(), 1, 0, StreamKind.DATA, 3);
        verifyStream(iterator.next().getStream(), 1, 0, StreamKind.PRESENT, 12);
        if (!isEmptyMap) {
            // flat map stream not reordered
            verifyStream(iterator.next().getStream(), 12, 5, StreamKind.DATA, 15);
            verifyStream(iterator.next().getStream(), 11, 5, StreamKind.IN_MAP, 13);
            verifyStream(iterator.next().getStream(), 11, 5, StreamKind.LENGTH, 14);
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
        // column 2
        verifyStream(iterator.next().getStream(), 8, 3, StreamKind.IN_MAP, 6);
        verifyStream(iterator.next().getStream(), 8, 3, StreamKind.DATA, 7);
        verifyStream(iterator.next().getStream(), 8, 2, StreamKind.IN_MAP, 4);
        verifyStream(iterator.next().getStream(), 8, 2, StreamKind.DATA, 5);
        verifyStream(iterator.next().getStream(), 8, 1, StreamKind.IN_MAP, 2);
        verifyStream(iterator.next().getStream(), 8, 1, StreamKind.DATA, 3);

        // column 3
        verifyStream(iterator.next().getStream(), 11, 1, StreamKind.IN_MAP, 1);
        verifyStream(iterator.next().getStream(), 11, 1, StreamKind.LENGTH, 2);
        verifyStream(iterator.next().getStream(), 12, 1, StreamKind.DATA, 3);

        verifyStream(iterator.next().getStream(), 11, 2, StreamKind.IN_MAP, 4);
        verifyStream(iterator.next().getStream(), 11, 2, StreamKind.LENGTH, 5);
        verifyStream(iterator.next().getStream(), 12, 2, StreamKind.DATA, 6);

        verifyStream(iterator.next().getStream(), 11, 4, StreamKind.IN_MAP, 10);
        verifyStream(iterator.next().getStream(), 11, 4, StreamKind.LENGTH, 11);
        verifyStream(iterator.next().getStream(), 12, 4, StreamKind.DATA, 12);

        verifyStream(iterator.next().getStream(), 11, 3, StreamKind.IN_MAP, 7);
        verifyStream(iterator.next().getStream(), 11, 3, StreamKind.LENGTH, 8);
        verifyStream(iterator.next().getStream(), 12, 3, StreamKind.DATA, 9);
    }

    private static List<StreamDataOutput> createStreams(boolean isEmptyMap)
    {
        // Assume the file has following schema
        // column 0: INT (Node 0)
        // column 1: MAP<INT, LIST<INT>> // non flat map
        // column 2: MAP<INT, FLOAT> // flat map
        // column 3: MAP<INT, LIST<INT> // flat map

        List<StreamDataOutput> streams = new ArrayList<>();
        // column 0
        streams.add(createStream(1, StreamKind.DATA, 3));
        streams.add(createStream(1, StreamKind.PRESENT, 12));

        // column 1 MAP<INT, LIST<INT>> <2, <3, 4<5>>>>
        streams.add(createStream(2, StreamKind.LENGTH, 2)); // MAP
        streams.add(createStream(3, StreamKind.DATA, 8)); // INT
        streams.add(createStream(4, StreamKind.LENGTH, 5)); // LIST<INT>
        streams.add(createStream(5, StreamKind.DATA, 1)); // INT

        if (!isEmptyMap) {
            // column 2 MAP<INT, FLOAT> <6 <7, 8>>
            streams.add(createStream(8, 1, StreamKind.IN_MAP, 2));
            streams.add(createStream(8, 1, StreamKind.DATA, 3));
            streams.add(createStream(8, 2, StreamKind.IN_MAP, 4));
            streams.add(createStream(8, 2, StreamKind.DATA, 5));
            streams.add(createStream(8, 3, StreamKind.IN_MAP, 6));
            streams.add(createStream(8, 3, StreamKind.DATA, 7));

            // column 3 MAP<INT, LIST<INT> <9 <10, 11<12>>>
            streams.add(createStream(11, 1, StreamKind.IN_MAP, 1));
            streams.add(createStream(11, 1, StreamKind.LENGTH, 2));
            streams.add(createStream(12, 1, StreamKind.DATA, 3));

            streams.add(createStream(11, 2, StreamKind.IN_MAP, 4));
            streams.add(createStream(11, 2, StreamKind.LENGTH, 5));
            streams.add(createStream(12, 2, StreamKind.DATA, 6));

            streams.add(createStream(11, 3, StreamKind.IN_MAP, 7));
            streams.add(createStream(11, 3, StreamKind.LENGTH, 8));
            streams.add(createStream(12, 3, StreamKind.DATA, 9));

            streams.add(createStream(11, 4, StreamKind.IN_MAP, 10));
            streams.add(createStream(11, 4, StreamKind.LENGTH, 11));
            streams.add(createStream(12, 4, StreamKind.DATA, 12));

            streams.add(createStream(11, 5, StreamKind.IN_MAP, 13));
            streams.add(createStream(11, 5, StreamKind.LENGTH, 14));
            streams.add(createStream(12, 5, StreamKind.DATA, 15));
        }
        return streams;
    }
}
