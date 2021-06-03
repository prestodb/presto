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

import com.facebook.presto.orc.checkpoint.StreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.reader.LongDictionaryProvider;
import com.facebook.presto.orc.reader.LongDictionaryProvider.DictionaryResult;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStreamDwrf;
import com.facebook.presto.orc.stream.LongOutputStreamDwrf;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.orc.stream.SharedBuffer;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.facebook.presto.orc.stream.ValueInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.checkpoint.Checkpoints.getDictionaryStreamCheckpoint;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.LONG;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.stream.CheckpointInputStreamSource.createCheckpointStreamSource;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestLongDictionaryProvider
{
    private static final OrcDataSourceId ORC_DATA_SOURCE_ID = new OrcDataSourceId("dict_provider_test");
    private static final DataSize COMPRESSION_BLOCK_SIZE = new DataSize(256, KILOBYTE);
    private static final OrcType LONG_TYPE = new OrcType(LONG, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty());
    private static final OrcDataSource DUMMY_ORC_DATA_SOURCE = new NoopOrcDataSource();

    @DataProvider(name = "dataForDictionaryLoadingTest")
    public Object[][] dataForDictionaryLoadingTest()
    {
        return new Object[][] {
                {
                        ImmutableMap.of(
                                new NodeId(1, 1), new long[] {1, 2, 3, 4}),
                        ImmutableList.of(new NodeId(0, 0),
                                new NodeId(2, 1),
                                new NodeId(1, 0),
                                new NodeId(1, 3),
                                new NodeId(42, 0))
                },
                {
                        ImmutableMap.of(
                                new NodeId(1, 1), new long[] {1, 2, 3, 4},
                                new NodeId(3, 1), new long[] {1, 3, 5, 7}),
                        ImmutableList.of(new NodeId(0, 0),
                                new NodeId(2, 0),
                                new NodeId(1, 0),
                                new NodeId(1, 3),
                                new NodeId(3, 0),
                                new NodeId(42, 0))
                },
                {
                        ImmutableMap.of(
                                new NodeId(1, 0), new long[] {1, 2, 3, 4},
                                new NodeId(3, 0), new long[] {1, 3, 5, 7},
                                new NodeId(4, 1), new long[] {1, 1, 2, 3},
                                new NodeId(4, 2), new long[] {2, 4, 6, 8},
                                new NodeId(4, 4), new long[] {1, 4, 9, 16}),
                        ImmutableList.of(new NodeId(2, 0),
                                new NodeId(4, 3),
                                new NodeId(4, 42),
                                new NodeId(42, 0))
                },
        };
    }

    @DataProvider(name = "dataForBufferReuseTest")
    public Object[][] dataForBufferReuseTest()
    {
        return new Object[][] {
                {new long[0], 4, false},
                {new long[4], 0, true},
                {new long[4], 4, true},
                {new long[4], 8, false},
                {new long[8], 4, true},
                {new long[16], 12, true},
                {new long[16], 16, true},
        };
    }

    @DataProvider(name = "dataForLongSharedDictionaryLoadingTest")
    private Object[][] dataForLongSharedDictionaryLoadingTest()
    {
        return new Object[][] {
                {
                        ImmutableMap.of(
                                new NodeId(1, 0), new long[] {1, 2, 3, 4}),
                        ImmutableList.of(
                                new SharedDictionaryTestInput(new NodeId(1, 0), true),
                                new SharedDictionaryTestInput(new NodeId(1, 1), true),
                                new SharedDictionaryTestInput(new NodeId(1, 3), false),
                                new SharedDictionaryTestInput(new NodeId(1, 4), false),
                                new SharedDictionaryTestInput(new NodeId(1, 9), false)),
                        ImmutableList.of(
                                new NodeId(0, 0),
                                new NodeId(2, 0),
                                new NodeId(2, 1),
                                new NodeId(42, 0))
                },
                {
                        ImmutableMap.of(
                                new NodeId(1, 0), new long[] {1, 2, 3, 4},
                                new NodeId(3, 0), new long[] {1, 3, 5, 7}),
                        ImmutableList.of(
                                new SharedDictionaryTestInput(new NodeId(1, 0), true),
                                new SharedDictionaryTestInput(new NodeId(1, 4), true),
                                new SharedDictionaryTestInput(new NodeId(1, 1), false),
                                new SharedDictionaryTestInput(new NodeId(1, 9), false),
                                new SharedDictionaryTestInput(new NodeId(3, 0), true),
                                new SharedDictionaryTestInput(new NodeId(3, 1), true),
                                new SharedDictionaryTestInput(new NodeId(3, 3), false),
                                new SharedDictionaryTestInput(new NodeId(3, 9), false)),
                        ImmutableList.of(
                                new NodeId(0, 0),
                                new NodeId(2, 0),
                                new NodeId(2, 1),
                                new NodeId(5, 1),
                                new NodeId(42, 0))
                },
                {
                        ImmutableMap.of(
                                new NodeId(1, 0), new long[] {1, 2, 3, 4},
                                new NodeId(3, 0), new long[] {1, 3, 5, 7},
                                new NodeId(4, 1), new long[] {1, 1, 2, 3},
                                new NodeId(4, 2), new long[] {2, 4, 6, 8},
                                new NodeId(4, 4), new long[] {1, 4, 9, 16}),
                        ImmutableList.of(
                                new SharedDictionaryTestInput(new NodeId(1, 0), true),
                                new SharedDictionaryTestInput(new NodeId(1, 1), true),
                                new SharedDictionaryTestInput(new NodeId(1, 4), false),
                                new SharedDictionaryTestInput(new NodeId(1, 9), false),
                                new SharedDictionaryTestInput(new NodeId(3, 0), true),
                                new SharedDictionaryTestInput(new NodeId(3, 1), true),
                                new SharedDictionaryTestInput(new NodeId(3, 3), false),
                                new SharedDictionaryTestInput(new NodeId(3, 9), false),
                                new SharedDictionaryTestInput(new NodeId(4, 1), true),
                                new SharedDictionaryTestInput(new NodeId(4, 2), true),
                                new SharedDictionaryTestInput(new NodeId(4, 4), true)),
                        ImmutableList.of(
                                new NodeId(2, 0),
                                new NodeId(2, 1),
                                new NodeId(4, 0),
                                new NodeId(4, 3),
                                new NodeId(4, 42),
                                new NodeId(42, 0))
                },
        };
    }

    @Test(dataProvider = "dataForDictionaryLoadingTest")
    public void testLongDictionaryLoading(Map<NodeId, long[]> dictionaryStreams, List<NodeId> missingNodes)
            throws Exception
    {
        TestingHiveOrcAggregatedMemoryContext aggregatedMemoryContext = new TestingHiveOrcAggregatedMemoryContext();
        LongDictionaryProvider dictionaryProvider = new LongDictionaryProvider(createLongDictionaryStreamSources(dictionaryStreams, aggregatedMemoryContext));
        for (Map.Entry<NodeId, long[]> entry : dictionaryStreams.entrySet()) {
            StreamId streamId = entry.getKey().toDictionaryDataStreamId();
            long[] data = entry.getValue();
            long[] dictionary = new long[0];
            DictionaryResult dictionaryResult = dictionaryProvider.getDictionary(createFlatStreamDescriptor(streamId), dictionary, data.length);
            long[] newDictionary = dictionaryResult.dictionaryBuffer();
            assertTrue(dictionaryResult.isBufferOwner());
            assertEquals(newDictionary, data);
        }
        for (NodeId missingNode : missingNodes) {
            long[] dictionary = new long[0];
            expectThrows(
                    OrcCorruptionException.class,
                    () -> dictionaryProvider.getDictionary(createFlatStreamDescriptor(missingNode.toDictionaryDataStreamId()), dictionary, 0));
        }
    }

    @Test(expectedExceptions = OrcCorruptionException.class, expectedExceptionsMessageRegExp = ".* Dictionary is not empty but data stream is not present.*")
    public void testDataCorruptionExceptionMessage()
            throws Exception
    {
        Map<NodeId, long[]> dictionaryStreams = ImmutableMap.of(new NodeId(1, 0), new long[] {1, 2, 3, 4});
        TestingHiveOrcAggregatedMemoryContext aggregatedMemoryContext = new TestingHiveOrcAggregatedMemoryContext();
        LongDictionaryProvider dictionaryProvider = new LongDictionaryProvider(createLongDictionaryStreamSources(dictionaryStreams, aggregatedMemoryContext));

        StreamId streamId = new NodeId(2, 0).toDictionaryDataStreamId();
        long[] dictionary = new long[0];
        dictionaryProvider.getDictionary(createFlatStreamDescriptor(streamId), dictionary, 0);
    }

    @Test(dataProvider = "dataForBufferReuseTest")
    public void testBufferReuse(long[] buffer, int items, boolean reused)
            throws IOException
    {
        NodeId nodeId = new NodeId(1, 0);
        long[] data = new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        TestingHiveOrcAggregatedMemoryContext aggregatedMemoryContext = new TestingHiveOrcAggregatedMemoryContext();
        LongDictionaryProvider dictionaryProvider = new LongDictionaryProvider(createLongDictionaryStreamSources(ImmutableMap.of(nodeId, data), aggregatedMemoryContext));
        StreamId streamId = nodeId.toDictionaryDataStreamId();
        DictionaryResult dictionaryResult = dictionaryProvider.getDictionary(createFlatStreamDescriptor(streamId), buffer, items);
        long[] newDictionary = dictionaryResult.dictionaryBuffer();
        assertTrue(dictionaryResult.isBufferOwner());
        assertEquals(newDictionary == buffer, reused);
        assertTrue(newDictionary.length >= items);
    }

    @Test(dataProvider = "dataForLongSharedDictionaryLoadingTest")
    public void testLongSharedDictionaryLoading(Map<NodeId, long[]> dictionaryStreams, List<SharedDictionaryTestInput> includedNodes, List<NodeId> excludedNodes)
            throws Exception
    {
        TestingHiveOrcAggregatedMemoryContext aggregatedMemoryContext = new TestingHiveOrcAggregatedMemoryContext();
        LongDictionaryProvider dictionaryProvider = new LongDictionaryProvider(createLongDictionaryStreamSources(dictionaryStreams, aggregatedMemoryContext));
        final int dictionaryLength = 4;
        // We can support some columns having dictionary sharing while others don't, because dictionary sharing is only
        // enabled for flatmap encoding but dictionary provider is agnostic of that. Hence, we need a map
        // that tells us whether a column has shared dictionary or not. And this map needs to also track
        // whether the shared dictionary already has an owner.
        Set<Integer> sharedColumnIds = getSharedColumnIds(dictionaryStreams);

        for (SharedDictionaryTestInput testCase : includedNodes) {
            NodeId nodeId = testCase.nodeId();
            StreamId streamId = nodeId.toDictionaryDataStreamId();
            // Getting the shared dictionary with sequence 0 doesn't create the entry.
            DictionaryResult dictionaryResult = dictionaryProvider.getDictionary(createFlatStreamDescriptor(streamId), null, dictionaryLength);
            long[] dictionary = dictionaryResult.dictionaryBuffer();
            if (sharedColumnIds.contains(nodeId.node)) {
                NodeId sharedDictionaryNodeId = new NodeId(nodeId.node, 0);
                DictionaryResult sharedDictionaryResult = dictionaryProvider.getDictionary(createFlatStreamDescriptor(sharedDictionaryNodeId.toDictionaryDataStreamId()), new long[0], dictionaryLength);
                assertEquals(testCase.isDictionaryOwner(), dictionaryResult.isBufferOwner());
                assertEquals(dictionary, sharedDictionaryResult.dictionaryBuffer());
            }
            else {
                assertEquals(dictionary, dictionaryStreams.get(nodeId));
            }
        }

        for (NodeId nodeId : excludedNodes) {
            StreamId streamId = nodeId.toDictionaryDataStreamId();
            long[] dictionary = new long[dictionaryLength];
            assertThrows(OrcCorruptionException.class, () -> dictionaryProvider.getDictionary(createFlatStreamDescriptor(streamId), dictionary, dictionaryLength));
        }
    }

    private Set<Integer> getSharedColumnIds(Map<NodeId, long[]> dictionaryStreams)
    {
        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        for (NodeId nodeId : dictionaryStreams.keySet()) {
            if (nodeId.sequence == 0) {
                builder.add(nodeId.node);
            }
        }
        return builder.build();
    }

    private StreamDescriptor createFlatStreamDescriptor(StreamId streamId)
    {
        return new StreamDescriptor("test_dictionary_stream", streamId.getColumn(),
                String.format("field_%d", streamId.getColumn()), LONG_TYPE, DUMMY_ORC_DATA_SOURCE, ImmutableList.of(), streamId.getSequence());
    }

    private InputStreamSources createLongDictionaryStreamSources(Map<NodeId, long[]> streams, OrcAggregatedMemoryContext aggregatedMemoryContext)
    {
        SharedBuffer decompressionBuffer = new SharedBuffer(aggregatedMemoryContext.newOrcLocalMemoryContext("sharedDecompressionBuffer"));
        ImmutableMap.Builder<StreamId, InputStreamSource<?>> dictionaryStreamsBuilder = ImmutableMap.builder();
        for (Map.Entry<NodeId, long[]> entry : streams.entrySet()) {
            StreamId streamId = entry.getKey().toDictionaryDataStreamId();

            DynamicSliceOutput sliceOutput = createSliceOutput(streamId, entry.getValue());

            ValueInputStream<?> valueStream = createValueStream(sliceOutput.slice(), aggregatedMemoryContext, decompressionBuffer);
            StreamCheckpoint streamCheckpoint = getDictionaryStreamCheckpoint(streamId, LONG, ColumnEncoding.ColumnEncodingKind.DICTIONARY);
            InputStreamSource<?> streamSource = createCheckpointStreamSource(valueStream, streamCheckpoint);

            dictionaryStreamsBuilder.put(streamId, streamSource);
        }
        return new InputStreamSources(dictionaryStreamsBuilder.build());
    }

    private DynamicSliceOutput createSliceOutput(StreamId streamId, long[] data)
    {
        LongOutputStreamDwrf outputStream = new LongOutputStreamDwrf(getColumnWriterOptions(), Optional.empty(), true, DICTIONARY_DATA);
        for (long val : data) {
            outputStream.writeLong(val);
        }
        outputStream.close();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
        StreamDataOutput streamDataOutput = outputStream.getStreamDataOutput(streamId.getColumn());
        streamDataOutput.writeData(sliceOutput);
        return sliceOutput;
    }

    private ColumnWriterOptions getColumnWriterOptions()
    {
        return ColumnWriterOptions.builder()
                .setCompressionKind(SNAPPY)
                .setCompressionMaxBufferSize(COMPRESSION_BLOCK_SIZE)
                .build();
    }

    private LongInputStreamDwrf createValueStream(Slice slice, OrcAggregatedMemoryContext aggregatedMemoryContext, SharedBuffer decompressionBuffer)
            throws OrcCorruptionException
    {
        OrcInputStream input = new OrcInputStream(
                ORC_DATA_SOURCE_ID,
                decompressionBuffer,
                slice.getInput(),
                getOrcDecompressor(),
                Optional.empty(),
                aggregatedMemoryContext,
                slice.getRetainedSize());
        return new LongInputStreamDwrf(input, LONG, true, true);
    }

    private Optional<OrcDecompressor> getOrcDecompressor()
    {
        return createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, toIntExact(COMPRESSION_BLOCK_SIZE.toBytes()));
    }

    private static class NodeId
    {
        private final int node;
        private final int sequence;

        public NodeId(int node, int sequence)
        {
            this.node = node;
            this.sequence = sequence;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(node, sequence);
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == this) {
                return true;
            }

            if (!(o instanceof NodeId)) {
                return false;
            }

            NodeId other = (NodeId) o;
            return other.node == node && other.sequence == sequence;
        }

        public StreamId toDictionaryDataStreamId()
        {
            return new StreamId(node, sequence, DICTIONARY_DATA);
        }
    }

    private static class SharedDictionaryTestInput
    {
        private final NodeId nodeId;
        private final boolean isDictionaryOwner;

        SharedDictionaryTestInput(NodeId nodeId, boolean isDictionaryOwner)
        {
            this.nodeId = nodeId;
            this.isDictionaryOwner = isDictionaryOwner;
        }

        NodeId nodeId()
        {
            return nodeId;
        }

        boolean isDictionaryOwner()
        {
            return isDictionaryOwner;
        }
    }
}
