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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDecompressor;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.stream.aria.AriaLongInputStreamV1;
import com.facebook.presto.orc.stream.aria.AriaOrcInputStream;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestAriaLongStreamV1
        extends AbstractTestValueStream<Long, LongStreamCheckpoint, LongOutputStreamV1, AriaLongInputStreamV1>
{
    @Test
    public void testWithPositions()
            throws IOException
    {
        List<List<Long>> data = getData();
        List<Long> flattened = data.stream()
                .flatMap(Collection::stream)
                .collect(toImmutableList());
        int[] positions = IntStream.range(flattened.size() / 11, flattened.size()).toArray();
        for (CompressionKind kind : CompressionKind.values()) {
            testWriteValueWithPositions(getData(), kind, positions);
        }
    }

    public void testWriteValueWithPositions(List<List<Long>> groups, CompressionKind compressionKind, int[] positions)
            throws IOException
    {
        LongOutputStreamV1 outputStream = createValueOutputStream(groups, compressionKind);
        List<LongStreamCheckpoint> checkpoints = outputStream.getCheckpoints();
        assertEquals(checkpoints.size(), groups.size());
        Slice slice = createSlice(outputStream, 33);
        AriaLongInputStreamV1 valueStream = createValueStream(slice, compressionKind);
        valueStream.setPositions(positions);
        List<Long> flattened = groups.stream()
                .flatMap(Collection::stream)
                .collect(toImmutableList());
        for (int position : positions) {
            long expectedValue = flattened.get(position);
            long actualValue = valueStream.nextLong();
            assertEquals(actualValue, expectedValue, "index=" + position);
        }
    }
    @Test
    public void test()
            throws IOException
    {
        List<List<Long>> groups = new ArrayList<>();
        List<Long> group;

        group = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            group.add((long) (i));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            group.add((long) (10_000 + (i * 17)));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            group.add((long) (10_000 - (i * 17)));
        }
        groups.add(group);

        group = new ArrayList<>();
        Random random = new Random(22);
        for (int i = 0; i < 1000; i++) {
            group.add(-1000L + random.nextInt(17));
        }
        groups.add(group);

        testWriteValue(groups);
    }

    private LongOutputStreamV1 createValueOutputStream(List<List<Long>> groups, CompressionKind compressionKind)
    {
        LongOutputStreamV1 outputStream = new LongOutputStreamV1(compressionKind, COMPRESSION_BLOCK_SIZE, true, DATA);
        outputStream.reset();
        for (List<Long> group : groups) {
            outputStream.recordCheckpoint();
            group.forEach(outputStream::writeLong);
        }
        outputStream.close();
        return outputStream;
    }

    private Slice createSlice(LongOutputStreamV1 outputStream, int column)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
        StreamDataOutput streamDataOutput = outputStream.getStreamDataOutput(column);
        streamDataOutput.writeData(sliceOutput);
        Stream stream = streamDataOutput.getStream();
        assertEquals(stream.getStreamKind(), Stream.StreamKind.DATA);
        assertEquals(stream.getColumn(), column);
        assertEquals(stream.getLength(), sliceOutput.size());
        return sliceOutput.slice();
    }

    private AriaLongInputStreamV1 createValueStream(Slice slice, CompressionKind compressionKind)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, compressionKind, COMPRESSION_BLOCK_SIZE);
        AriaOrcInputStream input = new AriaOrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, newSimpleAggregatedMemoryContext(), slice.getRetainedSize());
        return new AriaLongInputStreamV1(input, true);
    }

    private List<List<Long>> getData()
    {
        List<List<Long>> groups = new ArrayList<>();
        List<Long> group;

        group = new ArrayList<>();
        Random random = new Random(22);
        for (int i = 0; i < 470000; i++) {
            group.add(-1000L + random.nextInt(17));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            group.add((long) (i));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            group.add((long) (10_000 + (i * 17)));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            group.add((long) (10_000 - (i * 17)));
        }
        groups.add(group);
        return groups;
    }

    @Override
    protected LongOutputStreamV1 createValueOutputStream()
    {
        return new LongOutputStreamV1(SNAPPY, COMPRESSION_BLOCK_SIZE, true, DATA);
    }

    @Override
    protected void writeValue(LongOutputStreamV1 outputStream, Long value)
    {
        outputStream.writeLong(value);
    }

    @Override
    protected AriaLongInputStreamV1 createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, COMPRESSION_BLOCK_SIZE);
        AriaOrcInputStream input = new AriaOrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, newSimpleAggregatedMemoryContext(), slice.getRetainedSize());
        return new AriaLongInputStreamV1(input, true);
    }

    @Override
    protected Long readValue(AriaLongInputStreamV1 valueStream)
            throws IOException
    {
        return valueStream.next();
    }
}
