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

import com.facebook.presto.orc.OrcDecompressor;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Stream;
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
import static com.facebook.presto.orc.stream.AbstractTestValueStream.ORC_DATA_SOURCE_ID;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestLongStreamV1Aria
{
    private static final int COMPRESSION_BLOCK_SIZE = 256 * 1024;

    @Test
    public void testLiterals()
            throws IOException
    {
        int[] positions = IntStream.range(210, 20011).toArray();
        testWriteValueWithPositions(getData2(), SNAPPY, positions);
    }

    public void testWriteValueWithPositions(List<List<Long>> groups, CompressionKind kind, int[] positions)
            throws IOException
    {
        LongOutputStreamV1 outputStream = createOutputStream(groups, kind);

        Slice slice = getSlice(outputStream, 33);
        List<LongStreamCheckpoint> checkpoints = outputStream.getCheckpoints();

        assertEquals(checkpoints.size(), groups.size());

        LongInputStream valueStream = getInputStream(slice, kind);
        ((LongInputStreamV1) valueStream).setPositionsFilter(positions, false);
        List<Long> flattened = groups.stream()
                .flatMap(Collection::stream)
                .collect(toImmutableList());
        for (int position : positions) {
            long expectedValue = flattened.get(position);
            long actualValue = ((LongInputStreamV1) valueStream).nextLong();
            assertEquals(actualValue, expectedValue, "index=" + position);
        }
    }

    public void testWriteValue(List<List<Long>> groups, CompressionKind kind)
            throws IOException
    {
        LongOutputStreamV1 outputStream = createOutputStream(groups, kind);

        Slice slice = getSlice(outputStream, 33);
        List<LongStreamCheckpoint> checkpoints = outputStream.getCheckpoints();

        assertEquals(checkpoints.size(), groups.size());

        LongInputStream valueStream = getInputStream(slice, kind);

        for (List<Long> group : groups) {
            int index = 0;
            for (Long expectedValue : group) {
                index++;
                Long actualValue = valueStream.next();
                if (!actualValue.equals(expectedValue)) {
                    assertEquals(actualValue, expectedValue, "index=" + index);
                }
            }
        }
        for (int groupIndex = groups.size() - 1; groupIndex >= 0; groupIndex--) {
            valueStream.seekToCheckpoint(checkpoints.get(groupIndex));
            for (Long expectedValue : groups.get(groupIndex)) {
                Long actualValue = valueStream.next();
                if (!actualValue.equals(expectedValue)) {
                    assertEquals(actualValue, expectedValue);
                }
            }
        }
    }

    public LongInputStreamV1 getInputStream(Slice slice, CompressionKind kind)
            throws IOException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, kind, COMPRESSION_BLOCK_SIZE);
        OrcInputStream input = new OrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, newSimpleAggregatedMemoryContext(), slice.getRetainedSize());
        return new LongInputStreamV1(input, true);
    }

    public Slice getSlice(LongOutputStreamV1 outputStream, int column)
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

    public LongOutputStreamV1 createOutputStream(List<List<Long>> groups, CompressionKind kind)
    {
        LongOutputStreamV1 outputStream = new LongOutputStreamV1(kind, COMPRESSION_BLOCK_SIZE, true, DATA);
        outputStream.reset();
        for (List<Long> group : groups) {
            outputStream.recordCheckpoint();
            group.forEach(outputStream::writeLong);
        }
        outputStream.close();
        return outputStream;
    }

    public List<List<Long>> getRun()
    {
        List<List<Long>> groups = new ArrayList<>();
        List<Long> group;

        group = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            group.add(Long.MIN_VALUE);
        }
        groups.add(group);
        return groups;
    }

    public List<List<Long>> getLiterals()
    {
        List<List<Long>> groups = new ArrayList<>();
        List<Long> group;
        group = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            group.add(-1000L + random.nextInt(17));
        }
        groups.add(group);
        return groups;
    }

    public List<List<Long>> getData()
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
        return groups;
    }

    public static List<List<Long>> getData2()
    {
        List<List<Long>> groups = new ArrayList<>();
        List<Long> group;
        for (int j = 0; j < 4; j++) {
            group = new ArrayList<>();
            for (int i = 0; i < 10000; i++) {
                group.add((long) (i));
            }
            groups.add(group);

            group = new ArrayList<>();
            for (int i = 0; i < 5000; i++) {
                group.add((long) (10_000 + (i * 17)));
            }
            groups.add(group);

            group = new ArrayList<>();
            for (int i = 0; i < 5000; i++) {
                group.add((long) (10_000 - (i * 17)));
            }
            groups.add(group);

            group = new ArrayList<>();
            Random random = new Random(22);
            for (int i = 0; i < 30000; i++) {
                group.add(-1000L + random.nextInt(17));
            }
            groups.add(group);
        }
        return groups;
    }
}
