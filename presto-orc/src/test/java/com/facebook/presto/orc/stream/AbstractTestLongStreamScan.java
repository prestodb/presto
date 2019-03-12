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
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.stream.AbstractTestLongStream.TEST_DATA;
import static com.facebook.presto.orc.stream.AbstractTestLongStream.TEST_LITERAL_DATA;
import static com.facebook.presto.orc.stream.AbstractTestLongStream.TEST_LONG_LITERAL_DATA;
import static com.facebook.presto.orc.stream.AbstractTestLongStream.TEST_RUNLENGTH_DATA;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestLongStreamScan
{
    public static final int COMPRESSION_BLOCK_SIZE = 256 * 1024;
    public static final OrcDataSourceId ORC_DATA_SOURCE_ID = new OrcDataSourceId("test");

    @Test
    public void testData()
            throws IOException
    {
        for (CompressionKind kind : CompressionKind.values()) {
            for (PositionsType positionsType : PositionsType.values()) {
                testScan(TEST_LONG_LITERAL_DATA, kind, positionsType);
                testScan(TEST_RUNLENGTH_DATA, kind, positionsType);
                testScan(TEST_DATA, kind, positionsType);
                testScan(TEST_LITERAL_DATA, kind, positionsType);
            }
        }
    }

    protected void testScan(List<List<Long>> groups, CompressionKind kind, PositionsType positionsType)
            throws IOException
    {
        List<int[]> offsetGroups = getAllPositions(groups, positionsType);
        LongOutputStream outputStream = createValueOutputStream(kind);
        for (int i = 0; i < 3; i++) {
            outputStream.reset();
            long retainedBytes = 0;
            for (List<Long> group : groups) {
                outputStream.recordCheckpoint();
                group.forEach(value -> writeValue(outputStream, value));

                assertTrue(outputStream.getRetainedBytes() >= retainedBytes);
                retainedBytes = outputStream.getRetainedBytes();
            }
            outputStream.close();

            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
            StreamDataOutput streamDataOutput = outputStream.getStreamDataOutput(33);
            streamDataOutput.writeData(sliceOutput);
            Stream stream = streamDataOutput.getStream();
            assertEquals(stream.getStreamKind(), StreamKind.DATA);
            assertEquals(stream.getColumn(), 33);
            assertEquals(stream.getLength(), sliceOutput.size());

            List<LongStreamCheckpoint> checkpoints = outputStream.getCheckpoints();
            assertEquals(checkpoints.size(), groups.size());
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            TestResultsConsumer resultsConsumer = new TestResultsConsumer(builder);
            LongInputStream valueStream = createValueStream(sliceOutput.slice(), kind);
            for (int j = 0; j < offsetGroups.size(); j++) {
                valueStream.seekToCheckpoint(checkpoints.get(j));
                valueStream.scan(offsetGroups.get(j), 0, offsetGroups.get(j).length, groups.get(j).size(), resultsConsumer);
            }
            Iterator<Long> resultsIterator = builder.build().iterator();
            for (int j = 0; j < offsetGroups.size(); j++) {
                List<Long> group = groups.get(j);
                int[] offsets = offsetGroups.get(j);
                int index = 0;
                for (int offset : offsets) {
                    long expectedValue = group.get(offset);
                    long actualValue = resultsIterator.next();
                    if (expectedValue != actualValue) {
                        System.err.println("offset=" + index);
                        assertEquals(actualValue, expectedValue, "offset=" + index);
                    }
                    index++;
                }
            }
        }
    }

    protected abstract LongOutputStream createValueOutputStream(CompressionKind kind);

    private void writeValue(LongOutputStream outputStream, long value)
    {
        outputStream.writeLong(value);
    }

    protected abstract LongInputStream createValueStream(Slice slice, CompressionKind kind)
            throws OrcCorruptionException;

    static class TestResultsConsumer
            implements LongInputStream.ResultsConsumer
    {
        private ImmutableList.Builder<Long> results;

        public TestResultsConsumer(ImmutableList.Builder<Long> results)
        {
            this.results = results;
        }

        @Override
        public boolean consume(int offsetIndex, long value)
        {
            results.add(value);
            return true;
        }

        @Override
        public int consumeRepeated(int offsetIndex, long value, int count)
        {
            for (int i = 0; i < count; i++) {
                results.add(value);
            }
            return count;
        }
    }

    enum PositionsType
    {
        ALL,
        ODD,
        SECOND_HALF,
        RANDOM_QUARTER
    }

    static List<int[]> getAllPositions(List<List<Long>> groups, PositionsType positionsType)
    {
        ImmutableList.Builder<int[]> builder = ImmutableList.builder();
        for (List<Long> group : groups) {
            builder.add(getPositions(group, positionsType));
        }
        return builder.build();
    }

    private static int[] getPositions(List<Long> data, PositionsType positionsType)
    {
        int size = data.size();
        int[] positions;
        switch (positionsType) {
            case ALL:
                positions = IntStream.range(0, size).toArray();
                break;
            case ODD:
                int oddCount = (size & 1) == 1 ? (size - 1) >> 1 : size >> 1;
                positions = new int[oddCount];
                for (int i = 0; i < oddCount; i++) {
                    positions[i] = 2 * i + 1;
                }
                break;
            case SECOND_HALF:
                int mid = size >> 1;
                positions = new int[size - mid];
                for (int i = mid; i < size; i++) {
                    positions[i - mid] = i;
                }
                break;
            case RANDOM_QUARTER:
                List<Integer> all = IntStream.range(0, size).boxed().collect(toList());
                Collections.shuffle(all, new Random(22));
                int quarterSize = all.size() >> 2;
                positions = new int[quarterSize];
                for (int i = 0; i < quarterSize; i++) {
                    positions[i] = all.get(i);
                }
                Arrays.sort(positions);
                break;
            default:
                throw new IllegalStateException("Invalid PositionsType");
        }
        return positions;
    }
}
