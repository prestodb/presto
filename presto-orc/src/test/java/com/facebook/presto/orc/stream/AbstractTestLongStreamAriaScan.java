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
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestLongStreamAriaScan
{
    static final int COMPRESSION_BLOCK_SIZE = 256 * 1024;
    static final OrcDataSourceId ORC_DATA_SOURCE_ID = new OrcDataSourceId("test");

    private static final List<List<Long>> TEST_DATA = getTestData();
    private static final List<List<Long>> TEST_RUNLENGTH_DATA = getRunlengthTestData();

    @Test
    public void testData()
            throws IOException
    {
        testScan(TEST_DATA);
        testScan(TEST_RUNLENGTH_DATA);
    }

    protected void testScan(List<List<Long>> groups)
            throws IOException
    {
        List<int[]> offsetGroups = getOddOffsets(groups);
        LongOutputStream outputStream = createValueOutputStream();
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
            LongInputStream valueStream = createValueStream(sliceOutput.slice());
            for (int j = 0; j < offsetGroups.size(); j++) {
                valueStream.seekToCheckpoint(checkpoints.get(j));
                valueStream.scan(offsetGroups.get(j), 0, offsetGroups.get(j).length, groups.get(j).size(), resultsConsumer);
            }
            Iterator<Long> resultsIterator = builder.build().iterator();
            for (int j = 0; j < offsetGroups.size(); j++) {
                List<Long> group = groups.get(j);
                int[] offsets = offsetGroups.get(j);
                for (int offset : offsets) {
                    assertEquals(resultsIterator.next(), group.get(offset));
                }
            }
        }
    }

    protected abstract LongOutputStream createValueOutputStream();

    private void writeValue(LongOutputStream outputStream, long value)
    {
        outputStream.writeLong(value);
    }

    protected abstract LongInputStream createValueStream(Slice slice)
            throws OrcCorruptionException;

    private static class TestResultsConsumer
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

    private List<int[]> getOddOffsets(List<List<Long>> groups)
    {
        ImmutableList.Builder<int[]> builder = ImmutableList.builderWithExpectedSize(groups.size());
        for (List<?> group : groups) {
            int[] offsets = new int[group.size() >> 1];
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = (i << 1) + 1;
            }
            builder.add(offsets);
        }
        return builder.build();
    }

    private static List<List<Long>> getTestData()
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

    private static List<List<Long>> getRunlengthTestData()
    {
        List<List<Long>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Long> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((long) (groupIndex * 10_000 + i));
            }
            groups.add(group);
        }
        return groups;
    }
}
