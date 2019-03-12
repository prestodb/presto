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
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestLongStream
{
    public static final int COMPRESSION_BLOCK_SIZE = 256 * 1024;
    public static final OrcDataSourceId ORC_DATA_SOURCE_ID = new OrcDataSourceId("test");

    static final List<List<Long>> TEST_DATA = getTestData();
    static final List<List<Long>> TEST_RUNLENGTH_DATA = getRunlengthTestData();
    static final List<List<Long>> TEST_LITERAL_DATA = getLiteralTestData();
    static final List<List<Long>> TEST_LONG_LITERAL_DATA = getLargeLiteralTestData();

    @Test
    public void testData()
            throws IOException
    {
        for (CompressionKind kind : CompressionKind.values()) {
            testScan(TEST_LONG_LITERAL_DATA, kind);
            testScan(TEST_RUNLENGTH_DATA, kind);
            testScan(TEST_DATA, kind);
            testScan(TEST_LITERAL_DATA, kind);
        }
    }

    protected void testScan(List<List<Long>> groups, CompressionKind kind)
            throws IOException
    {
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
            LongInputStream valueStream = createValueStream(sliceOutput.slice(), kind);

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
    }

    protected abstract LongOutputStream createValueOutputStream(CompressionKind kind);

    protected abstract LongInputStream createValueStream(Slice slice, CompressionKind kind)
            throws OrcCorruptionException;

    private void writeValue(LongOutputStream outputStream, long value)
    {
        outputStream.writeLong(value);
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

    private static List<List<Long>> getLiteralTestData()
    {
        List<List<Long>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Long> group = new ArrayList<>();
            long value = groupIndex * 10_000;
            for (int i = 0; i < 40; i++) {
                group.add(value);
                value += 2 * i + 1;
            }
            groups.add(group);
        }
        return groups;
    }

    private static List<List<Long>> getLargeLiteralTestData()
    {
        List<List<Long>> groups = new ArrayList<>();
        List<Long> group = new ArrayList<>();
        long base = 5_900_000_000_000L;
        long value = 0;
        for (int i = 0; i < 50000; i++) {
            group.add(base + value);
            value += 2 * i + 1;
        }
        groups.add(group);
        return groups;
    }
}
