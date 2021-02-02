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
import com.facebook.presto.orc.TestingHiveOrcAggregatedMemoryContext;
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static org.testng.Assert.assertEquals;

public class TestBooleanStream
        extends AbstractTestValueStream<Boolean, BooleanStreamCheckpoint, BooleanOutputStream, BooleanInputStream>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Boolean>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Boolean> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add(i % 3 == 0);
            }
            groups.add(group);
        }
        List<Boolean> group = new ArrayList<>();
        for (int i = 0; i < 17; i++) {
            group.add(i % 3 == 0);
        }
        groups.add(group);
        testWriteValue(groups);
    }

    @Test
    public void testGetSetBits()
            throws IOException
    {
        BooleanOutputStream outputStream = createValueOutputStream();
        for (int i = 0; i < 100; i++) {
            outputStream.writeBoolean(true);
            outputStream.writeBoolean(false);
        }
        outputStream.close();

        BooleanInputStream valueStream = createValueStream(outputStream);
        // 0 left
        assertAlternatingValues(valueStream, 7, true);
        // 1 left
        assertAlternatingValues(valueStream, 7, false);
        // 2 left
        assertAlternatingValues(valueStream, 7, true);
        // 3 left
        assertAlternatingValues(valueStream, 7, false);
        // 4 left
        assertAlternatingValues(valueStream, 7, true);
        // 5 left
        assertAlternatingValues(valueStream, 7, false);
        // 6 left
        assertAlternatingValues(valueStream, 7, true);
        // 7 left
        assertAlternatingValues(valueStream, 7, false);
        // 0 left
        assertAlternatingValues(valueStream, 15, true);
        // 1 left
        assertAlternatingValues(valueStream, 10, false);
    }

    private void assertAlternatingValues(BooleanInputStream valueStream, int batchSize, boolean expectedFirstValue)
            throws IOException
    {
        boolean[] data = new boolean[batchSize];
        int setBits = valueStream.getSetBits(batchSize, data);
        assertEquals(setBits, (int) (expectedFirstValue ? Math.ceil(batchSize / 2.0) : Math.floor(batchSize / 2.0)));

        boolean expectedValue = expectedFirstValue;
        for (int i = 0; i < batchSize; i++) {
            assertEquals(data[i], expectedValue);
            expectedValue = !expectedValue;
        }
    }

    @Test
    public void testWriteMultiple()
            throws IOException
    {
        BooleanOutputStream outputStream = createValueOutputStream();
        for (int i = 0; i < 3; i++) {
            outputStream.reset();

            BooleanList expectedValues = new BooleanArrayList(1024);
            outputStream.writeBooleans(32, true);
            expectedValues.addAll(Collections.nCopies(32, true));
            outputStream.writeBooleans(32, false);
            expectedValues.addAll(Collections.nCopies(32, false));

            outputStream.writeBooleans(1, true);
            expectedValues.add(true);
            outputStream.writeBooleans(1, false);
            expectedValues.add(false);

            outputStream.writeBooleans(34, true);
            expectedValues.addAll(Collections.nCopies(34, true));
            outputStream.writeBooleans(34, false);
            expectedValues.addAll(Collections.nCopies(34, false));

            outputStream.writeBoolean(true);
            expectedValues.add(true);
            outputStream.writeBoolean(false);
            expectedValues.add(false);

            outputStream.close();

            BooleanInputStream valueStream = createValueStream(outputStream);
            for (int index = 0; index < expectedValues.size(); index++) {
                boolean expectedValue = expectedValues.getBoolean(index);
                boolean actualValue = readValue(valueStream);
                assertEquals(actualValue, expectedValue);
            }
        }
    }

    @Override
    protected BooleanOutputStream createValueOutputStream()
    {
        CompressionParameters compressionParameters = new CompressionParameters(
                SNAPPY,
                OptionalInt.empty(),
                COMPRESSION_BLOCK_SIZE);
        return new BooleanOutputStream(compressionParameters, Optional.empty());
    }

    @Override
    protected void writeValue(BooleanOutputStream outputStream, Boolean value)
    {
        outputStream.writeBoolean(value);
    }

    @Override
    protected BooleanInputStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, COMPRESSION_BLOCK_SIZE);
        TestingHiveOrcAggregatedMemoryContext aggregatedMemoryContext = new TestingHiveOrcAggregatedMemoryContext();
        return new BooleanInputStream(new OrcInputStream(
                ORC_DATA_SOURCE_ID,
                new SharedBuffer(aggregatedMemoryContext.newOrcLocalMemoryContext("sharedDecompressionBuffer")),
                slice.getInput(),
                orcDecompressor,
                Optional.empty(),
                aggregatedMemoryContext,
                slice.getRetainedSize()));
    }

    @Override
    protected Boolean readValue(BooleanInputStream valueStream)
            throws IOException
    {
        return valueStream.nextBit();
    }

    private BooleanInputStream createValueStream(BooleanOutputStream outputStream)
            throws OrcCorruptionException
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
        StreamDataOutput streamDataOutput = outputStream.getStreamDataOutput(33);
        streamDataOutput.writeData(sliceOutput);
        Stream stream = streamDataOutput.getStream();
        assertEquals(stream.getStreamKind(), StreamKind.DATA);
        assertEquals(stream.getColumn(), 33);
        assertEquals(stream.getLength(), sliceOutput.size());

        return createValueStream(sliceOutput.slice());
    }
}
