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
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.OutputSupplier;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockBuilders.createBlockBuilder;
import static com.facebook.presto.serde.BlocksFileReader.readBlocks;
import static com.facebook.presto.serde.TestingBlockEncodingManager.createTestingBlockEncodingManager;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static org.testng.Assert.assertEquals;

public class TestFileBlocksSerde
{
    private final List<String> expectedValues = ImmutableList.of(
            "alice",
            "bob",
            "charlie",
            "dave",
            "alice",
            "bob",
            "charlie",
            "dave",
            "alice",
            "bob",
            "charlie",
            "dave");

    private final Block expectedBlock = createBlockBuilder(SINGLE_VARBINARY)
            .append("alice")
            .append("bob")
            .append("charlie")
            .append("dave")
            .build();

    @Test
    public void testRoundTrip()
    {
        for (BlocksFileEncoding encoding : BlocksFileEncoding.values()) {
            try {
                testRoundTrip(encoding);
            }
            catch (Throwable e) {
                throw new RuntimeException("Round trip failed for encoding: " + encoding, e);
            }
        }
    }

    public void testRoundTrip(BlocksFileEncoding encoding)
    {
        DynamicSliceOutputSupplier sliceOutput = new DynamicSliceOutputSupplier(1024);

        // write 3 copies the expected block
        BlocksFileWriter fileWriter = new BlocksFileWriter(createTestingBlockEncodingManager(), encoding, sliceOutput);
        fileWriter.append(expectedBlock);
        fileWriter.append(expectedBlock);
        fileWriter.append(expectedBlock);
        fileWriter.close();

        // read the block
        Slice slice = sliceOutput.getLastSlice();
        BlocksFileReader actualBlocks = readBlocks(createTestingBlockEncodingManager(), slice);

        List<Object> actualValues = BlockAssertions.toValues(actualBlocks);

        assertEquals(actualValues, expectedValues);

        BlocksFileStats stats = actualBlocks.getStats();
        assertEquals(stats.getAvgRunLength(), 1);
        assertEquals(stats.getRowCount(), 12);
        assertEquals(stats.getRunsCount(), 12);
        assertEquals(stats.getUniqueCount(), 4);
    }

    private static class DynamicSliceOutputSupplier
            implements OutputSupplier<DynamicSliceOutput>
    {
        private final int estimatedSize;
        private DynamicSliceOutput lastOutput;

        public DynamicSliceOutputSupplier(int estimatedSize)
        {
            this.estimatedSize = estimatedSize;
        }

        public Slice getLastSlice()
        {
            return lastOutput.slice();
        }

        @Override
        public DynamicSliceOutput getOutput()
        {
            lastOutput = new DynamicSliceOutput(estimatedSize);
            return lastOutput;
        }
    }
}
