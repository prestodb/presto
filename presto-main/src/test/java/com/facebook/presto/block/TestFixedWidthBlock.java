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
package com.facebook.presto.block;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.FixedWidthBlock;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFixedWidthBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        for (int fixedSize = 0; fixedSize < 20; fixedSize++) {
            Slice[] expectedValues = createExpectedValues(17, fixedSize);
            assertFixedWithValues(expectedValues, fixedSize);
            assertFixedWithValues((Slice[]) alternatingNullValues(expectedValues), fixedSize);
        }
    }

    @Test
    public void testCopyPositions()
    {
        for (int fixedSize = 0; fixedSize < 20; fixedSize++) {
            Slice[] expectedValues = (Slice[]) alternatingNullValues(createExpectedValues(17, fixedSize));
            BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues, fixedSize);
            assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 2, 4, 6, 7, 9, 10, 16);
        }
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        for (int fixedSize = 0; fixedSize < 20; fixedSize++) {
            Slice[] expectedValues = (Slice[]) alternatingNullValues(createExpectedValues(17, fixedSize));
            BlockBuilder emptyBlockBuilder = new FixedWidthBlockBuilder(fixedSize, null, 0);

            BlockBuilder blockBuilder = new FixedWidthBlockBuilder(fixedSize, null, expectedValues.length);
            assertEquals(blockBuilder.getSizeInBytes(), emptyBlockBuilder.getSizeInBytes());
            assertEquals(blockBuilder.getRetainedSizeInBytes(), emptyBlockBuilder.getRetainedSizeInBytes());

            writeValues(expectedValues, blockBuilder);
            assertTrue(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes());
            assertTrue(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes());

            blockBuilder = blockBuilder.newBlockBuilderLike(null);
            assertEquals(blockBuilder.getSizeInBytes(), emptyBlockBuilder.getSizeInBytes());
            assertEquals(blockBuilder.getRetainedSizeInBytes(), emptyBlockBuilder.getRetainedSizeInBytes());
        }
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        for (int fixedSize = 0; fixedSize < 20; fixedSize++) {
            Slice[] expectedValues = (Slice[]) alternatingNullValues(createExpectedValues(17, fixedSize));
            BlockBuilder blockBuilder = new FixedWidthBlockBuilder(fixedSize, null, expectedValues.length);
            writeValues(expectedValues, blockBuilder);
            assertEstimatedDataSizeForStats(blockBuilder, expectedValues);
        }
    }

    @Test
    public void testCompactBlock()
    {
        Slice compactSlice = Slices.copyOf(createExpectedValue(24));
        Slice incompactSlice = Slices.copyOf(createExpectedValue(30)).slice(0, 24);
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(new FixedWidthBlock(4, 0, EMPTY_SLICE, Optional.empty()));
        testCompactBlock(new FixedWidthBlock(4, valueIsNull.length, compactSlice, Optional.of(Slices.wrappedBooleanArray(valueIsNull))));

        testIncompactBlock(new FixedWidthBlock(4, valueIsNull.length - 1, compactSlice, Optional.of(Slices.wrappedBooleanArray(valueIsNull))));
        // underlying slice is not compact
        testIncompactBlock(new FixedWidthBlock(4, valueIsNull.length, incompactSlice, Optional.of(Slices.wrappedBooleanArray(valueIsNull))));
    }

    private void assertFixedWithValues(Slice[] expectedValues, int fixedSize)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues, fixedSize);
        assertBlock(blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlock(blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues, int fixedSize)
    {
        FixedWidthBlockBuilder blockBuilder = new FixedWidthBlockBuilder(fixedSize, expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static void writeValues(Slice[] expectedValues, BlockBuilder blockBuilder)
    {
        for (Slice expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
            }
        }
    }

    private static Slice[] createExpectedValues(int positionCount, int fixedSize)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(fixedSize);
        }
        return expectedValues;
    }
}
