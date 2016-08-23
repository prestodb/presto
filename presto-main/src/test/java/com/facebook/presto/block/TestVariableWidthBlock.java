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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Arrays.copyOfRange;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestVariableWidthBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Slice[] expectedValues = createExpectedValues(100);
        assertVariableWithValues(expectedValues);
        assertVariableWithValues((Slice[]) alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyRegion()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(100);
        Block block = createBlockBuilderWithValues(expectedValues).build();
        Block actual = block.copyRegion(10, 10);
        Block expected = createBlockBuilderWithValues(copyOfRange(expectedValues, 10, 20)).build();
        assertEquals(actual.getPositionCount(), expected.getPositionCount());
        assertEquals(actual.getSizeInBytes(), expected.getSizeInBytes());
    }

    @Test
    public void testCopyPositions()
            throws Exception
    {
        Slice[] expectedValues = (Slice[]) alternatingNullValues(createExpectedValues(100));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(0, 2, 4, 6, 7, 9, 10, 16));
    }

    @Test
    private void testGetSizeInBytes()
    {
        int numEntries = 1000;
        VarcharType unboundedVarcharType = createUnboundedVarcharType();
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), numEntries, 20);
        for (int i = 0; i < numEntries; i++) {
            unboundedVarcharType.writeString(blockBuilder, String.valueOf(ThreadLocalRandom.current().nextLong()));
        }
        Block block = blockBuilder.build();

        Block half1 = block.getRegion(0, numEntries / 2);
        Block half2 = block.getRegion(numEntries / 2, numEntries / 2);
        Block quarter1 = half1.getRegion(0, numEntries / 4);
        Block quarter2 = half1.getRegion(numEntries / 4, numEntries / 4);
        Block quarter3 = half2.getRegion(0, numEntries / 4);
        Block quarter4 = half2.getRegion(numEntries / 4, numEntries / 4);

        int sizeInBytes = block.getSizeInBytes();
        int quarter1size = quarter1.getSizeInBytes();
        int quarter2size = quarter2.getSizeInBytes();
        int quarter3size = quarter3.getSizeInBytes();
        int quarter4size = quarter4.getSizeInBytes();
        double expectedQuarterSizeMin = sizeInBytes * 0.2;
        double expectedQuarterSizeMax = sizeInBytes * 0.3;
        assertTrue(quarter1size > expectedQuarterSizeMin && quarter1size < expectedQuarterSizeMax, format("quarter1size is %s, should be between %s and %s", quarter1size, expectedQuarterSizeMin, expectedQuarterSizeMax));
        assertTrue(quarter2size > expectedQuarterSizeMin && quarter2size < expectedQuarterSizeMax, format("quarter2size is %s, should be between %s and %s", quarter2size, expectedQuarterSizeMin, expectedQuarterSizeMax));
        assertTrue(quarter3size > expectedQuarterSizeMin && quarter3size < expectedQuarterSizeMax, format("quarter3size is %s, should be between %s and %s", quarter3size, expectedQuarterSizeMin, expectedQuarterSizeMax));
        assertTrue(quarter4size > expectedQuarterSizeMin && quarter4size < expectedQuarterSizeMax, format("quarter4size is %s, should be between %s and %s", quarter4size, expectedQuarterSizeMin, expectedQuarterSizeMax));
        assertEquals(quarter1size + quarter2size + quarter3size + quarter4size, sizeInBytes);
    }

    private void assertVariableWithValues(Slice[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues)
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), expectedValues.length, 32);
        for (Slice expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
            }
        }
        return blockBuilder;
    }
}
