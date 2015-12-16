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
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static java.util.Arrays.copyOfRange;
import static org.testng.Assert.assertEquals;

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
