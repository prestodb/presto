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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ByteArrayBlockBuilder;
import com.facebook.presto.common.block.IntArrayBlockBuilder;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.block.RunLengthBlockEncoding;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.block.ShortArrayBlockBuilder;
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestRunLengthEncodedBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        for (int positionCount = 0; positionCount < 10; positionCount++) {
            assertRleBlock(positionCount);
        }
    }

    private void assertRleBlock(int positionCount)
    {
        Slice expectedValue = createExpectedValue(0);
        Block block = new RunLengthEncodedBlock(createSingleValueBlock(expectedValue), positionCount);
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = expectedValue;
        }
        assertBlock(block, TestRunLengthEncodedBlock::createBlockBuilder, expectedValues);
    }

    private static Block createSingleValueBlock(Slice expectedValue)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, expectedValue.length());
        blockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
        return blockBuilder.build();
    }

    private static BlockBuilder createBlockBuilder()
    {
        return new VariableWidthBlockBuilder(null, 1, 1);
    }

    @Test
    public void testBuildingFromLongArrayBlockBuilder()
    {
        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), RunLengthBlockEncoding.NAME);
    }

    @Test
    public void testBuildingFromIntArrayBlockBuilder()
    {
        IntArrayBlockBuilder blockBuilder = new IntArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), RunLengthBlockEncoding.NAME);
    }

    @Test
    public void testBuildingFromShortArrayBlockBuilder()
    {
        ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), RunLengthBlockEncoding.NAME);
    }

    @Test
    public void testBuildingFromByteArrayBlockBuilder()
    {
        ByteArrayBlockBuilder blockBuilder = new ByteArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), RunLengthBlockEncoding.NAME);
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        int positionCount = 10;
        Slice expectedValue = createExpectedValue(5);
        Block block = new RunLengthEncodedBlock(createSingleValueBlock(expectedValue), positionCount);
        for (int postition = 0; postition < positionCount; postition++) {
            assertEquals(block.getEstimatedDataSizeForStats(postition), expectedValue.length());
        }
    }

    private void populateNullValues(BlockBuilder blockBuilder, int positionCount)
    {
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.appendNull();
        }
    }
}
