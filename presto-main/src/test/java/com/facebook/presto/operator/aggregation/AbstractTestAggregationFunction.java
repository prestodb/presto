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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.tuple.TupleInfo;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;

public abstract class AbstractTestAggregationFunction
{
    public abstract Block getSequenceBlock(int start, int length);

    public abstract AggregationFunction getFunction();

    public abstract Object getExpectedValue(int start, int length);

    public double getConfidence()
    {
        return 1.0;
    }

    public Object getExpectedValueIncludingNulls(int start, int length, int lengthIncludingNulls)
    {
        return getExpectedValue(start, length);
    }

    @Test
    public void testNoPositions()
    {
        testAggregation(getExpectedValue(0, 0), getSequenceBlock(0, 0));
    }

    @Test
    public void testSinglePosition()
    {
        testAggregation(getExpectedValue(0, 1), getSequenceBlock(0, 1));
    }

    @Test
    public void testMultiplePositions()
    {
        testAggregation(getExpectedValue(0, 5), getSequenceBlock(0, 5));
    }

    @Test
    public void testAllPositionsNull()
            throws Exception
    {
        TupleInfo tupleInfo = getSequenceBlock(0, 10).getTupleInfo();
        RandomAccessBlock nullValueBlock = new BlockBuilder(tupleInfo)
                .appendNull()
                .build()
                .toRandomAccessBlock();

        Block block = new RunLengthEncodedBlock(nullValueBlock, 10);
        testAggregation(getExpectedValueIncludingNulls(0, 0, 10), block);
    }

    @Test
    public void testMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(0, 10));
        testAggregation(getExpectedValueIncludingNulls(0, 10, 20), alternatingNullsBlock);
    }

    @Test
    public void testNegativeOnlyValues()
    {
        testAggregation(getExpectedValue(-10, 5), getSequenceBlock(-10, 5));
    }

    @Test
    public void testPositiveOnlyValues()
    {
        testAggregation(getExpectedValue(2, 4), getSequenceBlock(2, 4));
    }

    public Block createAlternatingNullsBlock(Block sequenceBlock)
    {
        BlockBuilder blockBuilder = new BlockBuilder(sequenceBlock.getTupleInfo());
        BlockCursor cursor = sequenceBlock.cursor();
        while (cursor.advanceNextPosition()) {
            // append null
            blockBuilder.appendNull();
            // append value
            cursor.appendTupleTo(blockBuilder);
        }
        return blockBuilder.build();
    }

    protected void testAggregation(Object expectedValue, Block block)
    {
        assertAggregation(getFunction(), getConfidence(), expectedValue, block.getPositionCount(), block);
    }
}
