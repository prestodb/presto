package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.Tuples.nullTuple;

public class TestCountAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int max)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_LONG);
        for (int i = 0; i < max; i++) {
            blockBuilder.append(i);
        }
        return blockBuilder.build();
    }

    @Override
    public FullAggregationFunction getFullFunction()
    {
        return new CountAggregation(0, 0);
    }

    @Override
    public Number getExpectedValue(int positions)
    {
        return (long) positions;
    }

    @Override
    public Number getActualValue(AggregationFunction function)
    {
        return function.evaluate().getLong(0);
    }

    @Override
    public void testAllPositionsNull()
            throws Exception
    {
        Block nullsBlock = new RunLengthEncodedBlock(nullTuple(getSequenceBlock(10).getTupleInfo()), 11);
        testMultiplePositions(nullsBlock.cursor(), 10L, 10);
    }

    @Override
    public void testMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(10));
        testMultiplePositions(alternatingNullsBlock.cursor(), 10L, 10);
    }

    @Override
    public void testVectorAllPositionsNull()
            throws Exception
    {
        Block nullsBlock = new RunLengthEncodedBlock(nullTuple(getSequenceBlock(10).getTupleInfo()), 10);
        testVectorMultiplePositions(nullsBlock, 10L);
    }

    @Override
    public void testVectorMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(5));
        testVectorMultiplePositions(alternatingNullsBlock, 10L);
    }

    @Override
    public void testPartialWithMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(10));
        testPartialWithMultiplePositions(alternatingNullsBlock, 20L);
    }

    @Override
    public void testVectorPartialWithMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(10));
        testPartialWithMultiplePositions(alternatingNullsBlock, 20L);
    }

    @Override
    public void testCombinerWithMixedNullAndNonNullPositions()
    {
        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceBlock(10).cursor());
        testCombinerWithMultiplePositions(cursor, 10L, 10);
    }
}
