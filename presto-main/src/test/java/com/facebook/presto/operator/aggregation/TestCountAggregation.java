package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;

import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.Tuples.nullTuple;

public class TestCountAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_LONG);
        for (int i = start; i < start + length; i++) {
            blockBuilder.append(i);
        }
        return blockBuilder.build();
    }

    @Override
    public AggregationFunction getFunction()
    {
        return COUNT;
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        return (long) length;
    }

    @Override
    public void testAllPositionsNull()
            throws Exception
    {
        Block nullsBlock = new RunLengthEncodedBlock(nullTuple(getSequenceBlock(0, 10).getTupleInfo()), 11);
        testMultiplePositions(nullsBlock, 10L, 10);
    }

    @Override
    public void testMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(0, 10));
        testMultiplePositions(alternatingNullsBlock, 10L, 10);
    }

    @Override
    public void testVectorAllPositionsNull()
            throws Exception
    {
        Block nullsBlock = new RunLengthEncodedBlock(nullTuple(getSequenceBlock(0, 10).getTupleInfo()), 10);
        testVectorMultiplePositions(nullsBlock, 10L);
    }

    @Override
    public void testVectorMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(0, 5));
        testVectorMultiplePositions(alternatingNullsBlock, 10L);
    }

    @Override
    public void testPartialWithMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(0, 10));
        testPartialWithMultiplePositions(alternatingNullsBlock, 20L);
    }

    @Override
    public void testVectorPartialWithMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(0, 10));
        testPartialWithMultiplePositions(alternatingNullsBlock, 20L);
    }

//    @Override
//    public void testCombinerWithMixedNullAndNonNullPositions()
//    {
//        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceBlock(0, 10).cursor());
//        testCombinerWithMultiplePositions(cursor, 10L, 10);
//    }
}
