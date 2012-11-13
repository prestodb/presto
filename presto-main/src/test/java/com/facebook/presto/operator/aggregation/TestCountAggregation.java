package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.rle.RunLengthEncodedBlockCursor;

import static com.facebook.presto.tuple.Tuples.nullTuple;

public class TestCountAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public BlockCursor getSequenceCursor(int max)
    {
        return new LongSequenceCursor(max);
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
        BlockCursor nullsCursor = new RunLengthEncodedBlockCursor(nullTuple(getSequenceCursor(0).getTupleInfo()), 11);
        testMultiplePositions(nullsCursor, 10L, 10);
    }

    @Override
    public void testMixedNullAndNonNullPositions()
    {
        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceCursor(10));
        testMultiplePositions(cursor, 10L, 10);
    }

    @Override
    public void testPartialWithMixedNullAndNonNullPositions()
    {
        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceCursor(10));
        testPartialWithMultiplePositions(cursor, 10L, 10);
    }

    @Override
    public void testCombinerWithMixedNullAndNonNullPositions()
    {
        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceCursor(10));
        testCombinerWithMultiplePositions(cursor, 10L, 10);
    }
}
