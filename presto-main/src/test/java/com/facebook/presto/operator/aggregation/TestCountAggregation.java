package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.rle.RunLengthEncodedBlockCursor;
import com.facebook.presto.tuple.Tuples;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCountAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public BlockCursor getSequenceCursor(int max)
    {
        return new LongSequenceCursor(max);
    }

    @Override
    public CountAggregation getFunction()
    {
        return new CountAggregation();
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
        BlockCursor nullsCursor = new RunLengthEncodedBlockCursor(Tuples.nullTuple(getSequenceCursor(0).getTupleInfo()), 11);

        CountAggregation function = getFunction();

        // add 10 null positions
        for (int i = 0; i < 10; i++) {
            function.add(nullsCursor);
        }

        assertEquals(getActualValue(function), 10L);
    }

    @Override
    public void testMixedNullAndNonNullPositions()
    {
        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceCursor(10));
        AggregationFunction function = getFunction();

        for (int i = 0; i < 10; i++) {
            assertTrue(cursor.advanceNextPosition());
            function.add(cursor);
        }

        assertEquals(getActualValue(function), 10L);
    }
}
