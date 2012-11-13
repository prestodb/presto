package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.rle.RunLengthEncodedBlockCursor;
import com.facebook.presto.tuple.Tuples;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestAggregationFunction
{
    public abstract BlockCursor getSequenceCursor(int max);
    public abstract AggregationFunction getFunction();
    public abstract Number getExpectedValue(long positions);
    public abstract Number getActualValue(AggregationFunction function);

    @Test
    public void testNoPositions()
            throws Exception
    {
        testMultiplePositions(getSequenceCursor(10), getExpectedValue(0), 0);
    }

    @Test
    public void testAllPositionsNull()
            throws Exception
    {
        BlockCursor nullsCursor = new RunLengthEncodedBlockCursor(Tuples.nullTuple(getSequenceCursor(0).getTupleInfo()), 11);
        testMultiplePositions(nullsCursor, getExpectedValue(0), 10);
    }

    @Test
    public void testSinglePosition()
            throws Exception
    {
        testMultiplePositions(getSequenceCursor(10), getExpectedValue(1), 1);
    }

    @Test
    public void testMultiplePositions()
    {
        testMultiplePositions(getSequenceCursor(10), getExpectedValue(5), 5);
    }

    @Test
    public void testMixedNullAndNonNullPositions()
    {
        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceCursor(10));
        testMultiplePositions(cursor, getExpectedValue(5), 10);
    }

    private void testMultiplePositions(BlockCursor cursor, Number expectedValue, int positions)
    {
        AggregationFunction function = getFunction();

        for (int i = 0; i < positions; i++) {
            assertTrue(cursor.advanceNextPosition());
            function.add(cursor);
        }

        assertEquals(getActualValue(function), expectedValue);
        if (positions > 0) {
            assertEquals(cursor.getPosition(), positions - 1);
        }
    }
}
