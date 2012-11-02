package com.facebook.presto.noperator.aggregation;

import com.facebook.presto.nblock.BlockCursor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class TestAggregationFunction
{
    public abstract BlockCursor getSequenceCursor(long max);
    public abstract AggregationFunction getFunction();
    public abstract Number getExpectedValue(long positions);
    public abstract Number getActualValue(AggregationFunction function);

    @Test
    public void testSinglePosition()
            throws Exception
    {
        AggregationFunction function = getFunction();
        BlockCursor cursor = getSequenceCursor(10);

        cursor.advanceNextPosition();

        long position = cursor.getPosition();
        function.add(cursor);

        assertEquals(getActualValue(function), getExpectedValue(1));
        assertEquals(cursor.getPosition(), position);
    }

    @Test
    public void testMultiplePositions()
    {
        AggregationFunction function = getFunction();
        BlockCursor cursor = getSequenceCursor(10);

        for (int i = 0; i < 5; i++) {
            assertTrue(cursor.advanceNextPosition());
            function.add(cursor);
        }

        assertEquals(getActualValue(function), getExpectedValue(5));
        assertEquals(cursor.getPosition(), 4);
    }

    @Test
    public void testWholeRange()
            throws Exception
    {
        int max = 10;

        AggregationFunction function = getFunction();
        BlockCursor cursor = getSequenceCursor(max);

        while (cursor.advanceNextPosition()) {
            function.add(cursor);
        }

        assertEquals(getActualValue(function), getExpectedValue(max));
        assertTrue(cursor.isFinished());
    }
}
