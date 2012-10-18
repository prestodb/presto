package com.facebook.presto.aggregation;

import com.facebook.presto.block.Cursor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class TestAggregationFunction
{
    public abstract Cursor getSequenceCursor(long max);
    public abstract AggregationFunction getFunction();
    public abstract Number getExpectedValue(long start, long end);
    public abstract Number getActualValue(AggregationFunction function);

    @Test
    public void testSinglePosition()
            throws Exception
    {
        AggregationFunction function = getFunction();
        Cursor cursor = getSequenceCursor(10);

        cursor.advanceNextPosition();

        long position = cursor.getPosition();
        function.add(cursor, position);

        assertEquals(getActualValue(function), getExpectedValue(position, position));
        assertEquals(cursor.getPosition(), position);
    }

    @Test
    public void testMultiplePositions()
    {
        AggregationFunction function = getFunction();
        Cursor cursor = getSequenceCursor(10);

        cursor.advanceNextPosition();

        long position = cursor.getPosition();
        function.add(cursor, position + 5);

        assertEquals(getActualValue(function), getExpectedValue(position, position + 5));
        assertEquals(cursor.getPosition(), position + 5);
    }

    @Test
    public void testWholeRange()
            throws Exception
    {
        int max = 10;

        AggregationFunction function = getFunction();
        Cursor cursor = getSequenceCursor(max);

        cursor.advanceNextPosition();

        function.add(cursor, Long.MAX_VALUE);

        assertEquals(getActualValue(function), getExpectedValue(0, max));
        assertTrue(cursor.isFinished());
    }
}
