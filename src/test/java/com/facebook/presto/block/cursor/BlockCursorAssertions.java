package com.facebook.presto.block.cursor;

import com.facebook.presto.Tuple;
import com.facebook.presto.Tuples;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BlockCursorAssertions
{
    public static void assertNextValue(BlockCursor blockCursor, long position, String value)
    {
        assertTrue(blockCursor.hasNextValue());

        blockCursor.advanceNextValue();
        assertCurrentValue(blockCursor, position, value);
    }

    public static void assertNextPosition(BlockCursor blockCursor, long position, String value)
    {
        assertTrue(blockCursor.hasNextPosition());
        blockCursor.advanceNextPosition();
        assertCurrentValue(blockCursor, position, value);
    }

    public static void assertCurrentValue(BlockCursor blockCursor, long position, String value)
    {
        Tuple tuple = Tuples.createTuple(value);
        assertEquals(blockCursor.getTuple(), tuple);
        assertEquals(blockCursor.getPosition(), position);
        assertEquals(blockCursor.getValuePositionEnd(), position);
        assertTrue(blockCursor.tupleEquals(tuple));
        assertEquals(blockCursor.getSlice(0), tuple.getSlice(0));
    }

    public static void assertNextValue(BlockCursor blockCursor, long position, long value)
    {
        assertTrue(blockCursor.hasNextValue());
        blockCursor.advanceNextValue();
        assertCurrentValue(blockCursor, position, value);
    }

    public static void assertNextPosition(BlockCursor blockCursor, long position, long value)
    {
        assertTrue(blockCursor.hasNextPosition());
        blockCursor.advanceNextPosition();
        assertCurrentValue(blockCursor, position, value);
    }

    public static void assertCurrentValue(BlockCursor blockCursor, long position, long value)
    {
        Tuple tuple = Tuples.createTuple(value);
        assertEquals(blockCursor.getTuple(), tuple);
        assertEquals(blockCursor.getPosition(), position);
        assertEquals(blockCursor.getValuePositionEnd(), position);
        assertTrue(blockCursor.tupleEquals(tuple));
        assertEquals(blockCursor.getLong(0), tuple.getLong(0));
    }

}
