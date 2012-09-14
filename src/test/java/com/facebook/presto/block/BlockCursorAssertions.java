package com.facebook.presto.block;

import com.facebook.presto.Tuple;
import com.facebook.presto.Tuples;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BlockCursorAssertions
{
    public static void assertNextValue(Cursor blockCursor, long position, String value)
    {
        assertTrue(blockCursor.advanceNextValue());
        assertCurrentValue(blockCursor, position, value);
    }

    public static void assertNextPosition(Cursor blockCursor, long position, String value)
    {
        assertTrue(blockCursor.advanceNextPosition());
        assertCurrentValue(blockCursor, position, value);
    }

    public static void assertNextPosition(Cursor blockCursor, long position)
    {
        assertTrue(blockCursor.advanceNextPosition());
        assertEquals(blockCursor.getPosition(), position);
    }

    public static void assertCurrentValue(Cursor blockCursor, long position, String value)
    {
        Tuple tuple = Tuples.createTuple(value);
        assertEquals(blockCursor.getTuple(), tuple);
        assertEquals(blockCursor.getPosition(), position);
        assertEquals(blockCursor.getCurrentValueEndPosition(), position);
        assertTrue(blockCursor.currentTupleEquals(tuple));
        assertEquals(blockCursor.getSlice(0), tuple.getSlice(0));
    }

    public static void assertNextValue(Cursor blockCursor, long position, long value)
    {
        assertTrue(blockCursor.advanceNextValue());
        assertCurrentValue(blockCursor, position, value);
    }

    public static void assertNextPosition(Cursor blockCursor, long position, long value)
    {
        assertTrue(blockCursor.advanceNextPosition());
        assertCurrentValue(blockCursor, position, value);
    }

    public static void assertCurrentValue(Cursor blockCursor, long position, long value)
    {
        Tuple tuple = Tuples.createTuple(value);
        assertEquals(blockCursor.getTuple(), tuple);
        assertEquals(blockCursor.getPosition(), position);
        assertEquals(blockCursor.getCurrentValueEndPosition(), position);
        assertTrue(blockCursor.currentTupleEquals(tuple));
        assertEquals(blockCursor.getLong(0), tuple.getLong(0));
    }

    public static void assertNextValue(Cursor blockCursor, long position, double value)
    {
        assertTrue(blockCursor.advanceNextValue());
        assertCurrentValue(blockCursor, position, value);
    }

    public static void assertNextPosition(Cursor blockCursor, long position, double value)
    {
        assertTrue(blockCursor.advanceNextPosition());
        assertCurrentValue(blockCursor, position, value);
    }

    public static void assertCurrentValue(Cursor blockCursor, long position, double value)
    {
        Tuple tuple = Tuples.createTuple(value);
        assertEquals(blockCursor.getTuple(), tuple);
        assertEquals(blockCursor.getPosition(), position);
        assertEquals(blockCursor.getCurrentValueEndPosition(), position);
        assertTrue(blockCursor.currentTupleEquals(tuple));
        assertEquals(blockCursor.getDouble(0), tuple.getDouble(0));
    }
}
