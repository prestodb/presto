package com.facebook.presto.block.cursor;

import com.facebook.presto.Tuple;
import com.facebook.presto.Tuples;
import com.facebook.presto.block.cursor.BlockCursor;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BlockCursorAssertions
{
    public static void assertNextValue(BlockCursor blockCursor, long position, String value)
    {
        assertTrue(blockCursor.hasNextValue());

        BlockCursor originalPosition = blockCursor.duplicate();

        blockCursor.advanceNextValue();
        assertCurrentValue(blockCursor, position, value);
        assertCurrentValue(blockCursor.duplicate(), position, value);

        blockCursor.moveTo(originalPosition);

        blockCursor.advanceNextValue();
        assertCurrentValue(blockCursor, position, value);
        assertCurrentValue(blockCursor.duplicate(), position, value);
    }

    public static void assertCurrentValue(BlockCursor blockCursor, long position, String value)
    {
        Tuple tuple = Tuples.createTuple(value);
        assertEquals(blockCursor.getTuple(), tuple);
        assertEquals(blockCursor.getPosition(), position);
        assertEquals(blockCursor.getValuePositionEnd(), position);
        assertFalse(blockCursor.hasNextValuePosition());
        assertTrue(blockCursor.tupleEquals(tuple));
        assertEquals(blockCursor.getSlice(0), tuple.getSlice(0));
    }

    public static void assertNextValue(BlockCursor blockCursor, long position, long value)
    {
        assertTrue(blockCursor.hasNextValue());

        BlockCursor originalPosition = blockCursor.duplicate();

        blockCursor.advanceNextValue();
        assertCurrentValue(blockCursor, position, value);
        assertCurrentValue(blockCursor.duplicate(), position, value);

        blockCursor.moveTo(originalPosition);

        blockCursor.advanceNextValue();
        assertCurrentValue(blockCursor, position, value);
        assertCurrentValue(blockCursor.duplicate(), position, value);
    }

    public static void assertCurrentValue(BlockCursor blockCursor, long position, long value)
    {
        Tuple tuple = Tuples.createTuple(value);
        assertEquals(blockCursor.getTuple(), tuple);
        assertEquals(blockCursor.getPosition(), position);
        assertEquals(blockCursor.getValuePositionEnd(), position);
        assertFalse(blockCursor.hasNextValuePosition());
        assertTrue(blockCursor.tupleEquals(tuple));
        assertEquals(blockCursor.getLong(0), tuple.getLong(0));
    }
}
