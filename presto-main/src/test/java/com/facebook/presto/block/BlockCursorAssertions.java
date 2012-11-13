package com.facebook.presto.block;

import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.Tuples;
import com.google.common.collect.ImmutableSortedMap;

import java.util.SortedMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class BlockCursorAssertions
{
    public static void assertAdvanceNextPosition(BlockCursor cursor)
    {
        assertTrue(cursor.advanceNextPosition());
    }

    public static void assertAdvanceNextValue(BlockCursor cursor) {
        assertTrue(cursor.advanceNextValue());
    }

    public static void assertAdvanceToPosition(BlockCursor cursor, int position) {
        assertTrue(cursor.advanceToPosition(position));
    }

    public static void assertNextPosition(BlockCursor cursor, int position)
    {
        assertAdvanceNextPosition(cursor);
        assertEquals(cursor.getPosition(), position);
    }

    public static void assertNextValuePosition(BlockCursor cursor, int position)
    {
        assertAdvanceNextValue(cursor);
        assertEquals(cursor.getPosition(), position);
    }

    public static void assertNextValue(BlockCursor cursor, int position, String value)
    {
        assertNextValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextPosition(BlockCursor cursor, int position, String value)
    {
        assertNextPosition(cursor, position, Tuples.createTuple(value));
    }

    public static void assertCurrentValue(BlockCursor cursor, int position, String value)
    {
        assertCurrentValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextValue(BlockCursor cursor, int position, long value)
    {
        assertNextValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextPosition(BlockCursor cursor, int position, long value)
    {
        assertNextPosition(cursor, position, Tuples.createTuple(value));
    }

    public static void assertCurrentValue(BlockCursor cursor, int position, long value)
    {
        assertCurrentValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextValue(BlockCursor cursor, int position, double value)
    {
        assertNextValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextPosition(BlockCursor cursor, int position, double value)
    {
        assertNextPosition(cursor, position, Tuples.createTuple(value));
    }

    public static void assertCurrentValue(BlockCursor cursor, int position, double value)
    {
        assertCurrentValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextValue(BlockCursor cursor, int position, Tuple tuple)
    {
        assertAdvanceNextValue(cursor);
        assertCurrentValue(cursor, position, tuple);
    }

    public static void assertNextPosition(BlockCursor cursor, int position, Tuple tuple)
    {
        assertAdvanceNextPosition(cursor);
        assertCurrentValue(cursor, position, tuple);
    }

    public static void assertCurrentValue(BlockCursor cursor, int position, Tuple tuple)
    {
        TupleInfo tupleInfo = tuple.getTupleInfo();
        assertEquals(cursor.getTupleInfo(), tupleInfo);

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
        assertTrue(cursor.currentTupleEquals(tuple));

        for (int index = 0; index < tupleInfo.getFieldCount(); index++) {
            assertEquals(cursor.isNull(index), tuple.isNull(index));
            switch (tupleInfo.getTypes().get(index)) {
                case FIXED_INT_64:
                    assertEquals(cursor.getLong(index), tuple.getLong(index));
                    try {
                        cursor.getDouble(index);
                        fail("Expected IllegalStateException or UnsupportedOperationException");
                    }
                    catch (IllegalStateException | UnsupportedOperationException expected) {
                    }
                    try {
                        cursor.getSlice(index);
                        fail("Expected IllegalStateException or UnsupportedOperationException");
                    }
                    catch (IllegalStateException | UnsupportedOperationException expected) {
                    }
                    break;
                case VARIABLE_BINARY:
                    assertEquals(cursor.getSlice(index), tuple.getSlice(index));
                    try {
                        cursor.getDouble(index);
                        fail("Expected IllegalStateException or UnsupportedOperationException");
                    }
                    catch (IllegalStateException | UnsupportedOperationException expected) {
                    }
                    try {
                        cursor.getLong(index);
                        fail("Expected IllegalStateException or UnsupportedOperationException");
                    }
                    catch (IllegalStateException | UnsupportedOperationException expected) {
                    }
                    break;
                case DOUBLE:
                    assertEquals(cursor.getDouble(index), tuple.getDouble(index));
                    try {
                        cursor.getSlice(index);
                        fail("Expected IllegalStateException or UnsupportedOperationException");
                    }
                    catch (IllegalStateException | UnsupportedOperationException expected) {
                    }
                    try {
                        cursor.getSlice(index);
                        fail("Expected IllegalStateException or UnsupportedOperationException");
                    }
                    catch (IllegalStateException | UnsupportedOperationException expected) {
                    }
                    break;
            }
        }
    }

    public static SortedMap<Integer, Tuple> toTuplesMap(BlockCursor cursor)
    {
        ImmutableSortedMap.Builder<Integer, Tuple> tuples = ImmutableSortedMap.naturalOrder();
        while (cursor.advanceNextPosition()) {
            tuples.put(cursor.getPosition(), cursor.getTuple());
        }
        return tuples.build();
    }

    public static void assertPositions(BlockCursor cursor, int... positions)
    {
        for (int position : positions) {
            assertAdvanceNextPosition(cursor);
            assertEquals(cursor.getPosition(), position);
        }
    }
}
