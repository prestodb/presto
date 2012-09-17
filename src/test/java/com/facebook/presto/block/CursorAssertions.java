package com.facebook.presto.block;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.Tuples;
import com.google.common.collect.ImmutableSortedMap;

import java.util.SortedMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class CursorAssertions
{
    public static void assertNextPosition(Cursor cursor, long position)
    {
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getPosition(), position);
    }

    public static void assertNextValuePosition(Cursor cursor, long position)
    {
        assertTrue(cursor.advanceNextValue());
        assertEquals(cursor.getPosition(), position);
    }

    public static void assertNextValue(Cursor cursor, long position, String value)
    {
        assertNextValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextPosition(Cursor cursor, long position, String value)
    {
        assertNextPosition(cursor, position, Tuples.createTuple(value));
    }

    public static void assertCurrentValue(Cursor cursor, long position, String value)
    {
        assertCurrentValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextValue(Cursor cursor, long position, long value)
    {
        assertNextValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextPosition(Cursor cursor, long position, long value)
    {
        assertNextPosition(cursor, position, Tuples.createTuple(value));
    }

    public static void assertCurrentValue(Cursor cursor, long position, long value)
    {
        assertCurrentValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextValue(Cursor cursor, long position, double value)
    {
        assertNextValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextPosition(Cursor cursor, long position, double value)
    {
        assertNextPosition(cursor, position, Tuples.createTuple(value));
    }

    public static void assertCurrentValue(Cursor cursor, long position, double value)
    {
        assertCurrentValue(cursor, position, Tuples.createTuple(value));
    }

    public static void assertNextValue(Cursor cursor, long position, Tuple tuple)
    {
        assertTrue(cursor.advanceNextValue());
        assertCurrentValue(cursor, position, tuple);
    }

    public static void assertNextPosition(Cursor cursor, long position, Tuple tuple)
    {
        assertTrue(cursor.advanceNextPosition());
        assertCurrentValue(cursor, position, tuple);
    }

    public static void assertCurrentValue(Cursor cursor, long position, Tuple tuple)
    {
        TupleInfo tupleInfo = tuple.getTupleInfo();
        assertEquals(cursor.getTupleInfo(), tupleInfo);

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
        assertTrue(cursor.currentTupleEquals(tuple));

        for (int index = 0; index < tupleInfo.getFieldCount(); index++) {
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

    public static SortedMap<Long, Tuple> toTuplesMap(Cursor cursor)
    {
        ImmutableSortedMap.Builder<Long, Tuple> tuples = ImmutableSortedMap.naturalOrder();
        while (cursor.advanceNextPosition()) {
            tuples.put(cursor.getPosition(), cursor.getTuple());
        }
        return tuples.build();
    }

    public static void assertPositions(Cursor cursor, long... positions)
    {
        for (long position : positions) {
            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getPosition(), position);
        }
    }
}
