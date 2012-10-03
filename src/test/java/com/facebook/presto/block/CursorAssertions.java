package com.facebook.presto.block;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.Tuples;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.google.common.collect.ImmutableSortedMap;

import java.util.SortedMap;

import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class CursorAssertions
{
    public static void assertAdvanceNextPosition(Cursor cursor)
    {
        assertAdvanceNextPosition(cursor, SUCCESS);
    }

    public static void assertAdvanceNextPosition(Cursor cursor, AdvanceResult result) {
        assertEquals(cursor.advanceNextPosition(), result);
    }

    public static void assertAdvanceNextValue(Cursor cursor)
    {
        assertAdvanceNextValue(cursor, SUCCESS);
    }

    public static void assertAdvanceNextValue(Cursor cursor, AdvanceResult result) {
        assertEquals(cursor.advanceNextValue(), result);
    }

    public static void assertAdvanceToPosition(Cursor cursor, long position)
    {
        assertAdvanceToPosition(cursor, position, SUCCESS);
    }

    public static void assertAdvanceToPosition(Cursor cursor, long position, AdvanceResult result) {
        assertEquals(cursor.advanceToPosition(position), result);
    }

    public static void assertNextPosition(Cursor cursor, long position)
    {
        assertAdvanceNextPosition(cursor);
        assertEquals(cursor.getPosition(), position);
    }

    public static void assertNextValuePosition(Cursor cursor, long position)
    {
        assertAdvanceNextValue(cursor);
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
        assertAdvanceNextValue(cursor);
        assertCurrentValue(cursor, position, tuple);
    }

    public static void assertNextPosition(Cursor cursor, long position, Tuple tuple)
    {
        assertAdvanceNextPosition(cursor);
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
        while (Cursors.advanceNextPositionNoYield(cursor)) {
            tuples.put(cursor.getPosition(), cursor.getTuple());
        }
        return tuples.build();
    }

    public static void assertPositions(Cursor cursor, long... positions)
    {
        for (long position : positions) {
            assertAdvanceNextPosition(cursor);
            assertEquals(cursor.getPosition(), position);
        }
    }
}
