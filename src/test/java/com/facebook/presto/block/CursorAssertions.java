package com.facebook.presto.block;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CursorAssertions
{
    public static void assertNextValue(Cursor cursor, long position, String value)
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.VARIABLE_BINARY);

        Tuple tuple = info.builder()
                .append(Slices.wrappedBuffer(value.getBytes(Charsets.UTF_8)))
                .build();

        assertTrue(cursor.advanceNextValue());

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
        assertTrue(cursor.currentTupleEquals(tuple));
        assertEquals(cursor.getSlice(0), tuple.getSlice(0));
    }

    public static void assertNextPosition(Cursor cursor, long position)
    {
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getPosition(), position);
    }

    public static void assertNextPosition(Cursor cursor, long position, String value)
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.VARIABLE_BINARY);

        Tuple tuple = info.builder()
                .append(Slices.wrappedBuffer(value.getBytes(Charsets.UTF_8)))
                .build();

        assertTrue(cursor.advanceNextPosition());

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
        assertTrue(cursor.currentTupleEquals(tuple));
        assertEquals(cursor.getSlice(0), tuple.getSlice(0));
    }

    public static void assertCurrentValue(Cursor cursor, long position, String value)
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.VARIABLE_BINARY);

        Tuple tuple = info.builder()
                .append(Slices.wrappedBuffer(value.getBytes(Charsets.UTF_8)))
                .build();

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
    }

    public static void assertNextValue(Cursor cursor, long position, long value)
    {
        TupleInfo info = new TupleInfo(Type.FIXED_INT_64);

        Tuple tuple = info.builder()
                .append(value)
                .build();

        assertTrue(cursor.advanceNextValue());

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
        assertTrue(cursor.currentTupleEquals(tuple));
        assertEquals(cursor.getLong(0), tuple.getLong(0));
    }

    public static void assertNextPosition(Cursor cursor, long position, long value)
    {
        TupleInfo info = new TupleInfo(Type.FIXED_INT_64);

        Tuple tuple = info.builder()
                .append(value)
                .build();

        assertTrue(cursor.advanceNextPosition());

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
        assertTrue(cursor.currentTupleEquals(tuple));
        assertEquals(cursor.getLong(0), tuple.getLong(0));
    }

    public static void assertCurrentValue(Cursor cursor, long position, long value)
    {
        TupleInfo info = new TupleInfo(Type.FIXED_INT_64);

        Tuple tuple = info.builder()
                .append(value)
                .build();

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
    }

    public static void assertNextValue(Cursor cursor, long position, double value)
    {
        TupleInfo info = new TupleInfo(Type.DOUBLE);

        Tuple tuple = info.builder()
                .append(value)
                .build();

        assertTrue(cursor.advanceNextValue());

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
        assertTrue(cursor.currentTupleEquals(tuple));
        assertEquals(cursor.getDouble(0), tuple.getDouble(0));
    }

    public static void assertNextPosition(Cursor cursor, long position, double value)
    {
        TupleInfo info = new TupleInfo(Type.DOUBLE);

        Tuple tuple = info.builder()
                .append(value)
                .build();

        assertTrue(cursor.advanceNextPosition());

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
        assertTrue(cursor.currentTupleEquals(tuple));
        assertEquals(cursor.getDouble(0), tuple.getDouble(0));
    }

    public static void assertCurrentValue(Cursor cursor, long position, double value)
    {
        TupleInfo info = new TupleInfo(Type.DOUBLE);

        Tuple tuple = info.builder()
                .append(value)
                .build();

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
    }


    public static void assertNextValuePosition(Cursor cursor, long position)
    {
        assertTrue(cursor.advanceNextValue());
        assertEquals(cursor.getPosition(), position);
    }
}
