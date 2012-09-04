package com.facebook.presto;

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

        assertTrue(cursor.hasNextValue());
        cursor.advanceNextValue();

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
    }

    public static void assertNextPosition(Cursor cursor, long position, String value)
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.VARIABLE_BINARY);

        Tuple tuple = info.builder()
                .append(Slices.wrappedBuffer(value.getBytes(Charsets.UTF_8)))
                .build();

        assertTrue(cursor.hasNextPosition());
        cursor.advanceNextPosition();

        assertEquals(cursor.getTuple(), tuple);
        assertEquals(cursor.getPosition(), position);
    }

    public static void assertNextValuePosition(Cursor cursor, long position)
    {
        assertTrue(cursor.hasNextValue());
        assertEquals(cursor.peekNextValuePosition(), position);
        cursor.advanceNextValue();
    }
}
