package com.facebook.presto;

import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.Blocks.createBlock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestUncompressedCursor
{
    @Test
    public void testValue()
            throws Exception
    {
        UncompressedCursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextValue(cursor, 1, "apple");
        assertNextValue(cursor, 2, "apple");
        assertNextValue(cursor, 3, "banana");
        assertNextValue(cursor, 4, "banana");
        assertNextValue(cursor, 5, "banana");
        assertNextValue(cursor, 6, "banana");
        assertNextValue(cursor, 7, "banana");
        assertNextValue(cursor, 20, "date");
        assertNextValue(cursor, 21, "date");
        assertNextValue(cursor, 30, "cherry");

        assertFalse(cursor.hasNextValue());
    }

    @Test
    public void testPosition()
    {
        UncompressedCursor cursor = createCursor();

        assertNextPosition(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextPosition(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextPosition(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextPosition(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");
        assertNextPosition(cursor, 20, "date");
        assertNextPosition(cursor, 21, "date");
        assertNextPosition(cursor, 30, "cherry");

        assertFalse(cursor.hasNextPosition());
    }

    @Test
    public void testNextValuePosition()
            throws Exception
    {
        UncompressedCursor cursor = createCursor();

        assertNextValuePosition(cursor, 0);
        assertNextValuePosition(cursor, 1);
        assertNextValuePosition(cursor, 2);
        assertNextValuePosition(cursor, 3);
        assertNextValuePosition(cursor, 4);
        assertNextValuePosition(cursor, 5);
        assertNextValuePosition(cursor, 6);
        assertNextValuePosition(cursor, 7);
        assertNextValuePosition(cursor, 20);
        assertNextValuePosition(cursor, 21);
        assertNextValuePosition(cursor, 30);

        assertFalse(cursor.hasNextValue());
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        UncompressedCursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextValue(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextValue(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextValue(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");
        assertNextValue(cursor, 20, "date");
        assertNextPosition(cursor, 21, "date");
        assertNextValue(cursor, 30, "cherry");

        assertFalse(cursor.hasNextPosition());
        assertFalse(cursor.hasNextValue());
    }

    private UncompressedCursor createCursor()
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.VARIABLE_BINARY);

        List<UncompressedValueBlock> blocks = ImmutableList.of(
                createBlock(0, "apple", "apple", "apple", "banana", "banana"),
                createBlock(5, "banana", "banana", "banana"),
                createBlock(20, "date", "date"),
                createBlock(30, "cherry"));

        return new UncompressedCursor(info, blocks.iterator());
    }

    private static void assertNextValue(Cursor cursor, long position, String value)
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

    private static void assertNextPosition(Cursor cursor, long position, String value)
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

    private static void assertNextValuePosition(Cursor cursor, long position)
    {
        assertTrue(cursor.hasNextValue());
        assertEquals(cursor.peekNextValuePosition(), position);
        cursor.advanceNextValue();
    }

}
