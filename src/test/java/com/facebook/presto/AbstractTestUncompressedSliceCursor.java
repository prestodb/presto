/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.NoSuchElementException;

import static com.facebook.presto.Blocks.createBlock;
import static com.facebook.presto.CursorAssertions.assertCurrentValue;
import static com.facebook.presto.CursorAssertions.assertNextPosition;
import static com.facebook.presto.CursorAssertions.assertNextValue;
import static com.facebook.presto.CursorAssertions.assertNextValuePosition;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

public abstract class AbstractTestUncompressedSliceCursor extends AbstractTestCursor
{
    @Test
    public void testTupleInfo()
            throws Exception
    {
        Cursor cursor = createCursor();
        TupleInfo tupleInfo = new TupleInfo(Type.VARIABLE_BINARY);
        assertEquals(cursor.getTupleInfo(), tupleInfo);
    }

    @Test
    public void testGetSliceState()
    {
        Cursor cursor = createCursor();
        try {
            cursor.getSlice(0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testFirstValue()
            throws Exception
    {
        Cursor cursor = createCursor();
        CursorAssertions.assertNextValue(cursor, 0, "apple");
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        Cursor cursor = createCursor();
        CursorAssertions.assertNextPosition(cursor, 0, "apple");
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        Cursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextValue(cursor, 1, "apple");
        assertNextValue(cursor, 2, "apple");
        assertNextValue(cursor, 3, "banana");
        assertNextValue(cursor, 4, "banana");
        assertNextValue(cursor, 5, "banana");
        assertNextValue(cursor, 6, "banana");
        assertNextValue(cursor, 7, "banana");
        assertNextValue(cursor, 20, "cherry");
        assertNextValue(cursor, 21, "cherry");
        assertNextValue(cursor, 30, "date");

        assertFalse(cursor.hasNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        Cursor cursor = createCursor();

        assertNextPosition(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextPosition(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextPosition(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextPosition(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");
        assertNextPosition(cursor, 20, "cherry");
        assertNextPosition(cursor, 21, "cherry");
        assertNextPosition(cursor, 30, "date");

        assertFalse(cursor.hasNextPosition());
    }

    @Test
    public void testAdvanceToNextValueAdvancesPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        // first, skip to middle of a block
        CursorAssertions.assertNextValue(cursor, 0, "apple");
        CursorAssertions.assertNextPosition(cursor, 1, "apple");

        // force jump to next block
        CursorAssertions.assertNextValue(cursor, 2, "apple");
    }

    @Test
    public void testAdvanceToNextPositionAdvancesValue()
    {
        Cursor cursor = createCursor();

        // first, advance to end of a block
        CursorAssertions.assertNextPosition(cursor, 0, "apple");
        CursorAssertions.assertNextPosition(cursor, 1, "apple");
        CursorAssertions.assertNextPosition(cursor, 2, "apple");
        CursorAssertions.assertNextPosition(cursor, 3, "banana");
        CursorAssertions.assertNextPosition(cursor, 4, "banana");
        CursorAssertions.assertNextPosition(cursor, 5, "banana");
        CursorAssertions.assertNextPosition(cursor, 6, "banana");
        CursorAssertions.assertNextPosition(cursor, 7, "banana");

        // force jump to next block
        CursorAssertions.assertNextPosition(cursor, 20, "cherry");
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        Cursor cursor = createCursor();

        // first, advance to end of a block
        CursorAssertions.assertNextPosition(cursor, 0, "apple");
        CursorAssertions.assertNextPosition(cursor, 1, "apple");
        CursorAssertions.assertNextPosition(cursor, 2, "apple");
        CursorAssertions.assertNextPosition(cursor, 3, "banana");
        CursorAssertions.assertNextPosition(cursor, 4, "banana");
        CursorAssertions.assertNextPosition(cursor, 5, "banana");
        CursorAssertions.assertNextPosition(cursor, 6, "banana");
        CursorAssertions.assertNextPosition(cursor, 7, "banana");

        // force jump to next block
        CursorAssertions.assertNextValue(cursor, 20, "cherry");
    }

    @Test
    public void testNextValuePosition()
            throws Exception
    {
        Cursor cursor = createCursor();

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
    public void testAdvanceToPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        // advance to first position
        cursor.advanceToPosition(0);
        assertCurrentValue(cursor, 0, "apple");

        // skip to position in first block
        cursor.advanceToPosition(2);
        assertCurrentValue(cursor, 2, "apple");

        // advance to same position
        cursor.advanceToPosition(2);
        assertCurrentValue(cursor, 2, "apple");

        // skip to position in same block
        cursor.advanceToPosition(4);
        assertCurrentValue(cursor, 4, "banana");

        // skip to position in middle block
        cursor.advanceToPosition(21);
        assertCurrentValue(cursor, 21, "cherry");

        // skip to position in gap
        cursor.advanceToPosition(25);
        assertCurrentValue(cursor, 30, "date");

        // skip backwards
        try {
            cursor.advanceToPosition(20);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertCurrentValue(cursor, 30, "date");
        }

        // skip past end
        try {
            cursor.advanceToPosition(100);
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException e) {
            // success
        }
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextValue(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextValue(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextValue(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");
        assertNextValue(cursor, 20, "cherry");
        assertNextPosition(cursor, 21, "cherry");
        assertNextValue(cursor, 30, "date");

        assertFalse(cursor.hasNextPosition());
        assertFalse(cursor.hasNextValue());
    }

    protected TupleInfo createTupleInfo()
    {
        return new TupleInfo(Type.VARIABLE_BINARY);
    }

    protected List<UncompressedValueBlock> createBlocks()
    {
        return ImmutableList.of(
                createBlock(0, "apple", "apple", "apple", "banana", "banana"),
                createBlock(5, "banana", "banana", "banana"),
                createBlock(20, "cherry", "cherry"),
                createBlock(30, "date"));
    }
}
