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

public class TestUncompressedLongCursor extends AbstractTestCursor
{
    @Test
    public void testTupleInfo()
            throws Exception
    {
        Cursor cursor = createCursor();
        TupleInfo tupleInfo = new TupleInfo(Type.FIXED_INT_64);
        assertEquals(cursor.getTupleInfo(), tupleInfo);
    }

    @Test
    public void testGetLongState()
    {
        Cursor cursor = createCursor();
        try {
            cursor.getLong(0);
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
        CursorAssertions.assertNextValue(cursor, 0, 1111L);
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        Cursor cursor = createCursor();
        CursorAssertions.assertNextPosition(cursor, 0, 1111L);
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        Cursor cursor = createCursor();

        assertNextValue(cursor, 0, 1111L);
        assertNextValue(cursor, 1, 1111L);
        assertNextValue(cursor, 2, 1111L);
        assertNextValue(cursor, 3, 2222L);
        assertNextValue(cursor, 4, 2222L);
        assertNextValue(cursor, 5, 2222L);
        assertNextValue(cursor, 6, 2222L);
        assertNextValue(cursor, 7, 2222L);
        assertNextValue(cursor, 20, 3333L);
        assertNextValue(cursor, 21, 3333L);
        assertNextValue(cursor, 30, 4444L);

        assertFalse(cursor.hasNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        Cursor cursor = createCursor();

        assertNextPosition(cursor, 0, 1111L);
        assertNextPosition(cursor, 1, 1111L);
        assertNextPosition(cursor, 2, 1111L);
        assertNextPosition(cursor, 3, 2222L);
        assertNextPosition(cursor, 4, 2222L);
        assertNextPosition(cursor, 5, 2222L);
        assertNextPosition(cursor, 6, 2222L);
        assertNextPosition(cursor, 7, 2222L);
        assertNextPosition(cursor, 20, 3333L);
        assertNextPosition(cursor, 21, 3333L);
        assertNextPosition(cursor, 30, 4444L);

        assertFalse(cursor.hasNextPosition());
    }

    @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        // advance to first position
        cursor.advanceToPosition(0);
        assertCurrentValue(cursor, 0, 1111L);

        // skip to position in first block
        cursor.advanceToPosition(2);
        assertCurrentValue(cursor, 2, 1111L);

        // advance to same position
        cursor.advanceToPosition(2);
        assertCurrentValue(cursor, 2, 1111L);

        // skip to position in same block
        cursor.advanceToPosition(4);
        assertCurrentValue(cursor, 4, 2222L);

        // skip to position in middle block
        cursor.advanceToPosition(21);
        assertCurrentValue(cursor, 21, 3333L);

        // skip to position in gap
        cursor.advanceToPosition(25);
        assertCurrentValue(cursor, 30, 4444L);

        // skip backwards
        try {
            cursor.advanceToPosition(20);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertCurrentValue(cursor, 30, 4444L);
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
    public void testAdvanceToNextValueAdvancesPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        // first, skip to middle of a block
        CursorAssertions.assertNextValue(cursor, 0, 1111L);
        CursorAssertions.assertNextPosition(cursor, 1, 1111L);

        // force jump to next block
        CursorAssertions.assertNextValue(cursor, 2, 1111L);
    }

    @Test
    public void testAdvanceToNextPositionAdvancesValue()
    {
        Cursor cursor = createCursor();

        // first, advance to end of a block
        CursorAssertions.assertNextPosition(cursor, 0, 1111L);
        CursorAssertions.assertNextPosition(cursor, 1, 1111L);
        CursorAssertions.assertNextPosition(cursor, 2, 1111L);
        CursorAssertions.assertNextPosition(cursor, 3, 2222L);
        CursorAssertions.assertNextPosition(cursor, 4, 2222L);
        CursorAssertions.assertNextPosition(cursor, 5, 2222L);
        CursorAssertions.assertNextPosition(cursor, 6, 2222L);
        CursorAssertions.assertNextPosition(cursor, 7, 2222L);

        // force jump to next block
        CursorAssertions.assertNextPosition(cursor, 20, 3333L);
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        Cursor cursor = createCursor();

        // first, advance to end of a block
        CursorAssertions.assertNextPosition(cursor, 0, 1111L);
        CursorAssertions.assertNextPosition(cursor, 1, 1111L);
        CursorAssertions.assertNextPosition(cursor, 2, 1111L);
        CursorAssertions.assertNextPosition(cursor, 3, 2222L);
        CursorAssertions.assertNextPosition(cursor, 4, 2222L);
        CursorAssertions.assertNextPosition(cursor, 5, 2222L);
        CursorAssertions.assertNextPosition(cursor, 6, 2222L);
        CursorAssertions.assertNextPosition(cursor, 7, 2222L);

        // force jump to next block
        CursorAssertions.assertNextValue(cursor, 20, 3333L);
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
    public void testMixedValueAndPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        assertNextValue(cursor, 0, 1111L);
        assertNextPosition(cursor, 1, 1111L);
        assertNextValue(cursor, 2, 1111L);
        assertNextPosition(cursor, 3, 2222L);
        assertNextValue(cursor, 4, 2222L);
        assertNextPosition(cursor, 5, 2222L);
        assertNextValue(cursor, 6, 2222L);
        assertNextPosition(cursor, 7, 2222L);
        assertNextValue(cursor, 20, 3333L);
        assertNextPosition(cursor, 21, 3333L);
        assertNextValue(cursor, 30, 4444L);

        assertFalse(cursor.hasNextPosition());
        assertFalse(cursor.hasNextValue());
    }

    @Test
    public void testGetSlice()
            throws Exception
    {
        Cursor cursor = createCursor();

        try {
            cursor.getSlice(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetCurrentValueEndPosition()
            throws Exception
    {
        Cursor cursor = createCursor();
        while (cursor.hasNextValue()) {
            cursor.advanceNextValue();
            assertEquals(cursor.getCurrentValueEndPosition(), cursor.getPosition());
        }
    }

    protected List<UncompressedValueBlock> createBlocks()
    {
        return ImmutableList.of(
                createBlock(0, 1111L, 1111L, 1111L, 2222L, 2222L),
                createBlock(5, 2222L, 2222L, 2222L),
                createBlock(20, 3333L, 3333L),
                createBlock(30, 4444L));
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedLongCursor(createBlocks().iterator());
    }

    @Test
    public void testConstructorNulls()
            throws Exception
    {
        try {
            new UncompressedLongCursor(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException expected) {
        }
    }
}
