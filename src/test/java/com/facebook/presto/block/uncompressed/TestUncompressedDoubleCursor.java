/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.AbstractTestCursor;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.CursorAssertions;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.Cursor;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.CursorAssertions.assertCurrentValue;
import static com.facebook.presto.block.CursorAssertions.assertNextPosition;
import static com.facebook.presto.block.CursorAssertions.assertNextValue;
import static com.facebook.presto.block.CursorAssertions.assertNextValuePosition;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestUncompressedDoubleCursor extends AbstractTestCursor
{
    @Test
    public void testTupleInfo()
            throws Exception
    {
        Cursor cursor = createCursor();
        TupleInfo tupleInfo = new TupleInfo(Type.DOUBLE);
        assertEquals(cursor.getTupleInfo(), tupleInfo);
    }

    @Test
    public void testGetDoubleState()
    {
        Cursor cursor = createCursor();
        try {
            cursor.getDouble(0);
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
        CursorAssertions.assertNextValue(cursor, 0, 11.11);
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        Cursor cursor = createCursor();
        CursorAssertions.assertNextPosition(cursor, 0, 11.11);
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        Cursor cursor = createCursor();

        assertNextValue(cursor, 0, 11.11);
        assertNextValue(cursor, 1, 11.11);
        assertNextValue(cursor, 2, 11.11);
        assertNextValue(cursor, 3, 22.22);
        assertNextValue(cursor, 4, 22.22);
        assertNextValue(cursor, 5, 22.22);
        assertNextValue(cursor, 6, 22.22);
        assertNextValue(cursor, 7, 22.22);
        assertNextValue(cursor, 20, 33.33);
        assertNextValue(cursor, 21, 33.33);
        assertNextValue(cursor, 30, 44.44);

        assertFalse(cursor.advanceNextValue());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        Cursor cursor = createCursor();

        assertNextPosition(cursor, 0, 11.11);
        assertNextPosition(cursor, 1, 11.11);
        assertNextPosition(cursor, 2, 11.11);
        assertNextPosition(cursor, 3, 22.22);
        assertNextPosition(cursor, 4, 22.22);
        assertNextPosition(cursor, 5, 22.22);
        assertNextPosition(cursor, 6, 22.22);
        assertNextPosition(cursor, 7, 22.22);
        assertNextPosition(cursor, 20, 33.33);
        assertNextPosition(cursor, 21, 33.33);
        assertNextPosition(cursor, 30, 44.44);

        assertFalse(cursor.advanceNextPosition());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        // advance to first position
        assertTrue(cursor.advanceToPosition(0));
        assertCurrentValue(cursor, 0, 11.11);

        // skip to position in first block
        assertTrue(cursor.advanceToPosition(2));
        assertCurrentValue(cursor, 2, 11.11);

        // advance to same position
        assertTrue(cursor.advanceToPosition(2));
        assertCurrentValue(cursor, 2, 11.11);

        // skip to position in same block
        assertTrue(cursor.advanceToPosition(4));
        assertCurrentValue(cursor, 4, 22.22);

        // skip to position in middle block
        assertTrue(cursor.advanceToPosition(21));
        assertCurrentValue(cursor, 21, 33.33);

        // skip to position in gap
        assertTrue(cursor.advanceToPosition(25));
        assertCurrentValue(cursor, 30, 44.44);

        // skip backwards
        try {
            cursor.advanceToPosition(20);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertCurrentValue(cursor, 30, 44.44);
        }

        // skip past end
        assertFalse(cursor.advanceToPosition(100));
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testAdvanceToNextValueAdvancesPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        // first, skip to middle of a block
        CursorAssertions.assertNextValue(cursor, 0, 11.11);
        CursorAssertions.assertNextPosition(cursor, 1, 11.11);

        // force jump to next block
        CursorAssertions.assertNextValue(cursor, 2, 11.11);
    }

    @Test
    public void testAdvanceToNextPositionAdvancesValue()
    {
        Cursor cursor = createCursor();

        // first, advance to end of a block
        CursorAssertions.assertNextPosition(cursor, 0, 11.11);
        CursorAssertions.assertNextPosition(cursor, 1, 11.11);
        CursorAssertions.assertNextPosition(cursor, 2, 11.11);
        CursorAssertions.assertNextPosition(cursor, 3, 22.22);
        CursorAssertions.assertNextPosition(cursor, 4, 22.22);
        CursorAssertions.assertNextPosition(cursor, 5, 22.22);
        CursorAssertions.assertNextPosition(cursor, 6, 22.22);
        CursorAssertions.assertNextPosition(cursor, 7, 22.22);

        // force jump to next block
        CursorAssertions.assertNextPosition(cursor, 20, 33.33);
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        Cursor cursor = createCursor();

        // first, advance to end of a block
        CursorAssertions.assertNextPosition(cursor, 0, 11.11);
        CursorAssertions.assertNextPosition(cursor, 1, 11.11);
        CursorAssertions.assertNextPosition(cursor, 2, 11.11);
        CursorAssertions.assertNextPosition(cursor, 3, 22.22);
        CursorAssertions.assertNextPosition(cursor, 4, 22.22);
        CursorAssertions.assertNextPosition(cursor, 5, 22.22);
        CursorAssertions.assertNextPosition(cursor, 6, 22.22);
        CursorAssertions.assertNextPosition(cursor, 7, 22.22);

        // force jump to next block
        CursorAssertions.assertNextValue(cursor, 20, 33.33);
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

        assertFalse(cursor.advanceNextValue());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        assertNextValue(cursor, 0, 11.11);
        assertNextPosition(cursor, 1, 11.11);
        assertNextValue(cursor, 2, 11.11);
        assertNextPosition(cursor, 3, 22.22);
        assertNextValue(cursor, 4, 22.22);
        assertNextPosition(cursor, 5, 22.22);
        assertNextValue(cursor, 6, 22.22);
        assertNextPosition(cursor, 7, 22.22);
        assertNextValue(cursor, 20, 33.33);
        assertNextPosition(cursor, 21, 33.33);
        assertNextValue(cursor, 30, 44.44);

        assertFalse(cursor.advanceNextPosition());
        assertFalse(cursor.advanceNextValue());
        assertTrue(cursor.isFinished());
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
        while (cursor.advanceNextValue()) {
            assertEquals(cursor.getCurrentValueEndPosition(), cursor.getPosition());
        }
    }

    protected List<UncompressedBlock> createBlocks()
    {
        return ImmutableList.of(
                Blocks.createDoublesBlock(0, 11.11, 11.11, 11.11, 22.22, 22.22),
                Blocks.createDoublesBlock(5, 22.22, 22.22, 22.22),
                Blocks.createDoublesBlock(20, 33.33, 33.33),
                Blocks.createDoublesBlock(30, 44.44));
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedDoubleCursor(createBlocks().iterator());
    }

    @Test
    public void testConstructorNulls()
            throws Exception
    {
        try {
            new UncompressedDoubleCursor(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException expected) {
        }
    }
}
