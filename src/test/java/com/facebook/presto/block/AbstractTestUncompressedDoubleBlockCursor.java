/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestUncompressedDoubleBlockCursor extends AbstractTestUncompressedBlockCursor
{
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
        BlockCursorAssertions.assertNextValue(cursor, 0, 11.11);
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        Cursor cursor = createCursor();
        BlockCursorAssertions.assertNextPosition(cursor, 0, 11.11);
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        Cursor cursor = createCursor();

        BlockCursorAssertions.assertNextValue(cursor, 0, 11.11);
        BlockCursorAssertions.assertNextValue(cursor, 1, 11.11);
        BlockCursorAssertions.assertNextValue(cursor, 2, 11.11);
        BlockCursorAssertions.assertNextValue(cursor, 3, 22.22);
        BlockCursorAssertions.assertNextValue(cursor, 4, 22.22);
        BlockCursorAssertions.assertNextValue(cursor, 5, 22.22);
        BlockCursorAssertions.assertNextValue(cursor, 6, 22.22);
        BlockCursorAssertions.assertNextValue(cursor, 7, 22.22);
        BlockCursorAssertions.assertNextValue(cursor, 8, 33.33);
        BlockCursorAssertions.assertNextValue(cursor, 9, 33.33);
        BlockCursorAssertions.assertNextValue(cursor, 10, 44.44);

        assertFalse(cursor.advanceNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        Cursor cursor = createCursor();

        BlockCursorAssertions.assertNextPosition(cursor, 0, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 1, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 2, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 3, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 4, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 5, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 6, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 7, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 8, 33.33);
        BlockCursorAssertions.assertNextPosition(cursor, 9, 33.33);
        BlockCursorAssertions.assertNextPosition(cursor, 10, 44.44);

        assertFalse(cursor.advanceNextPosition());
    }

   @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        // advance to first position
        assertTrue(cursor.advanceToPosition(0));
        BlockCursorAssertions.assertCurrentValue(cursor, 0, 11.11);

        // skip to position in first block
        assertTrue(cursor.advanceToPosition(2));
        BlockCursorAssertions.assertCurrentValue(cursor, 2, 11.11);

        // advance to same position
        assertTrue(cursor.advanceToPosition(2));
        BlockCursorAssertions.assertCurrentValue(cursor, 2, 11.11);

        // skip to position in same block
        assertTrue(cursor.advanceToPosition(4));
        BlockCursorAssertions.assertCurrentValue(cursor, 4, 22.22);

        // skip to position in middle block
        assertTrue(cursor.advanceToPosition(8));
        BlockCursorAssertions.assertCurrentValue(cursor, 8, 33.33);

        // skip to position in gap
        assertTrue(cursor.advanceToPosition(10));
        BlockCursorAssertions.assertCurrentValue(cursor, 10, 44.44);

        // skip backwards
        try {
            cursor.advanceToPosition(2);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            BlockCursorAssertions.assertCurrentValue(cursor, 10, 44.44);
        }

        // skip past end
        assertFalse(cursor.advanceToPosition(100));
    }

    @Test
    public void testAdvanceToNextValueAdvancesPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        // first, skip to middle of a block
        BlockCursorAssertions.assertNextValue(cursor, 0, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 1, 11.11);

        // force jump to next block
        BlockCursorAssertions.assertNextValue(cursor, 2, 11.11);
    }

    @Test
    public void testAdvanceToNextPositionAdvancesValue()
    {
        Cursor cursor = createCursor();

        // first, advance to end of a block
        BlockCursorAssertions.assertNextPosition(cursor, 0, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 1, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 2, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 3, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 4, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 5, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 6, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 7, 22.22);

        // force jump to next block
        BlockCursorAssertions.assertNextPosition(cursor, 8, 33.33);
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        Cursor cursor = createCursor();

        // first, advance to end of a block
        BlockCursorAssertions.assertNextPosition(cursor, 0, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 1, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 2, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 3, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 4, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 5, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 6, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 7, 22.22);

        // force jump to next block
        BlockCursorAssertions.assertNextValue(cursor, 8, 33.33);
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        BlockCursorAssertions.assertNextValue(cursor, 0, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 1, 11.11);
        BlockCursorAssertions.assertNextValue(cursor, 2, 11.11);
        BlockCursorAssertions.assertNextPosition(cursor, 3, 22.22);
        BlockCursorAssertions.assertNextValue(cursor, 4, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 5, 22.22);
        BlockCursorAssertions.assertNextValue(cursor, 6, 22.22);
        BlockCursorAssertions.assertNextPosition(cursor, 7, 22.22);
        BlockCursorAssertions.assertNextValue(cursor, 8, 33.33);
        BlockCursorAssertions.assertNextPosition(cursor, 9, 33.33);
        BlockCursorAssertions.assertNextValue(cursor, 10, 44.44);

        assertFalse(cursor.advanceNextPosition());
        assertFalse(cursor.advanceNextValue());
    }

    @Test
    public void testRange()
    {
        Cursor cursor = createCursor();
        Assert.assertEquals(cursor.getRange(), new Range(0, 10));
    }

    protected UncompressedBlock createTestBlock()
    {
        return Blocks.createDoublesBlock(0, 11.11, 11.11, 11.11, 22.22, 22.22, 22.22, 22.22, 22.22, 33.33, 33.33, 44.44);
    }

    protected abstract Cursor createCursor();
}
