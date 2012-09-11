/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.cursor;

import com.facebook.presto.Range;
import com.facebook.presto.UncompressedValueBlock;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.Blocks.createBlock;
import static com.facebook.presto.block.cursor.BlockCursorAssertions.assertCurrentValue;
import static com.facebook.presto.block.cursor.BlockCursorAssertions.assertNextPosition;
import static com.facebook.presto.block.cursor.BlockCursorAssertions.assertNextValue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestUncompressedSliceBlockCursor extends AbstractTestUncompressedBlockCursor
{
    @Test
    public void testGetSliceState()
    {
        BlockCursor cursor = createCursor();
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
        BlockCursor cursor = createCursor();
        assertNextValue(cursor, 0, "apple");
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();
        assertNextPosition(cursor, 0, "apple");
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextValue(cursor, 1, "apple");
        assertNextValue(cursor, 2, "apple");
        assertNextValue(cursor, 3, "banana");
        assertNextValue(cursor, 4, "banana");
        assertNextValue(cursor, 5, "banana");
        assertNextValue(cursor, 6, "banana");
        assertNextValue(cursor, 7, "banana");
        assertNextValue(cursor, 8, "cherry");
        assertNextValue(cursor, 9, "cherry");
        assertNextValue(cursor, 10, "date");

        assertFalse(cursor.advanceToNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        BlockCursor cursor = createCursor();

        assertNextPosition(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextPosition(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextPosition(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextPosition(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");
        assertNextPosition(cursor, 8, "cherry");
        assertNextPosition(cursor, 9, "cherry");
        assertNextPosition(cursor, 10, "date");

        assertFalse(cursor.advanceNextPosition());
    }

   @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        // advance to first position
        assertTrue(cursor.advanceToPosition(0));
        assertCurrentValue(cursor, 0, "apple");

        // skip to position in first block
        assertTrue(cursor.advanceToPosition(2));
        assertCurrentValue(cursor, 2, "apple");

        // advance to same position
        assertTrue(cursor.advanceToPosition(2));
        assertCurrentValue(cursor, 2, "apple");

        // skip to position in same block
        assertTrue(cursor.advanceToPosition(4));
        assertCurrentValue(cursor, 4, "banana");

        // skip to position in middle block
        assertTrue(cursor.advanceToPosition(8));
        assertCurrentValue(cursor, 8, "cherry");

        // skip to position in gap
        assertTrue(cursor.advanceToPosition(10));
        assertCurrentValue(cursor, 10, "date");

        // skip backwards
        try {
            cursor.advanceToPosition(2);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertCurrentValue(cursor, 10, "date");
        }

        // skip past end
        assertFalse(cursor.advanceToPosition(100));
    }

    @Test
    public void testAdvanceToNextValueAdvancesPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        // first, skip to middle of a block
        assertNextValue(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");

        // force jump to next block
        assertNextValue(cursor, 2, "apple");
    }

    @Test
    public void testAdvanceToNextPositionAdvancesValue()
    {
        BlockCursor cursor = createCursor();

        // first, advance to end of a block
        assertNextPosition(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextPosition(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextPosition(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextPosition(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");

        // force jump to next block
        assertNextPosition(cursor, 8, "cherry");
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        // first, advance to end of a block
        assertNextPosition(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextPosition(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextPosition(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextPosition(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");

        // force jump to next block
        assertNextValue(cursor, 8, "cherry");
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextValue(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextValue(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextValue(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");
        assertNextValue(cursor, 8, "cherry");
        assertNextPosition(cursor, 9, "cherry");
        assertNextValue(cursor, 10, "date");

        assertFalse(cursor.advanceNextPosition());
        assertFalse(cursor.advanceToNextValue());
    }

    @Test
    public void testRange()
    {
        BlockCursor cursor = createCursor();
        Assert.assertEquals(cursor.getRange(), new Range(0, 10));
    }

    public UncompressedValueBlock createTestBlock()
    {
        return createBlock(0, "apple", "apple", "apple", "banana", "banana", "banana", "banana", "banana", "cherry", "cherry", "date");
    }

}
