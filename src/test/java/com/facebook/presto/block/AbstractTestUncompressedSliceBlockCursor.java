/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.block.Blocks.createBlock;
import static com.facebook.presto.block.BlockCursorAssertions.assertCurrentValue;
import static com.facebook.presto.block.BlockCursorAssertions.assertNextPosition;
import static com.facebook.presto.block.BlockCursorAssertions.assertNextValue;
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
        BlockCursorAssertions.assertNextValue(cursor, 0, "apple");
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();
        BlockCursorAssertions.assertNextPosition(cursor, 0, "apple");
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        BlockCursorAssertions.assertNextValue(cursor, 0, "apple");
        BlockCursorAssertions.assertNextValue(cursor, 1, "apple");
        BlockCursorAssertions.assertNextValue(cursor, 2, "apple");
        BlockCursorAssertions.assertNextValue(cursor, 3, "banana");
        BlockCursorAssertions.assertNextValue(cursor, 4, "banana");
        BlockCursorAssertions.assertNextValue(cursor, 5, "banana");
        BlockCursorAssertions.assertNextValue(cursor, 6, "banana");
        BlockCursorAssertions.assertNextValue(cursor, 7, "banana");
        BlockCursorAssertions.assertNextValue(cursor, 8, "cherry");
        BlockCursorAssertions.assertNextValue(cursor, 9, "cherry");
        BlockCursorAssertions.assertNextValue(cursor, 10, "date");

        assertFalse(cursor.advanceToNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        BlockCursor cursor = createCursor();

        BlockCursorAssertions.assertNextPosition(cursor, 0, "apple");
        BlockCursorAssertions.assertNextPosition(cursor, 1, "apple");
        BlockCursorAssertions.assertNextPosition(cursor, 2, "apple");
        BlockCursorAssertions.assertNextPosition(cursor, 3, "banana");
        BlockCursorAssertions.assertNextPosition(cursor, 4, "banana");
        BlockCursorAssertions.assertNextPosition(cursor, 5, "banana");
        BlockCursorAssertions.assertNextPosition(cursor, 6, "banana");
        BlockCursorAssertions.assertNextPosition(cursor, 7, "banana");
        BlockCursorAssertions.assertNextPosition(cursor, 8, "cherry");
        BlockCursorAssertions.assertNextPosition(cursor, 9, "cherry");
        BlockCursorAssertions.assertNextPosition(cursor, 10, "date");

        assertFalse(cursor.advanceNextPosition());
    }

   @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        // advance to first position
        assertTrue(cursor.advanceToPosition(0));
        BlockCursorAssertions.assertCurrentValue(cursor, 0, "apple");

        // skip to position in first block
        assertTrue(cursor.advanceToPosition(2));
        BlockCursorAssertions.assertCurrentValue(cursor, 2, "apple");

        // advance to same position
        assertTrue(cursor.advanceToPosition(2));
        BlockCursorAssertions.assertCurrentValue(cursor, 2, "apple");

        // skip to position in same block
        assertTrue(cursor.advanceToPosition(4));
        BlockCursorAssertions.assertCurrentValue(cursor, 4, "banana");

        // skip to position in middle block
        assertTrue(cursor.advanceToPosition(8));
        BlockCursorAssertions.assertCurrentValue(cursor, 8, "cherry");

        // skip to position in gap
        assertTrue(cursor.advanceToPosition(10));
        BlockCursorAssertions.assertCurrentValue(cursor, 10, "date");

        // skip backwards
        try {
            cursor.advanceToPosition(2);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            BlockCursorAssertions.assertCurrentValue(cursor, 10, "date");
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
        BlockCursorAssertions.assertNextValue(cursor, 0, "apple");
        BlockCursorAssertions.assertNextPosition(cursor, 1, "apple");

        // force jump to next block
        BlockCursorAssertions.assertNextValue(cursor, 2, "apple");
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        BlockCursorAssertions.assertNextValue(cursor, 0, "apple");
        BlockCursorAssertions.assertNextPosition(cursor, 1, "apple");
        BlockCursorAssertions.assertNextValue(cursor, 2, "apple");
        BlockCursorAssertions.assertNextPosition(cursor, 3, "banana");
        BlockCursorAssertions.assertNextValue(cursor, 4, "banana");
        BlockCursorAssertions.assertNextPosition(cursor, 5, "banana");
        BlockCursorAssertions.assertNextValue(cursor, 6, "banana");
        BlockCursorAssertions.assertNextPosition(cursor, 7, "banana");
        BlockCursorAssertions.assertNextValue(cursor, 8, "cherry");
        BlockCursorAssertions.assertNextPosition(cursor, 9, "cherry");
        BlockCursorAssertions.assertNextValue(cursor, 10, "date");

        assertFalse(cursor.advanceNextPosition());
        assertFalse(cursor.advanceToNextValue());
    }

    @Test
    public void testRange()
    {
        BlockCursor cursor = createCursor();
        Assert.assertEquals(cursor.getRange(), new Range(0, 10));
    }

    public UncompressedBlock createTestBlock()
    {
        return createBlock(0, "apple", "apple", "apple", "banana", "banana", "banana", "banana", "banana", "cherry", "cherry", "date");
    }

}
