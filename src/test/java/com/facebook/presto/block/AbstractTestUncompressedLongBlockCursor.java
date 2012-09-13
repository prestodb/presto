/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.block.Blocks;
import com.facebook.presto.Range;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedValueBlock;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockCursorAssertions.assertCurrentValue;
import static com.facebook.presto.block.BlockCursorAssertions.assertNextPosition;
import static com.facebook.presto.block.BlockCursorAssertions.assertNextValue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestUncompressedLongBlockCursor extends AbstractTestUncompressedBlockCursor
{
    @Test
    public void testGetLongState()
    {
        BlockCursor cursor = createCursor();
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
        BlockCursor cursor = createCursor();
        BlockCursorAssertions.assertNextValue(cursor, 0, 1111L);
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();
        BlockCursorAssertions.assertNextPosition(cursor, 0, 1111L);
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        BlockCursorAssertions.assertNextValue(cursor, 0, 1111L);
        BlockCursorAssertions.assertNextValue(cursor, 1, 1111L);
        BlockCursorAssertions.assertNextValue(cursor, 2, 1111L);
        BlockCursorAssertions.assertNextValue(cursor, 3, 2222L);
        BlockCursorAssertions.assertNextValue(cursor, 4, 2222L);
        BlockCursorAssertions.assertNextValue(cursor, 5, 2222L);
        BlockCursorAssertions.assertNextValue(cursor, 6, 2222L);
        BlockCursorAssertions.assertNextValue(cursor, 7, 2222L);
        BlockCursorAssertions.assertNextValue(cursor, 8, 3333L);
        BlockCursorAssertions.assertNextValue(cursor, 9, 3333L);
        BlockCursorAssertions.assertNextValue(cursor, 10, 4444L);

        assertFalse(cursor.advanceToNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        BlockCursor cursor = createCursor();

        BlockCursorAssertions.assertNextPosition(cursor, 0, 1111L);
        BlockCursorAssertions.assertNextPosition(cursor, 1, 1111L);
        BlockCursorAssertions.assertNextPosition(cursor, 2, 1111L);
        BlockCursorAssertions.assertNextPosition(cursor, 3, 2222L);
        BlockCursorAssertions.assertNextPosition(cursor, 4, 2222L);
        BlockCursorAssertions.assertNextPosition(cursor, 5, 2222L);
        BlockCursorAssertions.assertNextPosition(cursor, 6, 2222L);
        BlockCursorAssertions.assertNextPosition(cursor, 7, 2222L);
        BlockCursorAssertions.assertNextPosition(cursor, 8, 3333L);
        BlockCursorAssertions.assertNextPosition(cursor, 9, 3333L);
        BlockCursorAssertions.assertNextPosition(cursor, 10, 4444L);

        assertFalse(cursor.advanceNextPosition());
    }

   @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        // advance to first position
        assertTrue(cursor.advanceToPosition(0));
        BlockCursorAssertions.assertCurrentValue(cursor, 0, 1111L);

        // skip to position in first block
        assertTrue(cursor.advanceToPosition(2));
        BlockCursorAssertions.assertCurrentValue(cursor, 2, 1111L);

        // advance to same position
        assertTrue(cursor.advanceToPosition(2));
        BlockCursorAssertions.assertCurrentValue(cursor, 2, 1111L);

        // skip to position in same block
        assertTrue(cursor.advanceToPosition(4));
        BlockCursorAssertions.assertCurrentValue(cursor, 4, 2222L);

        // skip to position in middle block
        assertTrue(cursor.advanceToPosition(8));
        BlockCursorAssertions.assertCurrentValue(cursor, 8, 3333L);

        // skip to position in gap
        assertTrue(cursor.advanceToPosition(10));
        BlockCursorAssertions.assertCurrentValue(cursor, 10, 4444L);

        // skip backwards
        try {
            cursor.advanceToPosition(2);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            BlockCursorAssertions.assertCurrentValue(cursor, 10, 4444L);
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
        BlockCursorAssertions.assertNextValue(cursor, 0, 1111L);
        BlockCursorAssertions.assertNextPosition(cursor, 1, 1111L);

        // force jump to next block
        BlockCursorAssertions.assertNextValue(cursor, 2, 1111L);
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        BlockCursorAssertions.assertNextValue(cursor, 0, 1111L);
        BlockCursorAssertions.assertNextPosition(cursor, 1, 1111L);
        BlockCursorAssertions.assertNextValue(cursor, 2, 1111L);
        BlockCursorAssertions.assertNextPosition(cursor, 3, 2222L);
        BlockCursorAssertions.assertNextValue(cursor, 4, 2222L);
        BlockCursorAssertions.assertNextPosition(cursor, 5, 2222L);
        BlockCursorAssertions.assertNextValue(cursor, 6, 2222L);
        BlockCursorAssertions.assertNextPosition(cursor, 7, 2222L);
        BlockCursorAssertions.assertNextValue(cursor, 8, 3333L);
        BlockCursorAssertions.assertNextPosition(cursor, 9, 3333L);
        BlockCursorAssertions.assertNextValue(cursor, 10, 4444L);

        assertFalse(cursor.advanceNextPosition());
        assertFalse(cursor.advanceToNextValue());
    }

    @Test
    public void testRange()
    {
        BlockCursor cursor = createCursor();
        Assert.assertEquals(cursor.getRange(), new Range(0, 10));
    }

    protected UncompressedValueBlock createTestBlock()
    {
        return Blocks.createLongsBlock(0, 1111L, 1111L, 1111L, 2222L, 2222L, 2222L, 2222L, 2222L, 3333L, 3333L, 4444L);
    }

    protected abstract BlockCursor createCursor();
}
