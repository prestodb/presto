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
        assertNextValue(cursor, 0, 1111L);
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();
        assertNextPosition(cursor, 0, 1111L);
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        assertNextValue(cursor, 0, 1111L);
        assertNextValue(cursor, 1, 1111L);
        assertNextValue(cursor, 2, 1111L);
        assertNextValue(cursor, 3, 2222L);
        assertNextValue(cursor, 4, 2222L);
        assertNextValue(cursor, 5, 2222L);
        assertNextValue(cursor, 6, 2222L);
        assertNextValue(cursor, 7, 2222L);
        assertNextValue(cursor, 8, 3333L);
        assertNextValue(cursor, 9, 3333L);
        assertNextValue(cursor, 10, 4444L);

        assertFalse(cursor.advanceToNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        BlockCursor cursor = createCursor();

        assertNextPosition(cursor, 0, 1111L);
        assertNextPosition(cursor, 1, 1111L);
        assertNextPosition(cursor, 2, 1111L);
        assertNextPosition(cursor, 3, 2222L);
        assertNextPosition(cursor, 4, 2222L);
        assertNextPosition(cursor, 5, 2222L);
        assertNextPosition(cursor, 6, 2222L);
        assertNextPosition(cursor, 7, 2222L);
        assertNextPosition(cursor, 8, 3333L);
        assertNextPosition(cursor, 9, 3333L);
        assertNextPosition(cursor, 10, 4444L);

        assertFalse(cursor.advanceNextPosition());
    }

   @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        // advance to first position
        assertTrue(cursor.advanceToPosition(0));
        assertCurrentValue(cursor, 0, 1111L);

        // skip to position in first block
        assertTrue(cursor.advanceToPosition(2));
        assertCurrentValue(cursor, 2, 1111L);

        // advance to same position
        assertTrue(cursor.advanceToPosition(2));
        assertCurrentValue(cursor, 2, 1111L);

        // skip to position in same block
        assertTrue(cursor.advanceToPosition(4));
        assertCurrentValue(cursor, 4, 2222L);

        // skip to position in middle block
        assertTrue(cursor.advanceToPosition(8));
        assertCurrentValue(cursor, 8, 3333L);

        // skip to position in gap
        assertTrue(cursor.advanceToPosition(10));
        assertCurrentValue(cursor, 10, 4444L);

        // skip backwards
        try {
            cursor.advanceToPosition(2);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertCurrentValue(cursor, 10, 4444L);
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
        assertNextValue(cursor, 0, 1111L);
        assertNextPosition(cursor, 1, 1111L);

        // force jump to next block
        assertNextValue(cursor, 2, 1111L);
    }

    @Test
    public void testAdvanceToNextPositionAdvancesValue()
    {
        BlockCursor cursor = createCursor();

        // first, advance to end of a block
        assertNextPosition(cursor, 0, 1111L);
        assertNextPosition(cursor, 1, 1111L);
        assertNextPosition(cursor, 2, 1111L);
        assertNextPosition(cursor, 3, 2222L);
        assertNextPosition(cursor, 4, 2222L);
        assertNextPosition(cursor, 5, 2222L);
        assertNextPosition(cursor, 6, 2222L);
        assertNextPosition(cursor, 7, 2222L);

        // force jump to next block
        assertNextPosition(cursor, 8, 3333L);
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        // first, advance to end of a block
        assertNextPosition(cursor, 0, 1111L);
        assertNextPosition(cursor, 1, 1111L);
        assertNextPosition(cursor, 2, 1111L);
        assertNextPosition(cursor, 3, 2222L);
        assertNextPosition(cursor, 4, 2222L);
        assertNextPosition(cursor, 5, 2222L);
        assertNextPosition(cursor, 6, 2222L);
        assertNextPosition(cursor, 7, 2222L);

        // force jump to next block
        assertNextValue(cursor, 8, 3333L);
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        assertNextValue(cursor, 0, 1111L);
        assertNextPosition(cursor, 1, 1111L);
        assertNextValue(cursor, 2, 1111L);
        assertNextPosition(cursor, 3, 2222L);
        assertNextValue(cursor, 4, 2222L);
        assertNextPosition(cursor, 5, 2222L);
        assertNextValue(cursor, 6, 2222L);
        assertNextPosition(cursor, 7, 2222L);
        assertNextValue(cursor, 8, 3333L);
        assertNextPosition(cursor, 9, 3333L);
        assertNextValue(cursor, 10, 4444L);

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
        return createBlock(0, 1111L, 1111L, 1111L, 2222L, 2222L, 2222L, 2222L, 2222L, 3333L, 3333L, 4444L);
    }

    protected abstract BlockCursor createCursor();
}
