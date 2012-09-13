/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.cursor;

import com.facebook.presto.Blocks;
import com.facebook.presto.Range;
import com.facebook.presto.UncompressedValueBlock;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.block.cursor.BlockCursorAssertions.assertCurrentValue;
import static com.facebook.presto.block.cursor.BlockCursorAssertions.assertNextPosition;
import static com.facebook.presto.block.cursor.BlockCursorAssertions.assertNextValue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestUncompressedDoubleBlockCursor extends AbstractTestUncompressedBlockCursor
{
    @Test
    public void testGetDoubleState()
    {
        BlockCursor cursor = createCursor();
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
        BlockCursor cursor = createCursor();
        assertNextValue(cursor, 0, 11.11);
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();
        assertNextPosition(cursor, 0, 11.11);
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        assertNextValue(cursor, 0, 11.11);
        assertNextValue(cursor, 1, 11.11);
        assertNextValue(cursor, 2, 11.11);
        assertNextValue(cursor, 3, 22.22);
        assertNextValue(cursor, 4, 22.22);
        assertNextValue(cursor, 5, 22.22);
        assertNextValue(cursor, 6, 22.22);
        assertNextValue(cursor, 7, 22.22);
        assertNextValue(cursor, 8, 33.33);
        assertNextValue(cursor, 9, 33.33);
        assertNextValue(cursor, 10, 44.44);

        assertFalse(cursor.advanceToNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        BlockCursor cursor = createCursor();

        assertNextPosition(cursor, 0, 11.11);
        assertNextPosition(cursor, 1, 11.11);
        assertNextPosition(cursor, 2, 11.11);
        assertNextPosition(cursor, 3, 22.22);
        assertNextPosition(cursor, 4, 22.22);
        assertNextPosition(cursor, 5, 22.22);
        assertNextPosition(cursor, 6, 22.22);
        assertNextPosition(cursor, 7, 22.22);
        assertNextPosition(cursor, 8, 33.33);
        assertNextPosition(cursor, 9, 33.33);
        assertNextPosition(cursor, 10, 44.44);

        assertFalse(cursor.advanceNextPosition());
    }

   @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

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
        assertTrue(cursor.advanceToPosition(8));
        assertCurrentValue(cursor, 8, 33.33);

        // skip to position in gap
        assertTrue(cursor.advanceToPosition(10));
        assertCurrentValue(cursor, 10, 44.44);

        // skip backwards
        try {
            cursor.advanceToPosition(2);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertCurrentValue(cursor, 10, 44.44);
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
        assertNextValue(cursor, 0, 11.11);
        assertNextPosition(cursor, 1, 11.11);

        // force jump to next block
        assertNextValue(cursor, 2, 11.11);
    }

    @Test
    public void testAdvanceToNextPositionAdvancesValue()
    {
        BlockCursor cursor = createCursor();

        // first, advance to end of a block
        assertNextPosition(cursor, 0, 11.11);
        assertNextPosition(cursor, 1, 11.11);
        assertNextPosition(cursor, 2, 11.11);
        assertNextPosition(cursor, 3, 22.22);
        assertNextPosition(cursor, 4, 22.22);
        assertNextPosition(cursor, 5, 22.22);
        assertNextPosition(cursor, 6, 22.22);
        assertNextPosition(cursor, 7, 22.22);

        // force jump to next block
        assertNextPosition(cursor, 8, 33.33);
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        // first, advance to end of a block
        assertNextPosition(cursor, 0, 11.11);
        assertNextPosition(cursor, 1, 11.11);
        assertNextPosition(cursor, 2, 11.11);
        assertNextPosition(cursor, 3, 22.22);
        assertNextPosition(cursor, 4, 22.22);
        assertNextPosition(cursor, 5, 22.22);
        assertNextPosition(cursor, 6, 22.22);
        assertNextPosition(cursor, 7, 22.22);

        // force jump to next block
        assertNextValue(cursor, 8, 33.33);
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        assertNextValue(cursor, 0, 11.11);
        assertNextPosition(cursor, 1, 11.11);
        assertNextValue(cursor, 2, 11.11);
        assertNextPosition(cursor, 3, 22.22);
        assertNextValue(cursor, 4, 22.22);
        assertNextPosition(cursor, 5, 22.22);
        assertNextValue(cursor, 6, 22.22);
        assertNextPosition(cursor, 7, 22.22);
        assertNextValue(cursor, 8, 33.33);
        assertNextPosition(cursor, 9, 33.33);
        assertNextValue(cursor, 10, 44.44);

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
        return Blocks.createDoublesBlock(0, 11.11, 11.11, 11.11, 22.22, 22.22, 22.22, 22.22, 22.22, 33.33, 33.33, 44.44);
    }

    protected abstract BlockCursor createCursor();
}
