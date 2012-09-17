/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.block.CursorAssertions.assertCurrentValue;
import static com.facebook.presto.block.CursorAssertions.assertNextPosition;
import static com.facebook.presto.block.CursorAssertions.assertNextValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestNonContiguousCursor extends AbstractTestCursor
{
    @BeforeClass
    public void setUp()
    {
        // verify expected values
        assertEquals(ImmutableList.copyOf(getExpectedValues().keySet()), ImmutableList.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 20L, 21L, 30L));
    }

    @Test
    public void testAdvanceToNextValueAdvancesPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        // first, skip to middle of a block
        assertNextValue(cursor, 0, getExpectedValue(0));
        assertNextPosition(cursor, 1, getExpectedValue(1));

        // force jump to next block
        assertNextValue(cursor, 2, getExpectedValue(2));
    }

    @Test
    public void testAdvanceToNextPositionAdvancesValue()
    {
        Cursor cursor = createCursor();

        // first, advance to end of a block
        assertTrue(cursor.advanceToPosition(7));

        // force jump to next block
        assertNextPosition(cursor, 20, getExpectedValue(20));
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        Cursor cursor = createCursor();

        // first, advance to end of a block
        assertTrue(cursor.advanceToPosition(7));

        // force jump to next block
        assertNextValue(cursor, 20, getExpectedValue(20));
    }

    @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        //
        // Note this code will more effective if the values are laid into blocks as follows:
        //
        //    0: A, A, A, B, B,
        //    5: B, B, B,
        //   20: C, C,
        //   30: D
        //

        Cursor cursor = createCursor();

        // advance to first position
        assertTrue(cursor.advanceToPosition(0));
        assertCurrentValue(cursor, 0, getExpectedValue(0));

        // skip to position in first block
        assertTrue(cursor.advanceToPosition(2));
        assertCurrentValue(cursor, 2, getExpectedValue(2));

        // advance to same position
        assertTrue(cursor.advanceToPosition(2));
        assertCurrentValue(cursor, 2, getExpectedValue(2));

        // skip to position in same block
        assertTrue(cursor.advanceToPosition(4));
        assertCurrentValue(cursor, 4, getExpectedValue(4));

        // skip to position in middle block
        assertTrue(cursor.advanceToPosition(21));
        assertCurrentValue(cursor, 21, getExpectedValue(21));

        // skip to position in gap
        assertTrue(cursor.advanceToPosition(25));
        assertCurrentValue(cursor, 30, getExpectedValue(30));

        // skip backwards
        try {
            cursor.advanceToPosition(20);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertCurrentValue(cursor, 30, getExpectedValue(30));
        }

        // skip past end
        assertFalse(cursor.advanceToPosition(100));

        assertTrue(cursor.isFinished());
    }
}
