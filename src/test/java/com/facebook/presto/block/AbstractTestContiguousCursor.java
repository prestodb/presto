/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceToPosition;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestContiguousCursor extends AbstractTestCursor
{
    @BeforeClass
    public void setUp()
    {
        // verify expected values
        assertEquals(ImmutableList.copyOf(getExpectedValues().keySet()), ImmutableList.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
    }

    @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        //
        // Note this code will more effective if the values are laid out as follows:
        //
        //   A, A, A, B, B, B, B, B, C, C, D
        //

        Cursor cursor = createCursor();

        // advance to first position
        assertAdvanceToPosition(cursor, 0);
        CursorAssertions.assertCurrentValue(cursor, 0, getExpectedValue(0));

        // skip to position in first value
        assertAdvanceToPosition(cursor, 2);
        CursorAssertions.assertCurrentValue(cursor, 2, getExpectedValue(2));

        // advance to same position
        assertAdvanceToPosition(cursor, 2);
        CursorAssertions.assertCurrentValue(cursor, 2, getExpectedValue(2));

        // skip to position in next value
        assertAdvanceToPosition(cursor, 4);
        CursorAssertions.assertCurrentValue(cursor, 4, getExpectedValue(4));

        // skip to position in third value
        assertAdvanceToPosition(cursor, 8);
        CursorAssertions.assertCurrentValue(cursor, 8, getExpectedValue(8));

        // skip to last position
        assertAdvanceToPosition(cursor, 10);
        CursorAssertions.assertCurrentValue(cursor, 10, getExpectedValue(10));

        // skip backwards
        try {
            cursor.advanceToPosition(2);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            CursorAssertions.assertCurrentValue(cursor, 10, getExpectedValue(10));
        }

        // skip past end
        assertAdvanceToPosition(cursor, 100, FINISHED);
        assertTrue(cursor.isFinished());
        assertFalse(cursor.isValid());
    }

    @Test
    public void testRange()
    {
        Cursor cursor = createCursor();
        Assert.assertEquals(cursor.getRange(), new Range(0, 10));
    }
}
