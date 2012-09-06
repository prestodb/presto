/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.cursor;

import com.facebook.presto.Range;
import com.facebook.presto.UncompressedValueBlock;
import com.facebook.presto.block.cursor.BlockCursor;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.Blocks.createBlock;
import static com.facebook.presto.block.cursor.BlockCursorAssertions.assertCurrentValue;
import static org.testng.Assert.assertFalse;
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

        assertFalse(cursor.hasNextValue());
    }

    @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        cursor.advanceToPosition(2);
        assertCurrentValue(cursor, 2, 1111L);
        cursor.advanceToPosition(4);
        assertCurrentValue(cursor, 4, 2222L);
        cursor.advanceToPosition(6);
        assertCurrentValue(cursor, 6, 2222L);
        cursor.advanceToPosition(8);
        assertCurrentValue(cursor, 8, 3333L);
        cursor.advanceToPosition(10);
        assertCurrentValue(cursor, 10, 4444L);

        assertFalse(cursor.hasNextValue());
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
