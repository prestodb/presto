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
import static com.facebook.presto.block.cursor.BlockCursorAssertions.assertNextValue;
import static org.testng.Assert.assertFalse;
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

        assertFalse(cursor.hasNextValue());
    }

    @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        cursor.advanceToPosition(2);
        assertCurrentValue(cursor, 2, "apple");
        cursor.advanceToPosition(4);
        assertCurrentValue(cursor, 4, "banana");
        cursor.advanceToPosition(6);
        assertCurrentValue(cursor, 6, "banana");
        cursor.advanceToPosition(8);
        assertCurrentValue(cursor, 8, "cherry");
        cursor.advanceToPosition(10);
        assertCurrentValue(cursor, 10, "date");

        assertFalse(cursor.hasNextValue());
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
