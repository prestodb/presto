/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.AbstractTestUncompressedSliceBlockCursor;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

public class TestUncompressedBlockCursorSlice extends AbstractTestUncompressedSliceBlockCursor
{
    @Test
    public void testGetLongState()
    {
        Cursor cursor = createCursor();
        try {
            cursor.getLong(0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testGetSlice()
    {
        Cursor cursor = createCursor();
        try {
            cursor.getSlice(0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedBlockCursor(createTestBlock());
    }
}
