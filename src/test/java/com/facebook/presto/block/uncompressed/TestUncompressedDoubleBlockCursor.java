/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.AbstractTestUncompressedDoubleBlockCursor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestUncompressedDoubleBlockCursor extends AbstractTestUncompressedDoubleBlockCursor
{
    @Test
    public void testGetSlice()
    {
        Cursor cursor = createCursor();
        try {
            cursor.getSlice(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }

        assertTrue(cursor.advanceNextValue());

        try {
            cursor.getSlice(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testGetLong()
    {
        Cursor cursor = createCursor();
        try {
            cursor.getLong(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }

        assertTrue(cursor.advanceNextValue());

        try {
            cursor.getLong(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedDoubleBlockCursor(createTestBlock());
    }
}
