/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.AbstractTestUncompressedLongBlockCursor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestUncompressedLongBlockCursor extends AbstractTestUncompressedLongBlockCursor
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
    public void testGetDouble()
    {
        Cursor cursor = createCursor();
        try {
            cursor.getDouble(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }

        assertTrue(cursor.advanceNextValue());

        try {
            cursor.getDouble(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedLongBlockCursor(createTestBlock());
    }
}
