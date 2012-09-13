/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.AbstractTestUncompressedLongBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedLongBlockCursor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestUncompressedLongBlockCursor extends AbstractTestUncompressedLongBlockCursor
{
    @Test
    public void testGetSlice()
    {
        BlockCursor cursor = createCursor();
        try {
            cursor.getSlice(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }

        assertTrue(cursor.advanceToNextValue());

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
        BlockCursor cursor = createCursor();
        try {
            cursor.getDouble(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }

        assertTrue(cursor.advanceToNextValue());

        try {
            cursor.getDouble(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }
    }

    @Override
    protected BlockCursor createCursor()
    {
        return new UncompressedLongBlockCursor(createTestBlock());
    }
}
