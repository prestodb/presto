/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.AbstractTestUncompressedSliceBlockCursor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestUncompressedSliceBlockCursor extends AbstractTestUncompressedSliceBlockCursor
{
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
        return new UncompressedSliceBlockCursor(createTestBlock());
    }
}
