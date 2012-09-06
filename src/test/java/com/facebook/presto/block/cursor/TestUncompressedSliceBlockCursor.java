/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.cursor;

import com.facebook.presto.block.cursor.UncompressedSliceBlockCursor;
import com.facebook.presto.block.cursor.BlockCursor;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

public class TestUncompressedSliceBlockCursor extends AbstractTestUncompressedSliceBlockCursor
{
    @Test
    public void testGetLong()
    {
        BlockCursor cursor = createCursor();
        try {
            cursor.getLong(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }

        cursor.advanceNextValue();

        try {
            cursor.getLong(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }
    }

    @Override
    protected BlockCursor createCursor()
    {
        return new UncompressedSliceBlockCursor(createTestBlock());
    }
}
