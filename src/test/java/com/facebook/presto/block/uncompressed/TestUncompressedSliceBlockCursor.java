/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.AbstractTestUncompressedSliceBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedSliceBlockCursor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
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

        assertTrue(cursor.advanceToNextValue());

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
