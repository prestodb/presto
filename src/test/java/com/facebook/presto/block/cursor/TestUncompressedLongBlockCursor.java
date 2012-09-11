/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.cursor;

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

    @Override
    protected BlockCursor createCursor()
    {
        return new UncompressedLongBlockCursor(createTestBlock());
    }
}
