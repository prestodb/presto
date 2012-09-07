/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.cursor;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.cursor.UncompressedBlockCursor;
import com.facebook.presto.block.cursor.BlockCursor;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

public class TestUncompressedBlockCursorSlice extends AbstractTestUncompressedSliceBlockCursor
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
    public void testGetSlice()
    {
        BlockCursor cursor = createCursor();
        try {
            cursor.getSlice(0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }
    }

    @Override
    protected BlockCursor createCursor()
    {
        return new UncompressedBlockCursor(new TupleInfo(Type.VARIABLE_BINARY), createTestBlock());
    }
}
