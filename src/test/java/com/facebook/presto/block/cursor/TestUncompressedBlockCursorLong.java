/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.cursor;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.cursor.UncompressedBlockCursor;
import com.facebook.presto.block.cursor.BlockCursor;

public class TestUncompressedBlockCursorLong extends AbstractTestUncompressedLongBlockCursor
{
    @Override
    protected BlockCursor createCursor()
    {
        return new UncompressedBlockCursor(new TupleInfo(Type.FIXED_INT_64), createTestBlock());
    }
}
