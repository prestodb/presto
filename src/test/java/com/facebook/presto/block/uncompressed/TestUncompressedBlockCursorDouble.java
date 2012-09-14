/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.AbstractTestUncompressedDoubleBlockCursor;

public class TestUncompressedBlockCursorDouble extends AbstractTestUncompressedDoubleBlockCursor
{
    @Override
    protected Cursor createCursor()
    {
        return new UncompressedBlockCursor(createTestBlock());
    }
}
