/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.AbstractTestUncompressedLongBlockCursor;

public class TestUncompressedBlockCursorLong extends AbstractTestUncompressedLongBlockCursor
{
    @Override
    protected Cursor createCursor()
    {
        return new UncompressedBlockCursor(createTestBlock());
    }
}
