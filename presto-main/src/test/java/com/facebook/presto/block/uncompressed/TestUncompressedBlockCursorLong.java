/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.AbstractTestContiguousCursor;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.Cursor;

public class TestUncompressedBlockCursorLong extends AbstractTestContiguousCursor
{
    protected UncompressedBlock createExpectedValues()
    {
        return Blocks.createLongsBlock(0, 1111L, 1111L, 1111L, 2222L, 2222L, 2222L, 2222L, 2222L, 3333L, 3333L, 4444L);
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedBlockCursor(createExpectedValues());
    }
}
