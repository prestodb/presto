/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.AbstractTestContiguousCursor;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.Cursor;

public class TestUncompressedDoubleBlockCursor extends AbstractTestContiguousCursor
{
    @Override
    protected UncompressedBlock createExpectedValues()
    {
        return Blocks.createDoublesBlock(0, 11.11, 11.11, 11.11, 22.22, 22.22, 22.22, 22.22, 22.22, 33.33, 33.33, 44.44);
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedDoubleBlockCursor(createExpectedValues());
    }
}
