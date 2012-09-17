/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.AbstractTestContiguousCursor;
import com.facebook.presto.block.Cursor;

import static com.facebook.presto.block.Blocks.createBlock;

public class TestUncompressedSliceBlockCursor extends AbstractTestContiguousCursor
{
    @Override
    public UncompressedBlock createExpectedValues()
    {
        return createBlock(0, "apple", "apple", "apple", "banana", "banana", "banana", "banana", "banana", "cherry", "cherry", "date");
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedSliceBlockCursor(createExpectedValues());
    }
}
