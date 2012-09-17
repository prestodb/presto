/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedTupleStream;
import com.facebook.presto.operator.GenericCursor;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.block.Blocks.createBlock;

public class TestGenericCursor
        extends AbstractTestNonContiguousCursor
{
    private List<UncompressedBlock> createBlocks()
    {
        return ImmutableList.of(
                createBlock(0, "apple", "apple", "apple", "banana", "banana"),
                createBlock(5, "banana", "banana", "banana"),
                createBlock(20, "cherry", "cherry"),
                createBlock(30, "date"));
    }

    @Override
    protected TupleStream createExpectedValues()
    {
        return new UncompressedTupleStream(TupleInfo.SINGLE_VARBINARY, createBlocks());
    }

    @Override
    protected Cursor createCursor()
    {
        return new GenericCursor(TupleInfo.SINGLE_VARBINARY, createBlocks().iterator());
    }
}
