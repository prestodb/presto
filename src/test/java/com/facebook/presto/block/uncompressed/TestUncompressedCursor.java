package com.facebook.presto.block.uncompressed;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractTestNonContiguousCursor;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.TupleStream;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.block.Blocks.createBlock;

public class TestUncompressedCursor extends AbstractTestNonContiguousCursor
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
        return new GenericTupleStream<>(TupleInfo.SINGLE_VARBINARY, createBlocks());
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedCursor(TupleInfo.SINGLE_VARBINARY, createBlocks().iterator());
    }
}
