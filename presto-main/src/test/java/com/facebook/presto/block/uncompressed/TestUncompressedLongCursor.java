/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractTestNonContiguousCursor;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.TupleStream;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.fail;

public class TestUncompressedLongCursor extends AbstractTestNonContiguousCursor
{
    @Test
    public void testConstructorNulls()
            throws Exception
    {
        try {
            new UncompressedLongCursor(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException expected) {
        }
    }

    protected List<UncompressedBlock> createBlocks()
    {
        return ImmutableList.of(
                Blocks.createLongsBlock(0, 1111L, 1111L, 1111L, 2222L, 2222L),
                Blocks.createLongsBlock(5, 2222L, 2222L, 2222L),
                Blocks.createLongsBlock(20, 3333L, 3333L),
                Blocks.createLongsBlock(30, 4444L));
    }

    @Override
    protected TupleStream createExpectedValues()
    {
        return new GenericTupleStream<>(TupleInfo.SINGLE_DOUBLE, createBlocks());
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedLongCursor(createBlocks().iterator());
    }
}
