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

public class TestUncompressedDoubleCursor extends AbstractTestNonContiguousCursor
{
    @Test
    public void testConstructorNulls()
            throws Exception
    {
        try {
            new UncompressedDoubleCursor(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException expected) {
        }
    }

    protected List<UncompressedBlock> createBlocks()
    {
        return ImmutableList.of(
                Blocks.createDoublesBlock(0, 11.11, 11.11, 11.11, 22.22, 22.22),
                Blocks.createDoublesBlock(5, 22.22, 22.22, 22.22),
                Blocks.createDoublesBlock(20, 33.33, 33.33),
                Blocks.createDoublesBlock(30, 44.44));
    }

    @Override
    protected TupleStream createExpectedValues()
    {
        return new GenericTupleStream<>(TupleInfo.SINGLE_DOUBLE, createBlocks());
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedDoubleCursor(createBlocks().iterator());
    }
}
