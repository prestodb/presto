/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;
import org.testng.annotations.Test;

import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestUncompressedBlockCursorWithNulls
        extends AbstractTestBlockCursor
{
    @Override
    protected Block createExpectedValues()
    {
        return new BlockBuilder(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64))
                .appendNull()
                .appendNull()
                .append("apple")
                .append(12)
                .appendNull()
                .append(13)
                .append("banana")
                .appendNull()
                .append("banana")
                .append(15)
                .appendNull()
                .appendNull()
                .append("banana")
                .append(17)
                .append("banana")
                .appendNull()
                .append("cherry")
                .append(19)
                .appendNull()
                .append(20)
                .appendNull()
                .appendNull()
                .build();
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createExpectedValues().cursor(), UncompressedBlockCursor.class);
    }
}
