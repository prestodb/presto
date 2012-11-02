/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.nblock.uncompressed;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.AbstractTestBlockCursor;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockBuilder;
import org.testng.annotations.Test;

import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestUncompressedBlockCursor extends AbstractTestBlockCursor
{
    @Override
    protected Block createExpectedValues()
    {
        return new BlockBuilder(0, new TupleInfo(VARIABLE_BINARY, FIXED_INT_64))
                .append("apple")
                .append(11)
                .append("apple")
                .append(12)
                .append("apple")
                .append(13)
                .append("banana")
                .append(14)
                .append("banana")
                .append(15)
                .append("banana")
                .append(16)
                .append("banana")
                .append(17)
                .append("banana")
                .append(18)
                .append("cherry")
                .append(19)
                .append("cherry")
                .append(20)
                .append("date")
                .append(21)
                .build();
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createExpectedValues().cursor(), UncompressedBlockCursor.class);
    }
}
