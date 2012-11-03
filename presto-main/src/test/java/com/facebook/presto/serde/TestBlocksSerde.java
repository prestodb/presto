/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.nblock.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.nblock.BlockAssertions.assertBlocksEquals;
import static com.facebook.presto.nblock.BlockIterables.createBlockIterable;
import static com.facebook.presto.serde.BlocksSerde.readBlocks;
import static com.facebook.presto.serde.BlocksSerde.writeBlocks;

public class TestBlocksSerde
{
    @Test
    public void testRoundTrip()
    {
        UncompressedBlock expectedBlock = new BlockBuilder(0, SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writeBlocks(sliceOutput, expectedBlock, expectedBlock, expectedBlock);
        BlockIterable actualBlocks = readBlocks(sliceOutput.slice(), 0);
        assertBlocksEquals(actualBlocks, createBlockIterable(new BlockBuilder(0, SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build()));
    }
}
