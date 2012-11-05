/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.block.BlockAssertions.assertBlocksEquals;
import static com.facebook.presto.block.BlockIterables.createBlockIterable;
import static com.facebook.presto.serde.UncompressedBlockSerde.UNCOMPRESSED_BLOCK_SERDE;

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

        SimpleBlocksSerde blocksSerde = new SimpleBlocksSerde(UNCOMPRESSED_BLOCK_SERDE);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blocksSerde.writeBlocks(sliceOutput, expectedBlock, expectedBlock, expectedBlock);
        BlockIterable actualBlocks = blocksSerde.createBlocksReader(sliceOutput.slice(), 0);
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
