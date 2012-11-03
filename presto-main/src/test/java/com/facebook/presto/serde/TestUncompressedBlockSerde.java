/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockAssertions;
import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.serde.UncompressedBlockSerde.UNCOMPRESSED_BLOCK_SERDE;

public class TestUncompressedBlockSerde
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
        UNCOMPRESSED_BLOCK_SERDE.writeBlock(sliceOutput, expectedBlock);
        Block actualBlock = UNCOMPRESSED_BLOCK_SERDE.readBlock(sliceOutput.slice().getInput(), SINGLE_VARBINARY, 0);
        BlockAssertions.assertBlockEquals(actualBlock, expectedBlock);
    }

    @Test
    public void testCreateBlockWriter()
    {
        UncompressedBlock block = new BlockBuilder(0, SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        UNCOMPRESSED_BLOCK_SERDE.createBlockWriter(sliceOutput).append(block).append(block).finish();
        Block actualBlock = UNCOMPRESSED_BLOCK_SERDE.readBlock(sliceOutput.slice().getInput(), SINGLE_VARBINARY, 0);
        BlockAssertions.assertBlockEquals(actualBlock, new BlockBuilder(0, SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build());
    }
}
