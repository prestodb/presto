/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.serde.UncompressedBlockSerde.UNCOMPRESSED_BLOCK_SERDE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.Tuples.createTuple;

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
        ImmutableList<Tuple> tuples = ImmutableList.of(createTuple("alice"),
                createTuple("bob"),
                createTuple("charlie"),
                createTuple("dave"));

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        UNCOMPRESSED_BLOCK_SERDE.createBlocksWriter(sliceOutput).append(tuples).append(tuples).finish();
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
