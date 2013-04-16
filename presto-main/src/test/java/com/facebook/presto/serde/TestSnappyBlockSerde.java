/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import java.util.Random;

import static com.facebook.presto.tuple.Tuples.createTuple;
import static org.testng.Assert.assertTrue;

public class TestSnappyBlockSerde
{
    @Test
    public void testRoundTrip()
    {
        ImmutableList<Tuple> tuples = ImmutableList.of(
                createTuple("alice"),
                createTuple("bob"),
                createTuple("charlie"),
                createTuple("dave"));

        DynamicSliceOutput blockSlice = new DynamicSliceOutput(1024);

        DynamicSliceOutput compressedOutput = new DynamicSliceOutput(1024);
        Encoder encoder = BlocksFileEncoding.SNAPPY.createBlocksWriter(compressedOutput);

        for (Tuple tuple : tuples) {
            tuple.writeTo(blockSlice);
        }
        Block expectedBlock = new UncompressedBlock(tuples.size(), TupleInfo.SINGLE_VARBINARY, blockSlice.slice());

        encoder.append(tuples);
        BlockEncoding snappyEncoding = encoder.finish();
        Block actualBlock = snappyEncoding.readBlock(compressedOutput.slice().getInput());
        BlockAssertions.assertBlockEquals(actualBlock, expectedBlock);
    }

    @Test
    public void testLotsOfStuff()
    {
        ImmutableList<Tuple> tuples = ImmutableList.of(
                createTuple("alice"),
                createTuple("bob"),
                createTuple("charlie"),
                createTuple("dave"));

        DynamicSliceOutput blockSlice = new DynamicSliceOutput(1024);

        DynamicSliceOutput compressedOutput = new DynamicSliceOutput(1024);
        Encoder encoder = BlocksFileEncoding.SNAPPY.createBlocksWriter(compressedOutput);

        int count = 1000;
        Random r = new Random();
        for (int i = 0; i < count; i++) {
            int x = r.nextInt(tuples.size());
            tuples.get(x).writeTo(blockSlice);
            encoder.append(ImmutableSet.of(tuples.get(x)));
        }

        Block expectedBlock = new UncompressedBlock(count, TupleInfo.SINGLE_VARBINARY, blockSlice.slice());

        BlockEncoding snappyEncoding = encoder.finish();
        assertTrue(compressedOutput.size() < blockSlice.size());

        Block actualBlock = snappyEncoding.readBlock(compressedOutput.slice().getInput());

        BlockAssertions.assertBlockEquals(actualBlock, expectedBlock);
    }
}
