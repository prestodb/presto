/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.nblock.uncompressed.UncompressedBlock;
import com.facebook.presto.serde.StatsCollectingBlocksSerde.StatsCollector.Stats;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.presto.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.nblock.BlockAssertions.assertBlocksEquals;
import static com.facebook.presto.nblock.BlockIterables.createBlockIterable;
import static com.facebook.presto.serde.StatsCollectingBlocksSerde.readBlocks;
import static com.facebook.presto.serde.StatsCollectingBlocksSerde.readStats;
import static com.facebook.presto.serde.StatsCollectingBlocksSerde.writeBlocks;
import static org.testng.Assert.assertEquals;

public class TestStatsCollectingBlocksSerde
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
        Slice slice = sliceOutput.slice();
        BlockIterable actualBlocks = readBlocks(slice, 0);
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

        Stats stats = readStats(slice);
        assertEquals(stats.getAvgRunLength(), 1);
        assertEquals(stats.getMinPosition(), 0);
        assertEquals(stats.getMaxPosition(), 3);
        assertEquals(stats.getRowCount(), 12);
        assertEquals(stats.getRunsCount(), 12);
        assertEquals(stats.getUniqueCount(), 4);
    }
}
