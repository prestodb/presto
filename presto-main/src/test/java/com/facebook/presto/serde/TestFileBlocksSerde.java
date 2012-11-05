/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.serde.FileBlocksSerde.FileBlockIterable;
import com.facebook.presto.serde.FileBlocksSerde.FileEncoding;
import com.facebook.presto.serde.FileBlocksSerde.StatsCollector.Stats;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.Slice;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.block.BlockAssertions.toValues;
import static com.facebook.presto.serde.FileBlocksSerde.readBlocks;
import static com.facebook.presto.serde.FileBlocksSerde.writeBlocks;
import static org.testng.Assert.assertEquals;

public class TestFileBlocksSerde
{
    private final List<ImmutableList<String>> expectedValues = ImmutableList.of(
            ImmutableList.of("alice"),
            ImmutableList.of("bob"),
            ImmutableList.of("charlie"),
            ImmutableList.of("dave"),
            ImmutableList.of("alice"),
            ImmutableList.of("bob"),
            ImmutableList.of("charlie"),
            ImmutableList.of("dave"),
            ImmutableList.of("alice"),
            ImmutableList.of("bob"),
            ImmutableList.of("charlie"),
            ImmutableList.of("dave"));

    private final UncompressedBlock expectedBlock = new BlockBuilder(0, SINGLE_VARBINARY)
            .append("alice")
            .append("bob")
            .append("charlie")
            .append("dave")
            .build();

    @Test
    public void testRoundTrip()
    {
        for (FileEncoding encoding : FileEncoding.values()) {
            testRoundTrip(encoding);
        }
    }

    public void testRoundTrip(FileEncoding encoding)
    {

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writeBlocks(encoding, sliceOutput, expectedBlock, expectedBlock, expectedBlock);
        Slice slice = sliceOutput.slice();
        FileBlockIterable actualBlocks = readBlocks(slice, 0);

        List<List<Object>> actualValues = toValues(actualBlocks);

        assertEquals(actualValues, expectedValues);

        Stats stats = actualBlocks.getStats();
        assertEquals(stats.getAvgRunLength(), 1);
        assertEquals(stats.getMinPosition(), 0);
        assertEquals(stats.getMaxPosition(), 3);
        assertEquals(stats.getRowCount(), 12);
        assertEquals(stats.getRunsCount(), 12);
        assertEquals(stats.getUniqueCount(), 4);
    }
}
