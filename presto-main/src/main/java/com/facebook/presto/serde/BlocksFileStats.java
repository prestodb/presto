/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class BlocksFileStats
{
    private final long rowCount;
    private final long runsCount;
    private final long avgRunLength;
    private final int uniqueCount;

    public BlocksFileStats(long rowCount, long runsCount, long avgRunLength, int uniqueCount)
    {
        this.rowCount = rowCount;
        this.runsCount = runsCount;
        this.avgRunLength = avgRunLength;
        this.uniqueCount = uniqueCount;
    }

    public static void serialize(BlocksFileStats stats, SliceOutput sliceOutput)
    {
        // TODO: add a better way of serializing the stats that is less fragile
        sliceOutput.appendLong(stats.getRowCount())
                .appendLong(stats.getRunsCount())
                .appendLong(stats.getAvgRunLength())
                .appendInt(stats.getUniqueCount());
    }

    public static BlocksFileStats deserialize(Slice slice)
    {
        SliceInput input = slice.getInput();
        return deserialize(input);
    }

    public static BlocksFileStats deserialize(SliceInput input)
    {
        long rowCount = input.readLong();
        long runsCount = input.readLong();
        long avgRunLength = input.readLong();
        int uniqueCount = input.readInt();
        return new BlocksFileStats(rowCount, runsCount, avgRunLength, uniqueCount);
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getRunsCount()
    {
        return runsCount;
    }

    public long getAvgRunLength()
    {
        return avgRunLength;
    }

    public int getUniqueCount()
    {
        return uniqueCount;
    }
}
