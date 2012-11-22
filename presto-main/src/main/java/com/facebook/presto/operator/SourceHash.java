/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;

public class SourceHash
{
    private final int hashChannel;
    private final PagesIndex pagesIndex;
    private final BlocksHash blocksHash;

    public SourceHash(Operator source, int hashChannel, int expectedPositions)
    {
        this.hashChannel = hashChannel;
        this.pagesIndex = new PagesIndex(source, expectedPositions);
        this.blocksHash = new BlocksHash(pagesIndex.getIndex(hashChannel));
    }

    public SourceHash(SourceHash sourceHash)
    {
        this.hashChannel = sourceHash.hashChannel;
        this.pagesIndex = sourceHash.pagesIndex;
        // hash strategy can not be shared across threads
        this.blocksHash = new BlocksHash(sourceHash.blocksHash);
    }

    public int getChannelCount()
    {
        return pagesIndex.getChannelCount();
    }

    public int getHashChannel()
    {
        return hashChannel;
    }

    public void setProbeSlice(Slice slice)
    {
        blocksHash.setProbeSlice(slice);
    }

    public int getJoinPosition(BlockCursor cursor)
    {
        return blocksHash.get(cursor);
    }

    public int getNextJoinPosition(int joinPosition)
    {
        return blocksHash.getNextPosition(joinPosition);
    }

    public void appendTupleTo(int channel, int position, BlockBuilder blockBuilder)
    {
        pagesIndex.appendTupleTo(channel, position, blockBuilder);
    }
}
