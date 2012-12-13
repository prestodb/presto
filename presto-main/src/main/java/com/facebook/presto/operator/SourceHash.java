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
    private final ChannelHash channelHash;

    public SourceHash(Operator source, int hashChannel, int expectedPositions, OperatorStats operatorStats)
    {
        this.hashChannel = hashChannel;
        this.pagesIndex = new PagesIndex(source, expectedPositions, operatorStats);
        this.channelHash = new ChannelHash(pagesIndex.getIndex(hashChannel));
    }

    public SourceHash(SourceHash sourceHash)
    {
        this.hashChannel = sourceHash.hashChannel;
        this.pagesIndex = sourceHash.pagesIndex;
        // hash strategy can not be shared across threads
        this.channelHash = new ChannelHash(sourceHash.channelHash);
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
        channelHash.setLookupSlice(slice);
    }

    public int getJoinPosition(BlockCursor cursor)
    {
        return channelHash.get(cursor);
    }

    public int getNextJoinPosition(int joinPosition)
    {
        return channelHash.getNextPosition(joinPosition);
    }

    public void appendTupleTo(int channel, int position, BlockBuilder blockBuilder)
    {
        pagesIndex.appendTupleTo(channel, position, blockBuilder);
    }
}
