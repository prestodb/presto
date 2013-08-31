/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import io.airlift.slice.Slice;

public class NewSourceHash
{
    private final NewChannelHash channelHash;
    private final NewPagesIndex pagesIndex;
    private final int channelCount;

    public NewSourceHash(NewChannelHash channelHash, NewPagesIndex pagesIndex)
    {
        this.channelHash = channelHash;
        this.pagesIndex = pagesIndex;
        this.channelCount = pagesIndex.getTupleInfos().size();
    }

    public int getChannelCount()
    {
        return channelCount;
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
