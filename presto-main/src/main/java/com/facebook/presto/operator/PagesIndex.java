/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public class PagesIndex
{
    private final BlocksIndex[] indexes;
    private final int channelCount;

    public PagesIndex(int channelCount, int expectedPositions, List<TupleInfo> tupleInfos)
    {
        this.channelCount = channelCount;
        indexes = new BlocksIndex[channelCount];
        for (int channel = 0; channel < indexes.length; channel++) {
            indexes[channel] = new BlocksIndex(channel, expectedPositions, tupleInfos.get(channel));
        }
    }

    public int getChannelCount()
    {
        return channelCount;
    }

    public TupleInfo getTupleInfo(int channel)
    {
        return indexes[channel].getTupleInfo();
    }

    public void indexPage(Page page)
    {
        Block[] blocks = page.getBlocks();
        for (int channel = 0; channel < indexes.length; channel++) {
            indexes[channel].indexBlock((UncompressedBlock) blocks[channel]);
        }
    }

    public BlocksIndex getIndex(int channel)
    {
        return indexes[channel];
    }

    public void appendTupleTo(int channel, int position, BlockBuilder output)
    {
        indexes[channel].appendTo(position, output);
    }
}
