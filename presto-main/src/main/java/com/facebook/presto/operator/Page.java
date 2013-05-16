/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.Arrays;

public class Page
{
    private final Block[] blocks;
    private final int positionCount;

    public Page(Block... blocks)
    {
        this(blocks[0].getPositionCount(), blocks);
    }

    public Page(int positionCount, Block... blocks)
    {
        Preconditions.checkNotNull(blocks, "blocks is null");
        this.blocks = Arrays.copyOf(blocks, blocks.length);
        this.positionCount = positionCount;
    }

    public int getChannelCount()
    {
        return blocks.length;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public DataSize getDataSize()
    {
        long dataSize = 0;
        for (Block block : blocks) {
            dataSize += block.getDataSize().toBytes();
        }
        return new DataSize(dataSize, Unit.BYTE);
    }

    public Block[] getBlocks()
    {
        return blocks.clone();
    }

    public Block getBlock(int channel)
    {
        return blocks[channel];
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("channelCount", getChannelCount())
                .addValue("@" +Integer.toHexString(System.identityHashCode(this)))
                .toString();
    }
}
