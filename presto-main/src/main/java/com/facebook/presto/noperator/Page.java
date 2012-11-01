/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.nblock.Block;
import com.google.common.base.Preconditions;

public class Page
{
    private final Block[] blocks;

    public Page(Block... blocks)
    {
        Preconditions.checkNotNull(blocks, "blocks is null");
        Preconditions.checkArgument(blocks.length > 0, "blocks is empty");
        this.blocks = blocks;
    }

    public int getChannelCount()
    {
        return blocks.length;
    }

    public int getPositionCount()
    {
        return blocks[0].getPositionCount();
    }

    public Block[] getBlocks()
    {
        return blocks.clone();
    }

    public Block getBlock(int channel)
    {
        return blocks[channel];
    }
}
