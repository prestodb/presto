/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public class PageBuilder
{
    private final BlockBuilder[] blockBuilders;

    public PageBuilder(List<TupleInfo> tupleInfos)
    {
        blockBuilders = new BlockBuilder[tupleInfos.size()];
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = new BlockBuilder(tupleInfos.get(i));
        }
    }

    public BlockBuilder getBlockBuilder(int channel)
    {
        return blockBuilders[channel];
    }

    public boolean isFull()
    {
        for (BlockBuilder blockBuilder : blockBuilders) {
            if (blockBuilder.isFull()) {
                return true;
            }
        }
        return false;
    }

    public boolean isEmpty()
    {
        return blockBuilders[0].isEmpty();
    }

    public Page build()
    {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        return new Page(blocks);
    }
}
