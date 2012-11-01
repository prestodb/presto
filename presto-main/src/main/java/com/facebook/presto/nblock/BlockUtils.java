/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.nblock;

import com.facebook.presto.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;

public class BlockUtils
{
    public static Blocks toBlocks(Block firstBlock, Block... rest)
    {
        return new BlocksAdapter(firstBlock.getTupleInfo(), ImmutableList.<Block>builder().add(firstBlock).add(rest).build());
    }

    public static Blocks toBlocks(Iterable<Block> blocks)
    {
        return new BlocksAdapter(Iterables.get(blocks, 0).getTupleInfo(), blocks);
    }

    private static class BlocksAdapter implements Blocks
    {
        private final TupleInfo info;
        private final Iterable<Block> blocks;

        public BlocksAdapter(TupleInfo info, Iterable<Block> blocks)
        {
            this.info = info;
            this.blocks = blocks;
        }

        @Override
        public Iterator<Block> iterator()
        {
            return blocks.iterator();
        }
    }
}
