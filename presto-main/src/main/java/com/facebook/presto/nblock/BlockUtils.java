/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.nblock;

import com.facebook.presto.TupleInfo;

import java.util.Arrays;
import java.util.Iterator;

public class BlockUtils
{
    public static Blocks toBlocks(TupleInfo info, Block... blocks)
    {
        return new BlocksAdapter(info, Arrays.asList(blocks));
    }

    public static Blocks toBlocks(TupleInfo info, Iterable<Block> blocks)
    {
        return new BlocksAdapter(info, blocks);
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
        public TupleInfo getTupleInfo()
        {
            return info;
        }

        @Override
        public Iterator<Block> iterator()
        {
            return blocks.iterator();
        }
    }
}
