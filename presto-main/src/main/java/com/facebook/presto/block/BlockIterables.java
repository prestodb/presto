/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public final class BlockIterables {
    private BlockIterables() {}

    public static BlockIterable createBlockIterable(Block... blocks) {
        return new StaticBlockIterable(ImmutableList.copyOf(blocks));
    }

    public static BlockIterable createBlockIterable(Iterable<? extends Block> blocks) {
        return new StaticBlockIterable(ImmutableList.copyOf(blocks));
    }

    private static class StaticBlockIterable implements BlockIterable
    {
        private final List<Block> blocks;

        public StaticBlockIterable(Iterable<Block> blocks)
        {
            this.blocks = ImmutableList.copyOf(blocks);
        }

        @Override
        public Iterator<Block> iterator()
        {
            return blocks.iterator();
        }
    }
}
