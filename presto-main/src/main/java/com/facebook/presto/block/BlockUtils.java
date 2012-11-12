/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;

public class BlockUtils
{
    public static BlockIterable toBlocks(Block firstBlock, Block... rest)
    {
        return new BlocksIterableAdapter(firstBlock.getTupleInfo(), ImmutableList.<Block>builder().add(firstBlock).add(rest).build());
    }

    public static BlockIterable toBlocks(Iterable<Block> blocks)
    {
        return new BlocksIterableAdapter(Iterables.get(blocks, 0).getTupleInfo(), blocks);
    }

    private static class BlocksIterableAdapter implements BlockIterable
    {
        private final TupleInfo tupleInfo;
        private final Iterable<Block> blocks;

        public BlocksIterableAdapter(TupleInfo tupleInfo, Iterable<Block> blocks)
        {
            this.tupleInfo = tupleInfo;
            this.blocks = blocks;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        @Override
        public Iterator<Block> iterator()
        {
            return blocks.iterator();
        }
    }

    public static Iterable<Tuple> toTupleIterable(Block block)
    {
        Preconditions.checkNotNull(block, "block is null");
        return new BlockIterableAdapter(block);
    }

    private static class BlockIterableAdapter implements Iterable<Tuple>
    {
        private final Block block;

        private BlockIterableAdapter(Block block)
        {
            this.block = block;
        }

        @Override
        public Iterator<Tuple> iterator()
        {
            return new BlockCursorIteratorAdapter(block.cursor());
        }
    }

    public static Iterator<Tuple> toTupleIterable(BlockCursor cursor)
    {
        return new BlockCursorIteratorAdapter(cursor);
    }

    private static class BlockCursorIteratorAdapter extends AbstractIterator<Tuple>
    {
        private final BlockCursor cursor;

        private BlockCursorIteratorAdapter(BlockCursor cursor)
        {
            this.cursor = cursor;
        }

        @Override
        protected Tuple computeNext()
        {
            if (!cursor.advanceNextPosition()) {
                return endOfData();
            }
            return cursor.getTuple();
        }
    }
}
