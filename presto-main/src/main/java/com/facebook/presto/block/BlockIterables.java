/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import java.util.Iterator;
import java.util.List;

import static io.airlift.units.DataSize.Unit.BYTE;

public final class BlockIterables
{
    private BlockIterables()
    {
    }

    public static BlockIterable createBlockIterable(Block... blocks)
    {
        return new StaticBlockIterable(ImmutableList.copyOf(blocks));
    }

    public static BlockIterable createBlockIterable(Iterable<? extends Block> blocks)
    {
        return new StaticBlockIterable(ImmutableList.copyOf(blocks));
    }

    private static class StaticBlockIterable
            implements BlockIterable
    {
        private final List<Block> blocks;
        private final int positionCount;
        private final DataSize dataSize;

        public StaticBlockIterable(Iterable<Block> blocks)
        {
            Preconditions.checkNotNull(blocks, "blocks is null");
            this.blocks = ImmutableList.copyOf(blocks);
            Preconditions.checkArgument(!this.blocks.isEmpty(), "blocks is empty");

            long positionCount = 0;
            long dataSize = 0;
            for (Block block : this.blocks) {
                positionCount += block.getPositionCount();
                dataSize += block.getDataSize().toBytes();
            }
            this.positionCount = Ints.checkedCast(positionCount);
            this.dataSize = new DataSize(dataSize, BYTE);
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return blocks.get(0).getTupleInfo();
        }

        @Override
        public Iterator<Block> iterator()
        {
            return blocks.iterator();
        }

        @Override
        public Optional<DataSize> getDataSize()
        {
            return Optional.of(dataSize);
        }

        @Override
        public Optional<Integer> getPositionCount()
        {
            return Optional.of(positionCount);
        }
    }

    public static Optional<DataSize> getDataSize(BlockIterable... blockIterables)
    {
        return getDataSize(ImmutableList.copyOf(blockIterables));
    }

    public static Optional<DataSize> getDataSize(Iterable<? extends BlockIterable> blockIterables)
    {
        long dataSize = 0;
        for (BlockIterable blocks : blockIterables) {
            if (!blocks.getDataSize().isPresent()) {
                return Optional.absent();
            }
            dataSize += blocks.getDataSize().get().toBytes();
        }
        return Optional.of(new DataSize(dataSize, BYTE));
    }

    public static Optional<Integer> getPositionCount(BlockIterable... blockIterables)
    {
        return getPositionCount(ImmutableList.copyOf(blockIterables));
    }

    public static Optional<Integer> getPositionCount(Iterable<? extends BlockIterable> blockIterables)
    {
        for (BlockIterable blocks : blockIterables) {
            if (!blocks.getDataSize().isPresent()) {
                return Optional.absent();
            }
        }

        return Iterables.getFirst(blockIterables, null).getPositionCount();
    }

    public static BlockIterable concat(BlockIterable... blockIterables)
    {
        return new ConcatBlockIterable(ImmutableList.copyOf(blockIterables));
    }

    public static BlockIterable concat(Iterable<? extends BlockIterable> blockIterables)
    {
        return new ConcatBlockIterable(blockIterables);
    }

    private static class ConcatBlockIterable
            implements BlockIterable
    {
        private final Iterable<? extends BlockIterable> blockIterables;
        private final TupleInfo tupleInfo;
        private final Optional<DataSize> dataSize;
        private final Optional<Integer> positionCount;

        private ConcatBlockIterable(Iterable<? extends BlockIterable> blockIterables)
        {
            this.blockIterables = blockIterables;

            ImmutableList.Builder<Type> types = ImmutableList.builder();
            for (BlockIterable blocks : blockIterables) {
                types.addAll(blocks.getTupleInfo().getTypes());
            }
            tupleInfo = new TupleInfo(types.build());
            this.dataSize = BlockIterables.getDataSize(blockIterables);
            this.positionCount = BlockIterables.getPositionCount(blockIterables);
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        @Override
        public Optional<DataSize> getDataSize()
        {
            return dataSize;
        }

        @Override
        public Optional<Integer> getPositionCount()
        {
            return positionCount;
        }

        @Override
        public Iterator<Block> iterator()
        {
            return new AbstractIterator<Block>()
            {
                private final Iterator<? extends BlockIterable> blockIterables = ConcatBlockIterable.this.blockIterables.iterator();
                private Iterator<Block> blocks;

                @Override
                protected Block computeNext()
                {
                    while ((blocks == null || !blocks.hasNext()) && blockIterables.hasNext()) {
                        blocks = blockIterables.next().iterator();
                    }
                    if (blocks == null || !blocks.hasNext()) {
                        return endOfData();
                    }
                    UncompressedBlock block = (UncompressedBlock) blocks.next();
                    return block;
                }
            };
        }
    }
}
