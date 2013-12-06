/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.block;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.BYTE;

public final class BlockIterables
{
    private BlockIterables()
    {
    }

    public static BlockIterable createBlockIterable(Block firstBlock, Block... otherBlocks)
    {
        TupleInfo tupleInfo = firstBlock.getTupleInfo();
        return new StaticBlockIterable(tupleInfo, ImmutableList.<Block>builder().add(firstBlock).add(otherBlocks).build());
    }

    public static BlockIterable createBlockIterable(Iterable<? extends Block> blocks)
    {
        TupleInfo tupleInfo = Iterables.get(blocks, 0).getTupleInfo();
        return new StaticBlockIterable(tupleInfo, ImmutableList.copyOf(blocks));
    }

    public static BlockIterable createBlockIterable(TupleInfo tupleInfo, Iterable<? extends Block> blocks)
    {
        return new StaticBlockIterable(tupleInfo, ImmutableList.copyOf(blocks));
    }

    private static class StaticBlockIterable
            implements BlockIterable
    {
        private final TupleInfo tupleInfo;
        private final List<Block> blocks;
        private final int positionCount;
        private final DataSize dataSize;

        public StaticBlockIterable(TupleInfo tupleInfo, Iterable<Block> blocks)
        {
            this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
            this.blocks = ImmutableList.copyOf(checkNotNull(blocks, "blocks is null"));

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
            return tupleInfo;
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
            this.dataSize = BlockIterables.getDataSize(blockIterables);
            this.positionCount = BlockIterables.getPositionCount(blockIterables);
            tupleInfo = blockIterables.iterator().next().getTupleInfo();
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
                    return blocks.next();
                }
            };
        }
    }
}
