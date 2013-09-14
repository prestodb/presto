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

import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.Iterator;

public class BlockUtils
{
    // TODO: remove this hack after empty blocks are supported
    public static BlockIterable emptyBlockIterable()
    {
        return new BlockIterable()
        {
            @Override
            public TupleInfo getTupleInfo()
            {
                return TupleInfo.SINGLE_LONG;
            }

            @Override
            public Optional<DataSize> getDataSize()
            {
                return Optional.of(new DataSize(0, Unit.BYTE));
            }

            @Override
            public Optional<Integer> getPositionCount()
            {
                return Optional.of(0);
            }

            @Override
            public Iterator<Block> iterator()
            {
                return Iterators.emptyIterator();
            }
        };
    }

    public static BlockIterable toBlocks(Block firstBlock, Block... rest)
    {
        return new BlocksIterableAdapter(firstBlock.getTupleInfo(),
                Optional.<DataSize>absent(),
                Optional.<Integer>absent(),
                ImmutableList.<Block>builder().add(firstBlock).add(rest).build());
    }

    public static BlockIterable toBlocks(Iterable<Block> blocks)
    {
        return new BlocksIterableAdapter(Iterables.get(blocks, 0).getTupleInfo(),
                Optional.<DataSize>absent(),
                Optional.<Integer>absent(),
                blocks);
    }

    public static BlockIterable toBlocks(DataSize dataSize, int positionCount, Iterable<Block> blocks)
    {
        return new BlocksIterableAdapter(Iterables.get(blocks, 0).getTupleInfo(),
                Optional.of(dataSize),
                Optional.of(positionCount),
                blocks);
    }

    private static class BlocksIterableAdapter
            implements BlockIterable
    {
        private final TupleInfo tupleInfo;
        private final Iterable<Block> blocks;
        private Optional<DataSize> dataSize;
        private final Optional<Integer> positionCount;

        public BlocksIterableAdapter(TupleInfo tupleInfo, Iterable<Block> blocks)
        {
            this(tupleInfo, Optional.<DataSize>absent(), Optional.<Integer>absent(), blocks);
        }

        public BlocksIterableAdapter(TupleInfo tupleInfo, Optional<DataSize> dataSize, Optional<Integer> positionCount, Iterable<Block> blocks)
        {
            this.tupleInfo = tupleInfo;
            this.blocks = blocks;
            this.dataSize = dataSize;
            this.positionCount = positionCount;
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
            return blocks.iterator();
        }
    }

    public static Iterable<Tuple> toTupleIterable(Block block)
    {
        Preconditions.checkNotNull(block, "block is null");
        return new BlockIterableAdapter(block);
    }

    private static class BlockIterableAdapter
            implements Iterable<Tuple>
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

    private static class BlockCursorIteratorAdapter
            extends AbstractIterator<Tuple>
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
