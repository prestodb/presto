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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.Iterator;

public final class BlockUtils
{
    private BlockUtils() {}

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

    public static BlockIterable toBlocks(Iterable<Block> blocks)
    {
        return new BlocksIterableAdapter(Iterables.get(blocks, 0).getTupleInfo(),
                Optional.<DataSize>absent(),
                Optional.<Integer>absent(),
                blocks);
    }

    private static class BlocksIterableAdapter
            implements BlockIterable
    {
        private final TupleInfo tupleInfo;
        private final Iterable<Block> blocks;
        private Optional<DataSize> dataSize;
        private final Optional<Integer> positionCount;

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
}
