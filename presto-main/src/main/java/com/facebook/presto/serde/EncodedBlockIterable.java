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
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.Iterator;

public class EncodedBlockIterable
        implements BlockIterable
{
    private final BlockEncoding blockEncoding;
    private final Slice blocksSlice;
    private final int positionCount;

    public EncodedBlockIterable(BlockEncoding blockEncoding, Slice blocksSlice, int positionCount)
    {
        this.positionCount = positionCount;
        Preconditions.checkNotNull(blockEncoding, "blockEncoding is null");
        Preconditions.checkNotNull(blocksSlice, "blocksSlice is null");

        this.blockEncoding = blockEncoding;
        this.blocksSlice = blocksSlice;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return blockEncoding.getTupleInfo();
    }

    @Override
    public Optional<DataSize> getDataSize()
    {
        return Optional.of(new DataSize(blocksSlice.length(), Unit.BYTE));
    }

    @Override
    public Optional<Integer> getPositionCount()
    {
        return Optional.of(positionCount);
    }

    @Override
    public Iterator<Block> iterator()
    {
        return new EncodedBlockIterator(blockEncoding, blocksSlice.getInput());
    }

    private static class EncodedBlockIterator
            extends AbstractIterator<Block>
    {
        private final BlockEncoding blockEncoding;
        private final SliceInput sliceInput;

        private EncodedBlockIterator(BlockEncoding blockEncoding, SliceInput sliceInput)
        {
            this.blockEncoding = blockEncoding;
            this.sliceInput = sliceInput;
        }

        protected Block computeNext()
        {
            if (!sliceInput.isReadable()) {
                return endOfData();
            }

            Block block = blockEncoding.readBlock(sliceInput);
            return block;
        }
    }
}
