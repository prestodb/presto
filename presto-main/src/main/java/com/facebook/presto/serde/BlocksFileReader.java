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
import com.google.common.primitives.Ints;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;

import java.util.Iterator;

public class BlocksFileReader
        implements BlockIterable
{
    public static BlocksFileReader readBlocks(Slice slice)
    {
        return new BlocksFileReader(slice);
    }

    private final BlockEncoding blockEncoding;
    private final Slice blocksSlice;
    private final BlockIterable blockIterable;
    private final BlocksFileStats stats;

    public BlocksFileReader(Slice slice)
    {
        Preconditions.checkNotNull(slice, "slice is null");

        // read file footer
        int footerLength = slice.getInt(slice.length() - SizeOf.SIZE_OF_INT);
        int footerOffset = slice.length() - footerLength - SizeOf.SIZE_OF_INT;
        Slice footerSlice = slice.slice(footerOffset, footerLength);
        SliceInput input = footerSlice.getInput();

        // read file encoding
        blockEncoding = BlockEncodings.readBlockEncoding(input);

        // read stats
        stats = BlocksFileStats.deserialize(input);

        blocksSlice = slice.slice(0, footerOffset);
        blockIterable = new EncodedBlockIterable(blockEncoding, blocksSlice, Ints.checkedCast(stats.getRowCount()));
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return blockEncoding.getTupleInfo();
    }

    @Override
    public Optional<DataSize> getDataSize()
    {
        return blockIterable.getDataSize();
    }

    @Override
    public Optional<Integer> getPositionCount()
    {
        return blockIterable.getPositionCount();
    }

    public BlockEncoding getEncoding()
    {
        return blockEncoding;
    }

    public BlocksFileStats getStats()
    {
        return stats;
    }

    @Override
    public Iterator<Block> iterator()
    {
        return blockIterable.iterator();
    }
}
