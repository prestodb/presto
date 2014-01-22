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
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.serde.BlockEncoding;
import com.facebook.presto.serde.UncompressedBlockEncoding;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class UncompressedLongBlock
        implements RandomAccessBlock
{
    private static final int ENTRY_SIZE = SIZE_OF_LONG + SIZE_OF_BYTE;
    private final Slice slice;

    public UncompressedLongBlock(Slice slice)
    {
        this.slice = checkNotNull(slice, "slice is null");
        checkArgument(slice.length() % ENTRY_SIZE == 0);
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public int getPositionCount()
    {
        return slice.length() / ENTRY_SIZE;
    }

    @Override
    public DataSize getDataSize()
    {
        return new DataSize(slice.length(), Unit.BYTE);
    }

    @Override
    public BlockCursor cursor()
    {
        return new UncompressedLongBlockCursor(getPositionCount(), slice);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new UncompressedBlockEncoding(TupleInfo.SINGLE_LONG);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        Preconditions.checkPositionIndexes(positionOffset, positionOffset + length, getPositionCount());
        return cursor().getRegionAndAdvance(length);
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        return this;
    }

    @Override
    public boolean getBoolean(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int position)
    {
        int entryOffset = position * ENTRY_SIZE;
        Preconditions.checkState(position >= 0 && entryOffset + ENTRY_SIZE <= slice.length(), "position is not valid");
        return slice.getLong(entryOffset + SIZE_OF_BYTE);
    }

    @Override
    public double getDouble(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int position)
    {
        int entryOffset = position * ENTRY_SIZE;
        Preconditions.checkState(position >= 0 && entryOffset + ENTRY_SIZE <= slice.length(), "position is not valid");
        return slice.getByte(entryOffset) != 0;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", getPositionCount())
                .add("slice", slice)
                .toString();
    }
}
