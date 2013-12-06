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

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class UncompressedSliceBlock
        implements RandomAccessBlock
{
    private final Slice slice;
    private final int[] offsets;

    public UncompressedSliceBlock(UncompressedBlock block)
    {
        checkNotNull(block, "block is null");
        checkArgument(block.getTupleInfo().equals(SINGLE_VARBINARY));

        offsets = new int[block.getPositionCount()];
        slice = block.getSlice();

        BlockCursor cursor = block.cursor();
        for (int position = 0; position < block.getPositionCount(); position++) {
            checkState(cursor.advanceNextPosition());
            int offset = cursor.getRawOffset();

            offsets[position] = offset;
        }
    }

    public UncompressedSliceBlock(Slice slice, int[] offsets)
    {
        checkNotNull(slice, "slice is null");
        checkNotNull(offsets, "offsets is null");
        this.slice = slice;
        this.offsets = offsets;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return SINGLE_VARBINARY;
    }

    @Override
    public int getPositionCount()
    {
        return offsets.length;
    }

    @Override
    public DataSize getDataSize()
    {
        return new DataSize(slice.length(), Unit.BYTE);
    }

    @Override
    public BlockCursor cursor()
    {
        return new UncompressedSliceBlockCursor(offsets.length, slice);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new UncompressedBlockEncoding(SINGLE_VARBINARY);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        Preconditions.checkPositionIndexes(positionOffset, positionOffset + length, offsets.length);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int position)
    {
        checkReadablePosition(position);

        int offset = offsets[position];
        int size = slice.getInt(offset + SIZE_OF_BYTE);
        return slice.slice(offset + SIZE_OF_INT + SIZE_OF_BYTE, size - SIZE_OF_INT - SIZE_OF_BYTE);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        int offset = offsets[position];
        return slice.getByte(offset) != 0;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", offsets.length)
                .add("slice", slice)
                .toString();
    }

    private void checkReadablePosition(int position)
    {
        Preconditions.checkState(position > 0 && position < offsets.length, "position is not valid");
    }
}
