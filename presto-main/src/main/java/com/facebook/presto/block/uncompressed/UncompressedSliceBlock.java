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
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.operator.SortOrder;
import com.facebook.presto.serde.BlockEncoding;
import com.facebook.presto.serde.UncompressedBlockEncoding;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
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
        checkPositionIndexes(positionOffset, positionOffset + length, offsets.length);
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
        int length = getLength(slice, offset);
        return slice.slice(offset + SIZE_OF_INT + SIZE_OF_BYTE, length);
    }

    @Override
    public boolean sliceEquals(int leftPosition, Slice rightSlice, int rightOffset, int rightLength)
    {
        checkReadablePosition(leftPosition);

        int leftOffset = offsets[leftPosition];
        int leftLength = getLength(slice, leftOffset);
        return slice.equals(leftOffset + SIZE_OF_INT + SIZE_OF_BYTE, leftLength, rightSlice, rightOffset, rightLength);
    }

    @Override
    public int sliceCompareTo(int leftPosition, Slice rightSlice, int rightOffset, int rightLength)
    {
        checkReadablePosition(leftPosition);

        int leftOffset = offsets[leftPosition];
        int leftLength = getLength(slice, leftOffset);
        return slice.compareTo(leftOffset + SIZE_OF_INT + SIZE_OF_BYTE, leftLength, rightSlice, rightOffset, rightLength);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        int offset = offsets[position];
        return slice.getByte(offset) != 0;
    }

    @Override
    public boolean equals(int position, RandomAccessBlock right, int rightPosition)
    {
        checkReadablePosition(position);
        int offset = offsets[position];
        boolean leftIsNull = slice.getByte(offset) != 0;
        boolean rightIsNull = right.isNull(rightPosition);

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        int length = getLength(slice, offset);
        return right.sliceEquals(rightPosition, slice, offset + SIZE_OF_INT + SIZE_OF_BYTE, length);
    }

    @Override
    public boolean equals(int position, TupleReadable value)
    {
        checkReadablePosition(position);
        int offset = offsets[position];
        boolean thisIsNull = slice.getByte(offset) != 0;
        boolean valueIsNull = value.isNull();

        if (thisIsNull != valueIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (thisIsNull) {
            return true;
        }

        // todo add equals method to TupleReader
        Slice valueSlice = ((BlockCursor) value).getRawSlice();
        int valueOffset = ((BlockCursor) value).getRawOffset();
        int valueLength = getLength(valueSlice, valueOffset);

        int length = getLength(slice, offset);
        return slice.equals(
                offset + SIZE_OF_INT + SIZE_OF_BYTE,
                length,
                valueSlice,
                valueOffset + SIZE_OF_INT + SIZE_OF_BYTE,
                valueLength);
    }

    @Override
    public int hashCode(int position)
    {
        checkReadablePosition(position);
        int offset = offsets[position];
        if (slice.getByte(offset) != 0) {
            return 0;
        }
        else {
            int length = getLength(slice, offset);
            return slice.hashCode(offset + SIZE_OF_INT + SIZE_OF_BYTE, length);
        }
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock right, int rightPosition)
    {
        checkReadablePosition(position);
        int offset = offsets[position];
        boolean leftIsNull = slice.getByte(offset) != 0;
        boolean rightIsNull = right.isNull(rightPosition);

        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }

        int length = getLength(slice, offset);
        int result = -right.sliceCompareTo(rightPosition, slice, offset + SIZE_OF_INT + SIZE_OF_BYTE, length);
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public void appendTupleTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int offset = offsets[position];
        if (slice.getByte(offset) != 0) {
            blockBuilder.appendNull();
        }
        else {
            int length = getLength(slice, offset);
            blockBuilder.append(slice, offset + SIZE_OF_INT + SIZE_OF_BYTE, length);
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", offsets.length)
                .add("slice", slice)
                .toString();
    }

    public static int getLength(Slice slice, int offset)
    {
        return slice.getInt(offset + SIZE_OF_BYTE) - SIZE_OF_INT - SIZE_OF_BYTE;
    }

    private void checkReadablePosition(int position)
    {
        checkState(position >= 0 && position < offsets.length, "position is not valid");
    }
}
