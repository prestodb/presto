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

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.operator.SortOrder;
import com.facebook.presto.serde.BlockEncoding;
import com.facebook.presto.serde.UncompressedBlockEncoding;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.tuple.VariableWidthTypeInfo;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public class VariableWidthRandomAccessBlock
        implements RandomAccessBlock
{
    private final VariableWidthTypeInfo typeInfo;
    private final Slice slice;
    private final int[] offsets;

    public VariableWidthRandomAccessBlock(VariableWidthTypeInfo typeInfo, int positionCount, Slice slice)
    {
        this.typeInfo = typeInfo;
        this.slice = slice;
        this.offsets = new int[positionCount];

        VariableWidthBlockCursor cursor = new VariableWidthBlockCursor(typeInfo, offsets.length, slice);
        for (int position = 0; position < positionCount; position++) {
            checkState(cursor.advanceNextPosition());
            offsets[position] = cursor.getRawOffset();
        }
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(typeInfo.getType());
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
        return new VariableWidthBlockCursor(typeInfo, offsets.length, slice);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new UncompressedBlockEncoding(SINGLE_VARBINARY);
    }

    @Override
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        checkPositionIndexes(positionOffset, positionOffset + length, offsets.length);
        return (RandomAccessBlock) cursor().getRegionAndAdvance(length);
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
        return typeInfo.getSlice(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public Tuple getTuple(int position)
    {
        checkReadablePosition(position);
        int entryOffset = offsets[position];
        int entrySize;
        if (slice.getByte(entryOffset) != 0) {
            entrySize = SIZE_OF_BYTE;
        }
        else {
            entrySize = typeInfo.getLength(slice, entryOffset + SIZE_OF_BYTE) + SIZE_OF_BYTE;
        }

        // TODO: add Slices.copyOf() to airlift
        Slice copy = Slices.allocate(entrySize);
        copy.setBytes(0, slice, entryOffset, entrySize);

        return new Tuple(copy, new TupleInfo(typeInfo.getType()));
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
        int leftOffset = offsets[position];

        VariableWidthRandomAccessBlock rightBlock = (VariableWidthRandomAccessBlock) right;
        rightBlock.checkReadablePosition(rightPosition);
        int rightOffset = rightBlock.offsets[rightPosition];

        boolean leftIsNull = slice.getByte(leftOffset) != 0;
        boolean rightIsNull = rightBlock.slice.getByte(rightPosition) != 0;

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        return typeInfo.equals(slice, leftOffset + SIZE_OF_BYTE, rightBlock.slice, rightOffset + SIZE_OF_BYTE);
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

        return typeInfo.equals(slice, offset + SIZE_OF_BYTE, value);
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
            return typeInfo.hashCode(slice, offset + SIZE_OF_BYTE);
        }
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock right, int rightPosition)
    {
        checkReadablePosition(position);
        int leftOffset = offsets[position];

        VariableWidthRandomAccessBlock rightBlock = (VariableWidthRandomAccessBlock) right;
        rightBlock.checkReadablePosition(rightPosition);
        int rightOffset = rightBlock.offsets[rightPosition];

        boolean leftIsNull = slice.getByte(leftOffset) != 0;
        boolean rightIsNull = rightBlock.slice.getByte(rightOffset) != 0;

        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }

        int result = typeInfo.compareTo(slice, leftOffset + SIZE_OF_BYTE, rightBlock.slice, rightOffset + SIZE_OF_BYTE);
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
            typeInfo.appendTo(slice, offset + SIZE_OF_BYTE, blockBuilder);
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

    private void checkReadablePosition(int position)
    {
        checkState(position >= 0 && position < offsets.length, "position is not valid");
    }

    @Override
    public Slice getRawSlice()
    {
        return slice;
    }
}
