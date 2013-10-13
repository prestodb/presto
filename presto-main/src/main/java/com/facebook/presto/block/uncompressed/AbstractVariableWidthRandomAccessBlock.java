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
import com.facebook.presto.block.BlockEncoding;
import com.facebook.presto.type.Type;
import com.facebook.presto.type.VariableWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.facebook.presto.type.Types.VARCHAR;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public abstract class AbstractVariableWidthRandomAccessBlock
        implements RandomAccessBlock
{
    protected final VariableWidthType type;

    protected AbstractVariableWidthRandomAccessBlock(VariableWidthType type)
    {
        this.type = type;
    }

    protected abstract Slice getRawSlice();
    protected abstract int getPositionOffset(int position);

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public DataSize getDataSize()
    {
        return new DataSize(getRawSlice().length(), Unit.BYTE);
    }

    @Override
    public BlockCursor cursor()
    {
        return new VariableWidthBlockCursor(type, getPositionCount(), getRawSlice());
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new VariableWidthBlockEncoding(VARCHAR);
    }

    @Override
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        checkPositionIndexes(positionOffset, positionOffset + length, getPositionCount());
        // todo add VariableWidthRandomAccessCursor
        return cursor().getRegionAndAdvance(length).toRandomAccessBlock();
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
    public Object getObjectValue(int position)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            return null;
        }
        int offset = getPositionOffset(position);
        return type.getObjectValue(getRawSlice(), offset + SIZE_OF_BYTE);
    }

    @Override
    public Slice getSlice(int position)
    {
        checkState(!isNull(position));
        int offset = getPositionOffset(position);
        return type.getSlice(getRawSlice(), offset + SIZE_OF_BYTE);
    }

    @Override
    public RandomAccessBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        int offset = getPositionOffset(position);
        if (getRawSlice().getByte(offset) != 0) {
            return new VariableWidthRandomAccessBlock(type, 1, Slices.wrappedBuffer(new byte[] {1}));
        }

        int entrySize = type.getLength(getRawSlice(), offset + SIZE_OF_BYTE) + SIZE_OF_BYTE;

        // TODO: add Slices.copyOf() to airlift
        Slice copy = Slices.allocate(entrySize);
        copy.setBytes(0, getRawSlice(), offset, entrySize);

        return new VariableWidthRandomAccessBlock(type, 1, copy);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        return getRawSlice().getByte(offset) != 0;
    }

    @Override
    public boolean equals(int position, RandomAccessBlock right, int rightPosition)
    {
        checkReadablePosition(position);
        int leftOffset = getPositionOffset(position);

        boolean leftIsNull = getRawSlice().getByte(leftOffset) != 0;
        boolean rightIsNull = right.isNull(rightPosition);

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        return right.equals(rightPosition, getRawSlice(), leftOffset + SIZE_OF_BYTE);
    }

    @Override
    public boolean equals(int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        boolean thisIsNull = getRawSlice().getByte(offset) != 0;
        boolean valueIsNull = cursor.isNull();

        if (thisIsNull != valueIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (thisIsNull) {
            return true;
        }

        return type.equals(getRawSlice(), offset + SIZE_OF_BYTE, cursor);
    }

    @Override
    public boolean equals(int position, Slice rightSlice, int rightOffset)
    {
        checkReadablePosition(position);
        int leftEntryOffset = getPositionOffset(position);
        return type.equals(getRawSlice(), leftEntryOffset + SIZE_OF_BYTE, rightSlice, rightOffset);
    }

    @Override
    public int hashCode(int position)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        if (getRawSlice().getByte(offset) != 0) {
            return 0;
        }
        else {
            return type.hashCode(getRawSlice(), offset + SIZE_OF_BYTE);
        }
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock rightBlock, int rightPosition)
    {
        checkReadablePosition(position);
        int leftOffset = getPositionOffset(position);

        boolean leftIsNull = getRawSlice().getByte(leftOffset) != 0;
        boolean rightIsNull = rightBlock.isNull(rightPosition);

        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }

        // compare the right block to our slice but negate the result since we are evaluating in the opposite order
        int result = -rightBlock.compareTo(rightPosition, getRawSlice(), leftOffset + SIZE_OF_BYTE);
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        int leftEntryOffset = getPositionOffset(position);
        boolean leftIsNull = getRawSlice().getByte(leftEntryOffset) != 0;

        boolean rightIsNull = cursor.isNull();

        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }

        // compare the right cursor to our slice but negate the result since we are evaluating in the opposite order
        int result = -cursor.compareTo(getRawSlice(), leftEntryOffset + SIZE_OF_BYTE);
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public int compareTo(int position, Slice rightSlice, int rightOffset)
    {
        checkReadablePosition(position);
        int leftEntryOffset = getPositionOffset(position);
        return type.compareTo(getRawSlice(), leftEntryOffset + SIZE_OF_BYTE, rightSlice, rightOffset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        if (getRawSlice().getByte(offset) != 0) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(getRawSlice(), offset + SIZE_OF_BYTE, blockBuilder);
        }
    }

    private void checkReadablePosition(int position)
    {
        checkState(position >= 0 && position < getPositionCount(), "position is not valid");
    }
}
