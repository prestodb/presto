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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.FixedWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static java.util.Objects.requireNonNull;

public abstract class AbstractFixedWidthBlock
        implements RandomAccessBlock
{
    protected final FixedWidthType type;
    protected final int entrySize;

    protected AbstractFixedWidthBlock(FixedWidthType type)
    {
        this.type = requireNonNull(type, "type is null");
        this.entrySize = type.getFixedSize() + SIZE_OF_BYTE;
    }

    protected abstract Slice getRawSlice();

    @Override
    public FixedWidthType getType()
    {
        return type;
    }

    @Override
    public int getSizeInBytes()
    {
        return getRawSlice().length();
    }

    @Override
    public BlockCursor cursor()
    {
        return new FixedWidthBlockCursor(type, getPositionCount(), getRawSlice());
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new FixedWidthBlockEncoding(type);
    }

    @Override
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }
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
        checkReadablePosition(position);
        return type.getBoolean(getRawSlice(), (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return type.getLong(getRawSlice(), (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public double getDouble(int position)
    {
        checkReadablePosition(position);
        return type.getDouble(getRawSlice(), (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public Object getObjectValue(int position)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            return null;
        }
        return type.getObjectValue(getRawSlice(), (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public Slice getSlice(int position)
    {
        checkReadablePosition(position);
        return type.getSlice(getRawSlice(), (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public RandomAccessBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        // TODO: add Slices.copyOf() to airlift
        Slice copy = Slices.allocate(entrySize);
        copy.setBytes(0, getRawSlice(), (position * entrySize), entrySize);

        return new FixedWidthBlock(type, 1, copy);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return getRawSlice().getByte((position * entrySize)) != 0;
    }

    @Override
    public boolean equals(int position, RandomAccessBlock rightBlock, int rightPosition)
    {
        checkReadablePosition(position);
        int leftEntryOffset = position * entrySize;
        boolean leftIsNull = getRawSlice().getByte(leftEntryOffset) != 0;

        boolean rightIsNull = rightBlock.isNull(rightPosition);

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        return rightBlock.equals(rightPosition, getRawSlice(), leftEntryOffset + SIZE_OF_BYTE);
    }

    @Override
    public boolean equals(int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        int entryOffset = position * entrySize;
        boolean thisIsNull = getRawSlice().getByte(entryOffset) != 0;
        boolean valueIsNull = cursor.isNull();

        if (thisIsNull != valueIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (thisIsNull) {
            return true;
        }
        return type.equals(getRawSlice(), entryOffset + SIZE_OF_BYTE, cursor);
    }

    @Override
    public boolean equals(int position, Slice rightSlice, int rightOffset)
    {
        checkReadablePosition(position);
        int leftEntryOffset = position * entrySize;
        return type.equals(getRawSlice(), leftEntryOffset + SIZE_OF_BYTE, rightSlice, rightOffset);
    }

    @Override
    public int hashCode(int position)
    {
        checkReadablePosition(position);
        int entryOffset = position * entrySize;
        if (getRawSlice().getByte(entryOffset) != 0) {
            return 0;
        }
        else {
            return type.hashCode(getRawSlice(), entryOffset + SIZE_OF_BYTE);
        }
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock rightBlock, int rightPosition)
    {
        checkReadablePosition(position);
        int leftEntryOffset = position * entrySize;
        boolean leftIsNull = getRawSlice().getByte(leftEntryOffset) != 0;

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
        int result = -rightBlock.compareTo(rightPosition, getRawSlice(), leftEntryOffset + SIZE_OF_BYTE);
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        int leftEntryOffset = position * entrySize;
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
        int leftEntryOffset = position * entrySize;
        return type.compareTo(getRawSlice(), leftEntryOffset + SIZE_OF_BYTE, rightSlice, rightOffset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int entryOffset = position * entrySize;
        if (getRawSlice().getByte(entryOffset) != 0) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(getRawSlice(), entryOffset + SIZE_OF_BYTE, blockBuilder);
        }
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalStateException("position is not valid");
        }
    }
}
