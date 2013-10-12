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
import com.facebook.presto.tuple.FixedWidthTypeInfo;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public abstract class AbstractFixedWidthBlock
        implements RandomAccessBlock
{
    protected final FixedWidthTypeInfo typeInfo;
    protected final int entrySize;

    protected AbstractFixedWidthBlock(FixedWidthTypeInfo typeInfo)
    {
        this.typeInfo = checkNotNull(typeInfo, "typeInfo is null");
        this.entrySize = typeInfo.getSize() + SIZE_OF_BYTE;
    }

    public FixedWidthTypeInfo getTypeInfo()
    {
        return typeInfo;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(typeInfo.getType());
    }

    @Override
    public DataSize getDataSize()
    {
        return new DataSize(getRawSlice().length(), Unit.BYTE);
    }

    @Override
    public BlockCursor cursor()
    {
        return new FixedWidthBlockCursor(typeInfo, getPositionCount(), getRawSlice());
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new UncompressedBlockEncoding(new TupleInfo(typeInfo.getType()));
    }

    @Override
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        checkPositionIndexes(positionOffset, positionOffset + length, getPositionCount());
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
        return typeInfo.getBoolean(getRawSlice(), (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return typeInfo.getLong(getRawSlice(), (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public double getDouble(int position)
    {
        checkReadablePosition(position);
        return typeInfo.getDouble(getRawSlice(), (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public Object getObjectValue(int position)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            return null;
        }
        return typeInfo.getObjectValue(getRawSlice(), (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public Slice getSlice(int position)
    {
        checkReadablePosition(position);
        return typeInfo.getSlice(getRawSlice(), (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public RandomAccessBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        // TODO: add Slices.copyOf() to airlift
        Slice copy = Slices.allocate(entrySize);
        copy.setBytes(0, getRawSlice(), (position * entrySize), entrySize);

        return new FixedWidthBlock(typeInfo, 1, copy);
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
        return typeInfo.equals(getRawSlice(), entryOffset + SIZE_OF_BYTE, cursor);
    }

    @Override
    public boolean equals(int position, Slice rightSlice, int rightOffset)
    {
        checkReadablePosition(position);
        int leftEntryOffset = position * entrySize;
        return typeInfo.equals(getRawSlice(), leftEntryOffset + SIZE_OF_BYTE, rightSlice, rightOffset);
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
            return typeInfo.hashCode(getRawSlice(), entryOffset + SIZE_OF_BYTE);
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
        return typeInfo.compareTo(getRawSlice(), leftEntryOffset + SIZE_OF_BYTE, rightSlice, rightOffset);
    }

    @Override
    public void appendTupleTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int entryOffset = position * entrySize;
        if (getRawSlice().getByte(entryOffset) != 0) {
            blockBuilder.appendNull();
        }
        else {
            typeInfo.appendTo(getRawSlice(), entryOffset + SIZE_OF_BYTE, blockBuilder);
        }
    }

    private void checkReadablePosition(int position)
    {
        checkState(position >= 0 && position < getPositionCount(), "position is not valid");
    }
}
