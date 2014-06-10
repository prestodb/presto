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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VariableWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

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

    protected abstract int[] getOffsets();

    @Override
    public Type getType()
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
        return new VariableWidthRandomAccessBlockCursor(type, getPositionCount(), getRawSlice(), getOffsets());
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new VariableWidthBlockEncoding(type);
    }

    @Override
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }
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
    public Object getObjectValue(ConnectorSession session, int position)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            return null;
        }
        int offset = getPositionOffset(position);
        return type.getObjectValue(session, getRawSlice(), valueOffset(offset));
    }

    @Override
    public Slice getSlice(int position)
    {
        if (isNull(position)) {
            throw new IllegalStateException("position is null");
        }
        int offset = getPositionOffset(position);
        return type.getSlice(getRawSlice(), valueOffset(offset));
    }

    @Override
    public RandomAccessBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        int offset = getPositionOffset(position);
        if (isEntryAtOffsetNull(offset)) {
            return new VariableWidthRandomAccessBlock(type, 1, Slices.wrappedBuffer(new byte[] {1}), new int[] {0});
        }

        int entrySize = valueOffset(type.getLength(getRawSlice(), valueOffset(offset)));

        Slice copy = Slices.copyOf(getRawSlice(), offset, entrySize);

        return new VariableWidthRandomAccessBlock(type, 1, copy, new int[] {0});
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        return isEntryAtOffsetNull(offset);
    }

    @Override
    public boolean equalTo(int position, RandomAccessBlock otherBlock, int otherPosition)
    {
        checkReadablePosition(position);
        int leftOffset = getPositionOffset(position);

        boolean leftIsNull = isEntryAtOffsetNull(leftOffset);
        boolean rightIsNull = otherBlock.isNull(otherPosition);

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        return otherBlock.equalTo(otherPosition, getRawSlice(), valueOffset(leftOffset));
    }

    @Override
    public boolean equalTo(int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        boolean thisIsNull = isEntryAtOffsetNull(offset);
        boolean valueIsNull = cursor.isNull();

        if (thisIsNull != valueIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (thisIsNull) {
            return true;
        }

        return type.equalTo(getRawSlice(), valueOffset(offset), cursor);
    }

    @Override
    public boolean equalTo(int position, Slice otherSlice, int otherOffset)
    {
        checkReadablePosition(position);
        int leftEntryOffset = getPositionOffset(position);
        return type.equalTo(getRawSlice(), valueOffset(leftEntryOffset), otherSlice, otherOffset);
    }

    @Override
    public int hash(int position)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        if (isEntryAtOffsetNull(offset)) {
            return 0;
        }
        return type.hash(getRawSlice(), valueOffset(offset));
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock otherBlock, int otherPosition)
    {
        checkReadablePosition(position);
        int leftOffset = getPositionOffset(position);

        boolean leftIsNull = isEntryAtOffsetNull(leftOffset);
        boolean rightIsNull = otherBlock.isNull(otherPosition);

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
        int result = -otherBlock.compareTo(otherPosition, getRawSlice(), valueOffset(leftOffset));
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        int leftEntryOffset = getPositionOffset(position);
        boolean leftIsNull = isEntryAtOffsetNull(leftEntryOffset);

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
        int result = -cursor.compareTo(getRawSlice(), valueOffset(leftEntryOffset));
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public int compareTo(int position, Slice otherSlice, int otherOffset)
    {
        checkReadablePosition(position);
        int leftEntryOffset = getPositionOffset(position);
        return type.compareTo(getRawSlice(), valueOffset(leftEntryOffset), otherSlice, otherOffset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int offset = getPositionOffset(position);
        if (isEntryAtOffsetNull(offset)) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(getRawSlice(), valueOffset(offset), blockBuilder);
        }
    }

    private boolean isEntryAtOffsetNull(int offset)
    {
        return getRawSlice().getByte(offset) != 0;
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalStateException("position is not valid");
        }
    }

    private static int valueOffset(int entryOffset)
    {
        return entryOffset + SIZE_OF_BYTE;
    }
}
