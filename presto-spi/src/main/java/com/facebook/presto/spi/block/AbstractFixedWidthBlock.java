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

import com.facebook.presto.spi.Session;
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
        this.entrySize = valueOffset(type.getFixedSize());
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
        return type.getBoolean(getRawSlice(), valueOffset(entryOffset(position)));
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return type.getLong(getRawSlice(), valueOffset(entryOffset(position)));
    }

    @Override
    public double getDouble(int position)
    {
        checkReadablePosition(position);
        return type.getDouble(getRawSlice(), valueOffset(entryOffset(position)));
    }

    @Override
    public Object getObjectValue(Session session, int position)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            return null;
        }
        return type.getObjectValue(session, getRawSlice(), valueOffset(entryOffset(position)));
    }

    @Override
    public Slice getSlice(int position)
    {
        checkReadablePosition(position);
        return type.getSlice(getRawSlice(), valueOffset(entryOffset(position)));
    }

    @Override
    public RandomAccessBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        // TODO: add Slices.copyOf() to airlift
        Slice copy = Slices.allocate(entrySize);
        copy.setBytes(0, getRawSlice(), entryOffset(position), entrySize);

        return new FixedWidthBlock(type, 1, copy);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return isEntryAtOffsetNull(entryOffset(position));
    }

    @Override
    public boolean equalTo(int position, RandomAccessBlock otherBlock, int otherPosition)
    {
        checkReadablePosition(position);
        int leftEntryOffset = entryOffset(position);
        boolean leftIsNull = isEntryAtOffsetNull(leftEntryOffset);

        boolean rightIsNull = otherBlock.isNull(otherPosition);

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        return otherBlock.equalTo(otherPosition, getRawSlice(), valueOffset(leftEntryOffset));
    }

    @Override
    public boolean equalTo(int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        int entryOffset = entryOffset(position);
        boolean thisIsNull = isEntryAtOffsetNull(entryOffset);
        boolean valueIsNull = cursor.isNull();

        if (thisIsNull != valueIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (thisIsNull) {
            return true;
        }
        return type.equalTo(getRawSlice(), valueOffset(entryOffset), cursor);
    }

    @Override
    public boolean equalTo(int position, Slice otherSlice, int otherOffset)
    {
        checkReadablePosition(position);
        int leftEntryOffset = entryOffset(position);
        return type.equalTo(getRawSlice(), valueOffset(leftEntryOffset), otherSlice, otherOffset);
    }

    @Override
    public int hash(int position)
    {
        checkReadablePosition(position);
        int entryOffset = entryOffset(position);
        if (isEntryAtOffsetNull(entryOffset)) {
            return 0;
        }
        return type.hash(getRawSlice(), valueOffset(entryOffset));
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock otherBlock, int otherPosition)
    {
        checkReadablePosition(position);
        int leftEntryOffset = entryOffset(position);
        boolean leftIsNull = isEntryAtOffsetNull(leftEntryOffset);

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
        int result = -otherBlock.compareTo(otherPosition, getRawSlice(), valueOffset(leftEntryOffset));
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        int leftEntryOffset = entryOffset(position);
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
        int leftEntryOffset = entryOffset(position);
        return type.compareTo(getRawSlice(), valueOffset(leftEntryOffset), otherSlice, otherOffset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int entryOffset = entryOffset(position);
        if (isEntryAtOffsetNull(entryOffset)) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(getRawSlice(), valueOffset(entryOffset), blockBuilder);
        }
    }

    private int entryOffset(int position)
    {
        return position * entrySize;
    }

    private int valueOffset(int entryOffset)
    {
        return entryOffset + SIZE_OF_BYTE;
    }

    private boolean isEntryAtOffsetNull(int entryOffset)
    {
        return getRawSlice().getByte(entryOffset) != 0;
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalStateException("position is not valid");
        }
    }
}
