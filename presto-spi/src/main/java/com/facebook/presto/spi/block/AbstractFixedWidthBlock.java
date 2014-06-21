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
import com.facebook.presto.spi.type.FixedWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static java.util.Objects.requireNonNull;

public abstract class AbstractFixedWidthBlock
        implements Block
{
    protected final FixedWidthType type;
    protected final int entrySize;

    protected AbstractFixedWidthBlock(FixedWidthType type)
    {
        this.type = requireNonNull(type, "type is null");
        this.entrySize = type.getFixedSize();
    }

    protected abstract Slice getRawSlice();

    protected abstract boolean isEntryNull(int position);

    @Override
    public int getLength(int position)
    {
        return entrySize;
    }

    @Override
    public byte getByte(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getByte(valueOffset(position) + offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getShort(valueOffset(position) + offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getInt(valueOffset(position) + offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getLong(valueOffset(position) + offset);
    }

    @Override
    public float getFloat(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getFloat(valueOffset(position) + offset);
    }

    @Override
    public double getDouble(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getDouble(valueOffset(position) + offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return getRawSlice().slice(valueOffset(position) + offset, length);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        checkReadablePosition(position);
        if (entrySize < length) {
            return false;
        }
        int thisOffset = valueOffset(position) + offset;
        return otherBlock.bytesEqual(otherPosition, otherOffset, getRawSlice(), thisOffset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        checkReadablePosition(position);
        int thisOffset = valueOffset(position) + offset;
        return getRawSlice().equals(thisOffset, length, otherSlice, otherOffset, length);
    }

    @Override
    public int hash(int position, int offset, int length)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            return 0;
        }
        return getRawSlice().hashCode(valueOffset(position), length);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        if (entrySize < length) {
            throw new IllegalArgumentException("Length longer than value length");
        }
        int thisOffset = valueOffset(position) + offset;
        return -otherBlock.bytesCompare(otherPosition, otherOffset, otherLength, getRawSlice(), thisOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        return getRawSlice().compareTo(valueOffset(position) + offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void appendSliceTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.appendSlice(getRawSlice(), valueOffset(position), length);
        }
    }

    @Override
    public FixedWidthType getType()
    {
        return type;
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new FixedWidthBlockEncoding(type);
    }

    @Override
    public boolean getBoolean(int position)
    {
        checkReadablePosition(position);
        return type.getBoolean(this, position);
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return type.getLong(this, position);
    }

    @Override
    public double getDouble(int position)
    {
        checkReadablePosition(position);
        return type.getDouble(this, position);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, int position)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            return null;
        }
        return type.getObjectValue(session, this, position);
    }

    @Override
    public Slice getSlice(int position)
    {
        checkReadablePosition(position);
        return type.getSlice(this, position);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        Slice copy = Slices.copyOf(getRawSlice(), valueOffset(position), entrySize);

        return new FixedWidthBlock(type, 1, copy, new boolean[] {isNull(position)});
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return isEntryNull(position);
    }

    @Override
    public boolean equalTo(int position, Block otherBlock, int otherPosition)
    {
        boolean leftIsNull = isNull(position);
        boolean rightIsNull = otherBlock.isNull(otherPosition);

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        return type.equalTo(this, position, otherBlock, otherPosition);
    }

    @Override
    public int hash(int position)
    {
        if (isNull(position)) {
            return 0;
        }
        return type.hash(this, position);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, Block otherBlock, int otherPosition)
    {
        boolean leftIsNull = isNull(position);
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

        int result = type.compareTo(this, position, otherBlock, otherPosition);
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        if (isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(this, position, blockBuilder);
        }
    }

    private int valueOffset(int position)
    {
        return position * entrySize;
    }

    protected void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
