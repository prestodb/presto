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

public abstract class AbstractVariableWidthBlock
        implements Block
{
    protected final VariableWidthType type;

    protected AbstractVariableWidthBlock(VariableWidthType type)
    {
        this.type = type;
    }

    protected abstract Slice getRawSlice();

    protected abstract int getPositionOffset(int position);

    protected abstract boolean isEntryNull(int position);

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new VariableWidthBlockEncoding(type);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getByte(getPositionOffset(position) + offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getShort(getPositionOffset(position) + offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getInt(getPositionOffset(position) + offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getLong(getPositionOffset(position) + offset);
    }

    @Override
    public float getFloat(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getFloat(getPositionOffset(position) + offset);
    }

    @Override
    public double getDouble(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice().getDouble(getPositionOffset(position) + offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return getRawSlice().slice(getPositionOffset(position) + offset, length);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        checkReadablePosition(position);
        Slice rawSlice = getRawSlice();
        if (getLength(position) < length) {
            return false;
        }
        return otherBlock.bytesEqual(otherPosition, otherOffset, rawSlice, getPositionOffset(position) + offset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        checkReadablePosition(position);
        return getRawSlice().equals(getPositionOffset(position) + offset, length, otherSlice, otherOffset, length);
    }

    @Override
    public int hash(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return getRawSlice().hashCode(getPositionOffset(position) + offset, length);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        Slice rawSlice = getRawSlice();
        if (getLength(position) < length) {
            throw new IllegalArgumentException("Length longer than value length");
        }
        return -otherBlock.bytesCompare(otherPosition, otherOffset, otherLength, rawSlice, getPositionOffset(position) + offset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        return getRawSlice().compareTo(getPositionOffset(position) + offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void appendSliceTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.appendSlice(getRawSlice(), getPositionOffset(position) + offset, length);
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
        if (isNull(position)) {
            return null;
        }
        return type.getObjectValue(session, this, position);
    }

    @Override
    public Slice getSlice(int position)
    {
        if (isNull(position)) {
            throw new IllegalStateException("position is null");
        }
        return type.getSlice(this, position);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        if (isNull(position)) {
            return new VariableWidthBlock(type, 1, Slices.wrappedBuffer(new byte[0]), new int[] {0, 0}, new boolean[] {true});
        }

        int offset = getPositionOffset(position);
        int entrySize = getLength(position);

        Slice copy = Slices.copyOf(getRawSlice(), offset, entrySize);

        return new VariableWidthBlock(type, 1, copy, new int[] {0, copy.length()}, new boolean[] {false});
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
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        if (isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(this, position, blockBuilder);
        }
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
