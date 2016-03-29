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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

public abstract class AbstractFixedWidthBlock
        implements Block
{
    protected final int fixedSize;

    protected AbstractFixedWidthBlock(int fixedSize)
    {
        if (fixedSize < 0) {
            throw new IllegalArgumentException("fixedSize is negative");
        }
        this.fixedSize = fixedSize;
    }

    protected abstract Slice getRawSlice();

    protected abstract boolean isEntryNull(int position);

    public int getFixedSize()
    {
        return fixedSize;
    }

    @Override
    public int getLength(int position)
    {
        return fixedSize;
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
        if (fixedSize < length) {
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
    public long hash(int position, int offset, int length)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            return 0;
        }

        return XxHash64.hash(getRawSlice(), valueOffset(position) + offset, length);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        if (fixedSize < length) {
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
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeBytes(getRawSlice(), valueOffset(position) + offset, length);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        writeBytesTo(position, 0, getLength(position), blockBuilder);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new FixedWidthBlockEncoding(fixedSize);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        Slice copy = Slices.copyOf(getRawSlice(), valueOffset(position), fixedSize);

        return new FixedWidthBlock(fixedSize, 1, copy, Slices.wrappedBooleanArray(isNull(position)));
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return isEntryNull(position);
    }

    @Override
    public void assureLoaded()
    {
    }

    private int valueOffset(int position)
    {
        return position * fixedSize;
    }

    protected void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
