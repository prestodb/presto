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
package com.facebook.presto.type;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.FixedWidthBlockBuilder;
import com.facebook.presto.spi.ColumnType;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;

import static com.facebook.presto.spi.ColumnType.LONG;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public final class BigintType
        implements FixedWidthType
{
    public static final BigintType BIGINT = new BigintType();

    private BigintType()
    {
    }

    @Override

    public String getName()
    {
        return "bigint";
    }

    @Override
    public int getFixedSize()
    {
        return (int) SIZE_OF_LONG;
    }

    @Override
    public ColumnType toColumnType()
    {
        return LONG;
    }

    @Override
    public Object getObjectValue(Slice slice, int offset)
    {
        return slice.getLong(offset);
    }

    @Override
    public BlockBuilder createBlockBuilder(DataSize maxBlockSize)
    {
        return new FixedWidthBlockBuilder(this, maxBlockSize);
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new FixedWidthBlockBuilder(this, positionCount);
    }

    @Override
    public boolean getBoolean(Slice slice, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBoolean(SliceOutput sliceOutput, boolean value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Slice slice, int offset)
    {
        return slice.getLong(offset);
    }

    @Override
    public void setLong(SliceOutput sliceOutput, long value)
    {
        sliceOutput.writeLong(value);
    }

    @Override
    public double getDouble(Slice slice, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDouble(SliceOutput sliceOutput, double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(Slice slice, int offset)
    {
        return slice.slice(offset, getFixedSize());
    }

    @Override
    public void setSlice(SliceOutput sliceOutput, Slice value, int offset, int length)
    {
        checkArgument(length == (int) SIZE_OF_LONG);
        sliceOutput.writeBytes(value, offset, length);
    }

    public boolean equals(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        long leftValue = leftSlice.getLong(leftOffset);
        long rightValue = rightSlice.getLong(rightOffset);
        return leftValue == rightValue;
    }

    public boolean equals(Slice leftSlice, int leftOffset, BlockCursor rightCursor)
    {
        long leftValue = leftSlice.getLong(leftOffset);
        long rightValue = rightCursor.getLong();
        return leftValue == rightValue;
    }

    public int hashCode(Slice slice, int offset)
    {
        return Longs.hashCode(slice.getLong(offset));
    }

    public int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        long leftValue = leftSlice.getLong(leftOffset);
        long rightValue = rightSlice.getLong(rightOffset);
        return Long.compare(leftValue, rightValue);
    }

    public void appendTo(Slice slice, int offset, BlockBuilder blockBuilder)
    {
        long value = slice.getLong(offset);
        blockBuilder.append(value);
    }

    @Override
    public void appendTo(Slice slice, int offset, SliceOutput sliceOutput)
    {
        sliceOutput.writeBytes(slice, offset, (int) SIZE_OF_LONG);
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
