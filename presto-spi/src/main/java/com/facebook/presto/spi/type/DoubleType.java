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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding.BlockEncodingFactory;
import com.facebook.presto.spi.block.FixedWidthBlockUtil.FixedWidthBlockBuilderFactory;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.block.FixedWidthBlockUtil.createIsolatedFixedWidthBlockBuilderFactory;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public final class DoubleType
        implements FixedWidthType
{
    public static final DoubleType DOUBLE = new DoubleType();

    private static final FixedWidthBlockBuilderFactory BLOCK_BUILDER_FACTORY = createIsolatedFixedWidthBlockBuilderFactory(DOUBLE);
    public static final BlockEncodingFactory<?> BLOCK_ENCODING_FACTORY = BLOCK_BUILDER_FACTORY.getBlockEncodingFactory();

    private DoubleType()
    {
    }

    @Override
    public String getName()
    {
        return "double";
    }

    @Override
    public Class<?> getJavaType()
    {
        return double.class;
    }

    @Override
    public int getFixedSize()
    {
        return (int) SIZE_OF_DOUBLE;
    }

    @Override
    public Object getObjectValue(Slice slice, int offset)
    {
        return slice.getDouble(offset);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        return BLOCK_BUILDER_FACTORY.createFixedWidthBlockBuilder(blockBuilderStatus);
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return BLOCK_BUILDER_FACTORY.createFixedWidthBlockBuilder(positionCount);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLong(SliceOutput sliceOutput, long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Slice slice, int offset)
    {
        return slice.getDouble(offset);
    }

    @Override
    public void setDouble(SliceOutput sliceOutput, double value)
    {
        sliceOutput.writeDouble(value);
    }

    @Override
    public Slice getSlice(Slice slice, int offset)
    {
        return slice.slice(offset, getFixedSize());
    }

    @Override
    public void setSlice(SliceOutput sliceOutput, Slice value, int offset)
    {
        sliceOutput.writeBytes(value, offset, SIZE_OF_DOUBLE);
    }

    @Override
    public boolean equals(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        long leftValue = leftSlice.getLong(leftOffset);
        long rightValue = rightSlice.getLong(rightOffset);
        return leftValue == rightValue;
    }

    @Override
    public boolean equals(Slice leftSlice, int leftOffset, BlockCursor rightCursor)
    {
        long leftValue = leftSlice.getLong(leftOffset);
        long rightValue = Double.doubleToLongBits(rightCursor.getDouble());
        return leftValue == rightValue;
    }

    @Override
    public int hashCode(Slice slice, int offset)
    {
        long value = slice.getLong(offset);
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        double leftValue = leftSlice.getDouble(leftOffset);
        double rightValue = rightSlice.getDouble(rightOffset);
        return Double.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Slice slice, int offset, BlockBuilder blockBuilder)
    {
        double value = slice.getDouble(offset);
        blockBuilder.append(value);
    }

    @Override
    public void appendTo(Slice slice, int offset, SliceOutput sliceOutput)
    {
        sliceOutput.writeBytes(slice, offset, (int) SIZE_OF_DOUBLE);
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
