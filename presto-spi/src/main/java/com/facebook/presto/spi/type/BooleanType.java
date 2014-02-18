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

import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding.BlockEncodingFactory;
import com.facebook.presto.spi.block.FixedWidthBlockUtil.FixedWidthBlockBuilderFactory;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.block.FixedWidthBlockUtil.createIsolatedFixedWidthBlockBuilderFactory;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public final class BooleanType
        implements FixedWidthType
{
    public static final BooleanType BOOLEAN = new BooleanType();

    private static final FixedWidthBlockBuilderFactory BLOCK_BUILDER_FACTORY = createIsolatedFixedWidthBlockBuilderFactory(BOOLEAN);
    public static final BlockEncodingFactory<?> BLOCK_ENCODING_FACTORY = BLOCK_BUILDER_FACTORY.getBlockEncodingFactory();

    private BooleanType()
    {
    }

    @Override
    public String getName()
    {
        return "boolean";
    }

    @Override
    public Class<?> getJavaType()
    {
        return boolean.class;
    }

    @Override
    public int getFixedSize()
    {
        return (int) SIZE_OF_BYTE;
    }

    @Override
    public Object getObjectValue(Session session, Slice slice, int offset)
    {
        return slice.getByte(offset) != 0;
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
        return slice.getByte(offset) != 0;
    }

    @Override
    public void setBoolean(SliceOutput sliceOutput, boolean value)
    {
        sliceOutput.writeByte(value ? 1 : 0);
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
    public void setSlice(SliceOutput sliceOutput, Slice value, int offset)
    {
        sliceOutput.writeBytes(value, offset, SIZE_OF_BYTE);
    }

    @Override
    public boolean equals(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        boolean leftValue = leftSlice.getByte(leftOffset) != 0;
        boolean rightValue = rightSlice.getByte(rightOffset) != 0;
        return leftValue == rightValue;
    }

    @Override
    public boolean equals(Slice leftSlice, int leftOffset, BlockCursor rightCursor)
    {
        boolean leftValue = leftSlice.getByte(leftOffset) != 0;
        boolean rightValue = rightCursor.getBoolean();
        return leftValue == rightValue;
    }

    @Override
    public int hashCode(Slice slice, int offset)
    {
        return slice.getByte(offset) != 0 ? 1231 : 1237;
    }

    @Override
    public int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        boolean leftValue = leftSlice.getByte(leftOffset) != 0;
        boolean rightValue = rightSlice.getByte(rightOffset) != 0;
        return Boolean.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Slice slice, int offset, BlockBuilder blockBuilder)
    {
        boolean value = slice.getByte(offset) != 0;
        blockBuilder.append(value);
    }

    @Override
    public void appendTo(Slice slice, int offset, SliceOutput sliceOutput)
    {
        sliceOutput.writeBytes(slice, offset, (int) SIZE_OF_BYTE);
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
