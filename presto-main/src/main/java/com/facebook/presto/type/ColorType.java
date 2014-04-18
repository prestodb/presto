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

import com.facebook.presto.operator.scalar.ColorFunctions;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.FixedWidthBlockUtil;
import com.facebook.presto.spi.type.FixedWidthType;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.block.FixedWidthBlockUtil.createIsolatedFixedWidthBlockBuilderFactory;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class ColorType
        implements FixedWidthType
{
    public static final ColorType COLOR = new ColorType();

    private static final FixedWidthBlockUtil.FixedWidthBlockBuilderFactory BLOCK_BUILDER_FACTORY = createIsolatedFixedWidthBlockBuilderFactory(COLOR);
    public static final BlockEncodingFactory<?> BLOCK_ENCODING_FACTORY = BLOCK_BUILDER_FACTORY.getBlockEncodingFactory();

    public static ColorType getInstance()
    {
        return COLOR;
    }

    private ColorType()
    {
    }

    @Override
    public String getName()
    {
        return "color";
    }

    @Override
    public Class<?> getJavaType()
    {
        return long.class;
    }

    @Override
    public int getFixedSize()
    {
        return (int) SIZE_OF_INT;
    }

    @Override
    public Object getObjectValue(Session session, Slice slice, int offset)
    {
        int color = slice.getInt(offset);
        if (color < 0) {
            return ColorFunctions.SystemColor.valueOf(-(color + 1)).getName();
        }

        return String.format("#%02x%02x%02x",
                (color >> 16) & 0xFF,
                (color >> 8) & 0xFF,
                color & 0xFF);
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
    public void writeBoolean(SliceOutput sliceOutput, boolean value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Slice slice, int offset)
    {
        return slice.getInt(offset);
    }

    @Override
    public void writeLong(SliceOutput sliceOutput, long value)
    {
        sliceOutput.writeInt((int) value);
    }

    @Override
    public double getDouble(Slice slice, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(SliceOutput sliceOutput, double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(Slice slice, int offset)
    {
        return slice.slice(offset, getFixedSize());
    }

    @Override
    public void writeSlice(SliceOutput sliceOutput, Slice value, int offset)
    {
        Preconditions.checkArgument(value.length() == SIZE_OF_INT);
        sliceOutput.writeBytes(value, offset, SIZE_OF_INT);
    }

    @Override
    public boolean equalTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        return leftSlice.getInt(leftOffset) == rightSlice.getInt(rightOffset);
    }

    @Override
    public boolean equalTo(Slice leftSlice, int leftOffset, BlockCursor rightCursor)
    {
        return leftSlice.getInt(leftOffset) == (int) rightCursor.getLong();
    }

    @Override
    public int hash(Slice slice, int offset)
    {
        return slice.getInt(offset);
    }

    @Override
    public int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        throw new UnsupportedOperationException("Color is not ordered");
    }

    @Override
    public void appendTo(Slice slice, int offset, BlockBuilder blockBuilder)
    {
        blockBuilder.appendLong(slice.getInt(offset));
    }

    @Override
    public void appendTo(Slice slice, int offset, SliceOutput sliceOutput)
    {
        sliceOutput.writeBytes(slice, offset, (int) SIZE_OF_INT);
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
