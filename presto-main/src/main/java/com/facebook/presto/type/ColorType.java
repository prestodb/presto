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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import com.facebook.presto.spi.type.FixedWidthType;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class ColorType
        implements FixedWidthType
{
    public static final ColorType COLOR = new ColorType();

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
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return false;
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
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        int color = block.getInt(position, 0);
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
        return new FixedWidthBlockBuilder(this, blockBuilderStatus);
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new FixedWidthBlockBuilder(this, positionCount);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int leftValue = leftBlock.getInt(leftPosition, 0);
        int rightValue = rightBlock.getInt(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public int hash(Block block, int position)
    {
        return block.getInt(position, 0);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException("Color is not ordered");
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        int value = block.getInt(position, 0);
        blockBuilder.appendLong(value);
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBoolean(SliceOutput sliceOutput, boolean value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Block block, int position)
    {
        return block.getInt(position, 0);
    }

    @Override
    public void writeLong(SliceOutput sliceOutput, long value)
    {
        sliceOutput.writeInt((int) value);
    }

    @Override
    public double getDouble(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(SliceOutput sliceOutput, double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, getFixedSize());
    }

    @Override
    public void writeSlice(SliceOutput sliceOutput, Slice value, int offset)
    {
        Preconditions.checkArgument(value.length() == SIZE_OF_INT);
        sliceOutput.writeBytes(value, offset, SIZE_OF_INT);
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
