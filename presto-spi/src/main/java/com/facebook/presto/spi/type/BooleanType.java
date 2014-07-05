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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import io.airlift.slice.Slice;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public final class BooleanType
        implements FixedWidthType
{
    public static final BooleanType BOOLEAN = new BooleanType();

    public static BooleanType getInstance()
    {
        return BOOLEAN;
    }

    private BooleanType()
    {
    }

    @Override
    public String getName()
    {
        return "boolean";
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
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
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return block.getByte(position, 0) != 0;
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
        boolean leftValue = leftBlock.getByte(leftPosition, 0) != 0;
        boolean rightValue = rightBlock.getByte(rightPosition, 0) != 0;
        return leftValue == rightValue;
    }

    @Override
    public int hash(Block block, int position)
    {
        boolean value = block.getByte(position, 0) != 0;
        return value ? 1231 : 1237;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        boolean leftValue = leftBlock.getByte(leftPosition, 0) != 0;
        boolean rightValue = rightBlock.getByte(rightPosition, 0) != 0;
        return Boolean.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeByte(block.getByte(position, 0)).closeEntry();
        }
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        return block.getByte(position, 0) != 0;
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        blockBuilder.writeByte(value ? 1 : 0).closeEntry();
    }

    @Override
    public long getLong(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, getFixedSize());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
