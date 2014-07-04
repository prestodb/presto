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
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding.VariableWidthBlockEncodingFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

public class VarcharType
        implements VariableWidthType
{
    public static final VarcharType VARCHAR = new VarcharType();

    public static VarcharType getInstance()
    {
        return VARCHAR;
    }

    public static final BlockEncodingFactory<?> BLOCK_ENCODING_FACTORY = new VariableWidthBlockEncodingFactory(VARCHAR);

    @JsonCreator
    public VarcharType()
    {
    }

    @Override
    public String getName()
    {
        return "varchar";
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
        return Slice.class;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return block.getSlice(position, 0, block.getLength(position)).toStringUtf8();
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int leftLength = leftBlock.getLength(leftPosition);
        int rightLength = rightBlock.getLength(rightPosition);
        if (leftLength != rightLength) {
            return false;
        }
        return leftBlock.equals(leftPosition, 0, rightBlock, rightPosition, 0, leftLength);
    }

    @Override
    public int hash(Block block, int position)
    {
        return block.hash(position, 0, block.getLength(position));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int leftLength = leftBlock.getLength(leftPosition);
        int rightLength = rightBlock.getLength(rightPosition);
        return leftBlock.compareTo(leftPosition, 0, leftLength, rightBlock, rightPosition, 0, rightLength);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        block.appendSliceTo(position, 0, block.getLength(position), blockBuilder);
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getLength(position));
    }

    @Override
    public int writeSlice(SliceOutput sliceOutput, Slice value, int offset, int length)
    {
        sliceOutput.writeBytes(value, offset, length);
        return length;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        return new VariableWidthBlockBuilder(this, blockBuilderStatus);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
