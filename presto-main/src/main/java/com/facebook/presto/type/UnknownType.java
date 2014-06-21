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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.FixedWidthBlockUtil.FixedWidthBlockBuilderFactory;
import com.facebook.presto.spi.type.FixedWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.block.FixedWidthBlockUtil.createIsolatedFixedWidthBlockBuilderFactory;

public final class UnknownType
        implements FixedWidthType
{
    public static final UnknownType UNKNOWN = new UnknownType();

    private static final FixedWidthBlockBuilderFactory BLOCK_BUILDER_FACTORY = createIsolatedFixedWidthBlockBuilderFactory(UNKNOWN);
    public static final BlockEncodingFactory<?> BLOCK_ENCODING_FACTORY = BLOCK_BUILDER_FACTORY.getBlockEncodingFactory();

    private UnknownType()
    {
    }

    @Override
    public String getName()
    {
        return "unknown";
    }

    @Override
    public Class<?> getJavaType()
    {
        return void.class;
    }

    @Override
    public int getFixedSize()
    {
        return 0;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        // This type is always null, so this method should never be called
        throw new UnsupportedOperationException();
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
    public int hash(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLong(SliceOutput sliceOutput, long value)
    {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeSlice(SliceOutput sliceOutput, Slice value, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
