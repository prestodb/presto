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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding.BlockEncodingFactory;
import com.facebook.presto.block.FixedWidthBlockUtil.FixedWidthBlockBuilderFactory;
import com.facebook.presto.spi.ColumnType;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.block.FixedWidthBlockUtil.createIsolatedFixedWidthBlockBuilderFactory;

public final class NullType
        implements FixedWidthType
{
    public static final NullType NULL = new NullType();

    private static final FixedWidthBlockBuilderFactory BLOCK_BUILDER_FACTORY = createIsolatedFixedWidthBlockBuilderFactory(NULL);
    public static final BlockEncodingFactory<?> BLOCK_ENCODING_FACTORY = BLOCK_BUILDER_FACTORY.getBlockEncodingFactory();

    private NullType()
    {
    }

    @Override
    public String getName()
    {
        return "null";
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
    public ColumnType toColumnType()
    {
        return ColumnType.NULL;
    }

    @Override
    public Object getObjectValue(Slice slice, int offset)
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSlice(SliceOutput sliceOutput, Slice value, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Slice leftSlice, int leftOffset, BlockCursor rightCursor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode(Slice slice, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Slice slice, int offset, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Slice slice, int offset, SliceOutput sliceOutput)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
