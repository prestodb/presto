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
import io.airlift.slice.SliceOutput;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

//
// A timestamp is stored as milliseconds from 1970-01-01T00:00:00 UTC.  When performing calculations
// on a timestamp the client's time zone must be taken into account.
//
public final class TimestampType
        implements FixedWidthType
{
    public static final TimestampType TIMESTAMP = new TimestampType();

    public static TimestampType getInstance()
    {
        return TIMESTAMP;
    }

    private TimestampType()
    {
    }

    @Override
    public String getName()
    {
        return "timestamp";
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
        return long.class;
    }

    @Override
    public int getFixedSize()
    {
        return (int) SIZE_OF_LONG;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return new SqlTimestamp(block.getLong(position, 0), session.getTimeZoneKey());
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
        long leftValue = leftBlock.getLong(leftPosition, 0);
        long rightValue = rightBlock.getLong(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public int hash(Block block, int position)
    {
        long value = block.getLong(position, 0);
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = leftBlock.getLong(leftPosition, 0);
        long rightValue = rightBlock.getLong(rightPosition, 0);
        return Long.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        long value = block.getLong(position, 0);
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
        return block.getLong(position, 0);
    }

    @Override
    public void writeLong(SliceOutput sliceOutput, long value)
    {
        sliceOutput.writeLong(value);
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
        sliceOutput.writeBytes(value, offset, SIZE_OF_LONG);
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
