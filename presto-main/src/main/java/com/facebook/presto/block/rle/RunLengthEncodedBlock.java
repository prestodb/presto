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
package com.facebook.presto.block.rle;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;

public class RunLengthEncodedBlock
        implements RandomAccessBlock
{
    private final RandomAccessBlock value;
    private final int positionCount;

    public RunLengthEncodedBlock(RandomAccessBlock value, int positionCount)
    {
        this.value = checkNotNull(value, "value is null");
        checkArgument(value.getPositionCount() == 1, "Expected value to contain a single position but has %s positions", value.getPositionCount());

        // value can not be a RunLengthEncodedBlock because this could cause stack overflow in some of the methods
        checkArgument(!(value instanceof RunLengthEncodedBlock), "Value can not be an instance of a %s", getClass().getName());

        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = checkNotNull(positionCount, "positionCount is null");
    }

    public RandomAccessBlock getValue()
    {
        return value;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        return value.getSizeInBytes();
    }

    @Override
    public RunLengthBlockEncoding getEncoding()
    {
        return new RunLengthBlockEncoding(value.getEncoding());
    }

    @Override
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return new RunLengthEncodedBlock(value, length);
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        return this;
    }

    @Override
    public Type getType()
    {
        return value.getType();
    }

    @Override
    public boolean getBoolean(int position)
    {
        checkReadablePosition(position);
        return value.getBoolean(0);
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return value.getLong(0);
    }

    @Override
    public double getDouble(int position)
    {
        checkReadablePosition(position);
        return value.getDouble(0);
    }

    @Override
    public Object getObjectValue(int position)
    {
        checkReadablePosition(position);
        return value.getObjectValue(0);
    }

    @Override
    public Slice getSlice(int position)
    {
        checkReadablePosition(position);
        return value.getSlice(0);
    }

    @Override
    public RandomAccessBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return value;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return value.isNull(0);
    }

    @Override
    public boolean equals(int position, RandomAccessBlock right, int rightPosition)
    {
        checkReadablePosition(position);
        return value.equals(0, right, rightPosition);
    }

    @Override
    public boolean equals(int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        return this.value.equals(0, cursor);
    }

    @Override
    public boolean equals(int position, Slice slice, int offset)
    {
        checkReadablePosition(position);
        return value.equals(0, slice, offset);
    }

    @Override
    public int hashCode(int position)
    {
        checkReadablePosition(position);
        return value.hashCode(0);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock right, int rightPosition)
    {
        checkReadablePosition(position);
        return value.compareTo(sortOrder, 0, right, rightPosition);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        return value.compareTo(sortOrder, 0, cursor);
    }

    @Override
    public int compareTo(int position, Slice slice, int offset)
    {
        checkReadablePosition(position);
        return value.compareTo(0, slice, offset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        value.appendTo(0, blockBuilder);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", value)
                .add("positionCount", positionCount)
                .toString();
    }

    @Override
    public RunLengthEncodedBlockCursor cursor()
    {
        return new RunLengthEncodedBlockCursor(value, positionCount);
    }

    private void checkReadablePosition(int position)
    {
        checkState(position >= 0 && position < positionCount, "position is not valid");
    }
}
