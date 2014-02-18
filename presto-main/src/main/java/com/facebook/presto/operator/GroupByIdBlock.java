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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;

public class GroupByIdBlock
        implements RandomAccessBlock
{
    private final long groupCount;
    private final RandomAccessBlock block;

    public GroupByIdBlock(long groupCount, RandomAccessBlock block)
    {
        this.groupCount = groupCount;
        this.block = block;
    }

    public long getGroupCount()
    {
        return groupCount;
    }

    public long getGroupId(int position)
    {
        return block.getLong(position);
    }

    @Override
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        return block.getRegion(positionOffset, length);
    }

    @Override
    public boolean getBoolean(int position)
    {
        return block.getBoolean(position);
    }

    @Override
    public long getLong(int position)
    {
        return block.getLong(position);
    }

    @Override
    public double getDouble(int position)
    {
        return block.getDouble(position);
    }

    @Override
    public Object getObjectValue(Session session, int position)
    {
        return block.getObjectValue(session, position);
    }

    @Override
    public Slice getSlice(int position)
    {
        return block.getSlice(position);
    }

    @Override
    public RandomAccessBlock getSingleValueBlock(int position)
    {
        return block.getSingleValueBlock(position);
    }

    @Override
    public boolean isNull(int position)
    {
        return block.isNull(position);
    }

    @Override
    public boolean equals(int position, RandomAccessBlock right, int rightPosition)
    {
        return block.equals(position, right, rightPosition);
    }

    @Override
    public boolean equals(int position, BlockCursor cursor)
    {
        return block.equals(position, cursor);
    }

    @Override
    public boolean equals(int rightPosition, Slice slice, int offset)
    {
        return block.equals(rightPosition, slice, offset);
    }

    @Override
    public int hashCode(int position)
    {
        return block.hashCode(position);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock right, int rightPosition)
    {
        return block.compareTo(sortOrder, position, right, rightPosition);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        return block.compareTo(sortOrder, position, cursor);
    }

    @Override
    public int compareTo(int position, Slice slice, int offset)
    {
        return block.compareTo(position, slice, offset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        block.appendTo(position, blockBuilder);
    }

    @Override
    public Type getType()
    {
        return block.getType();
    }

    @Override
    public int getPositionCount()
    {
        return block.getPositionCount();
    }

    @Override
    public int getSizeInBytes()
    {
        return block.getSizeInBytes();
    }

    @Override
    public BlockCursor cursor()
    {
        return block.cursor();
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return block.getEncoding();
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        return block.toRandomAccessBlock();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("groupCount", groupCount)
                .add("positionCount", getPositionCount())
                .toString();
    }
}
