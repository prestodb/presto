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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class GroupByIdBlock
        implements Block
{
    private final long groupCount;
    private final Block block;

    public GroupByIdBlock(long groupCount, Block block)
    {
        checkNotNull(block, "block is null");
        checkArgument(block.getType().equals(BIGINT));
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
    public Block getRegion(int positionOffset, int length)
    {
        return block.getRegion(positionOffset, length);
    }

    @Override
    public boolean getBoolean(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int position)
    {
        return block.getLong(position);
    }

    @Override
    public double getDouble(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObjectValue(ConnectorSession session, int position)
    {
        return block.getObjectValue(session, position);
    }

    @Override
    public Slice getSlice(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return block.getSingleValueBlock(position);
    }

    @Override
    public boolean isNull(int position)
    {
        return block.isNull(position);
    }

    @Override
    public boolean equalTo(int position, Block otherBlock, int otherPosition)
    {
        return block.equalTo(position, otherBlock, otherPosition);
    }

    @Override
    public boolean equalTo(int position, BlockCursor cursor)
    {
        return block.equalTo(position, cursor);
    }

    @Override
    public boolean equalTo(int position, Slice otherSlice, int otherOffset)
    {
        return block.equalTo(position, otherSlice, otherOffset);
    }

    @Override
    public int hash(int position)
    {
        return block.hash(position);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, Block otherBlock, int otherPosition)
    {
        return block.compareTo(sortOrder, position, otherBlock, otherPosition);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        return block.compareTo(sortOrder, position, cursor);
    }

    @Override
    public int compareTo(int position, Slice otherSlice, int otherOffset)
    {
        return block.compareTo(position, otherSlice, otherOffset);
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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("groupCount", groupCount)
                .add("positionCount", getPositionCount())
                .toString();
    }
}
