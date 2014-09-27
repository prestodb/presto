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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockEncoding;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkNotNull;

public class GroupByIdBlock
        implements Block
{
    private final long groupCount;
    private final Block block;

    public GroupByIdBlock(long groupCount, Block block)
    {
        checkNotNull(block, "block is null");
        this.groupCount = groupCount;
        this.block = block;
    }

    public long getGroupCount()
    {
        return groupCount;
    }

    public long getGroupId(int position)
    {
        return BIGINT.getLong(block, position);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        return block.getRegion(positionOffset, length);
    }

    @Override
    public int getLength(int position)
    {
        return block.getLength(position);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return block.getByte(position, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return block.getShort(position, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return block.getInt(position, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return block.getLong(position, offset);
    }

    @Override
    public float getFloat(int position, int offset)
    {
        return block.getFloat(position, offset);
    }

    @Override
    public double getDouble(int position, int offset)
    {
        return block.getDouble(position, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return block.getSlice(position, offset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return block.bytesEqual(position, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return block.bytesCompare(position, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        block.writeBytesTo(position, offset, length, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return block.equals(position, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public int hash(int position, int offset, int length)
    {
        return block.hash(position, offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return block.compareTo(leftPosition, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
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
    public BlockEncoding getEncoding()
    {
        return block.getEncoding();
    }

    @Override
    public void assureLoaded()
    {
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
