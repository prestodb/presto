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
package com.facebook.presto.spi.function.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class GroupByIdBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupByIdBlock.class).instanceSize();

    private final long groupCount;
    private final Block block;

    public GroupByIdBlock(long groupCount, Block block)
    {
        requireNonNull(block, "block is null");
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

    public boolean isRunLengthBlock() {
        return block instanceof RunLengthEncodedBlock;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        return block.getRegion(positionOffset, length);
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        return block.getRegionSizeInBytes(positionOffset, length);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int usedPositionCount)
    {
        return block.getPositionsSizeInBytes(positions, usedPositionCount);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        return block.copyRegion(positionOffset, length);
    }

    @Override
    public int getSliceLength(int position)
    {
        return block.getSliceLength(position);
    }

    @Override
    public byte getByte(int position)
    {
        return block.getByte(position);
    }

    @Override
    public short getShort(int position)
    {
        return block.getShort(position);
    }

    @Override
    public int getInt(int position)
    {
        return block.getInt(position);
    }

    @Override
    public long getLong(int position)
    {
        return block.getLong(position);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return block.getLong(position, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return block.getSlice(position, offset, length);
    }

    @Override
    public Block getBlock(int position)
    {
        return block.getBlock(position);
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
    public void writeBytesTo(int position, int offset, int length, SliceOutput sliceOutput)
    {
        block.writeBytesTo(position, offset, length, sliceOutput);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        block.writePositionTo(position, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        block.writePositionTo(position, output);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return block.equals(position, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
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
    public boolean mayHaveNull()
    {
        return block.mayHaveNull();
    }

    @Override
    public int getPositionCount()
    {
        return block.getPositionCount();
    }

    @Override
    public long getSizeInBytes()
    {
        return block.getSizeInBytes();
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return block.fixedSizeInBytesPerPosition();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + block.getRetainedSizeInBytes();
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return block.getEstimatedDataSizeForStats(position);
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(block, block.getRetainedSizeInBytes());
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        throw new UnsupportedOperationException("GroupByIdBlock does not support serialization");
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        return block.copyPositions(positions, offset, length);
    }

    @Override
    public String toString()
    {
        return "GroupByIdBlock{" +
                "groupCount=" + groupCount +
                ", positionCount=" + getPositionCount() +
                '}';
    }

    @Override
    public Block getLoadedBlock()
    {
        return block.getLoadedBlock();
    }

    @Override
    public byte getByteUnchecked(int internalPosition)
    {
        return block.getByte(internalPosition);
    }

    @Override
    public short getShortUnchecked(int internalPosition)
    {
        return block.getShort(internalPosition);
    }

    @Override
    public int getIntUnchecked(int internalPosition)
    {
        return block.getInt(internalPosition);
    }

    @Override
    public long getLongUnchecked(int internalPosition)
    {
        return block.getLong(internalPosition);
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        return block.getLong(internalPosition, offset);
    }

    @Override
    public Slice getSliceUnchecked(int internalPosition, int offset, int length)
    {
        return block.getSlice(internalPosition, offset, length);
    }

    @Override
    public int getSliceLengthUnchecked(int internalPosition)
    {
        return block.getSliceLength(internalPosition);
    }

    @Override
    public Block getBlockUnchecked(int internalPosition)
    {
        return block.getBlock(internalPosition);
    }

    @Override
    public int getOffsetBase()
    {
        return 0;
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        return block.isNull(internalPosition);
    }

    @Override
    public Block appendNull()
    {
        throw new UnsupportedOperationException("GroupByIdBlock does not support appendNull()");
    }
}
