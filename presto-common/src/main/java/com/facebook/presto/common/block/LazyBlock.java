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
package com.facebook.presto.common.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static java.util.Objects.requireNonNull;

public class LazyBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LazyBlock.class).instanceSize();

    private final int positionCount;
    private LazyBlockLoader<LazyBlock> loader;

    private Block block;

    public LazyBlock(int positionCount, LazyBlockLoader<LazyBlock> loader)
    {
        this.positionCount = positionCount;
        this.loader = requireNonNull(loader, "loader is null");
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSliceLength(int position)
    {
        assureLoaded();
        return block.getSliceLength(position);
    }

    @Override
    public byte getByte(int position)
    {
        assureLoaded();
        return block.getByte(position);
    }

    @Override
    public short getShort(int position)
    {
        assureLoaded();
        return block.getShort(position);
    }

    @Override
    public int getInt(int position)
    {
        assureLoaded();
        return block.getInt(position);
    }

    @Override
    public long getLong(int position)
    {
        assureLoaded();
        return block.getLong(position);
    }

    @Override
    public long getLong(int position, int offset)
    {
        assureLoaded();
        return block.getLong(position, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        assureLoaded();
        return block.getSlice(position, offset, length);
    }

    @Override
    public Block getBlock(int position)
    {
        assureLoaded();
        return block.getBlock(position);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        assureLoaded();
        return block.bytesEqual(position, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        assureLoaded();
        return block.bytesCompare(position,
                offset,
                length,
                otherSlice,
                otherOffset,
                otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        assureLoaded();
        block.writeBytesTo(position, offset, length, blockBuilder);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, SliceOutput sliceOutput)
    {
        assureLoaded();
        block.writeBytesTo(position, offset, length, sliceOutput);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        assureLoaded();
        block.writePositionTo(position, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        assureLoaded();
        block.writePositionTo(position, output);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        assureLoaded();
        return block.equals(position,
                offset,
                otherBlock,
                otherPosition,
                otherOffset,
                length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        assureLoaded();
        return block.hash(position, offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        assureLoaded();
        return block.compareTo(leftPosition,
                leftOffset,
                leftLength,
                rightBlock,
                rightPosition,
                rightOffset,
                rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        assureLoaded();
        return block.getSingleValueBlock(position);
    }

    @Override
    public long getSizeInBytes()
    {
        assureLoaded();
        return block.getSizeInBytes();
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        assureLoaded();
        return block.fixedSizeInBytesPerPosition();
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        assureLoaded();
        return block.getRegionSizeInBytes(position, length);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int usedPositionCount)
    {
        assureLoaded();
        return block.getPositionsSizeInBytes(positions, usedPositionCount);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        assureLoaded();
        return INSTANCE_SIZE + block.getRetainedSizeInBytes();
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        assureLoaded();
        return block.getEstimatedDataSizeForStats(position);
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        assureLoaded();
        block.retainedBytesForEachPart(consumer);
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        assureLoaded();
        return LazyBlockEncoding.NAME;
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        assureLoaded();
        return block.getPositions(positions, offset, length);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        assureLoaded();
        return block.copyPositions(positions, offset, length);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        assureLoaded();
        return block.getRegion(positionOffset, length);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        assureLoaded();
        return block.copyRegion(position, length);
    }

    @Override
    public boolean isNull(int position)
    {
        assureLoaded();
        return block.isNull(position);
    }

    @Override
    public boolean mayHaveNull()
    {
        assureLoaded();
        return block.mayHaveNull();
    }

    public void setBlock(Block block)
    {
        if (this.block != null) {
            throw new IllegalStateException("block already set");
        }
        this.block = requireNonNull(block, "block is null");
    }

    public boolean isLoaded()
    {
        return block != null;
    }

    @Override
    public Block getLoadedBlock()
    {
        assureLoaded();
        return block;
    }

    private void assureLoaded()
    {
        if (block != null) {
            return;
        }
        loader.load(this);

        if (block == null) {
            throw new IllegalArgumentException("Lazy block loader did not load this block");
        }

        // clear reference to loader to free resources, since load was successful
        loader = null;
    }

    @Override
    public byte getByteUnchecked(int internalPosition)
    {
        assert block != null : "block is not loaded";
        return block.getByte(internalPosition);
    }

    @Override
    public short getShortUnchecked(int internalPosition)
    {
        assert block != null : "block is not loaded";
        return block.getShort(internalPosition);
    }

    @Override
    public int getIntUnchecked(int internalPosition)
    {
        assert block != null : "block is not loaded";
        return block.getInt(internalPosition);
    }

    @Override
    public long getLongUnchecked(int internalPosition)
    {
        assert block != null : "block is not loaded";
        return block.getLong(internalPosition);
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        assert block != null : "block is not loaded";
        return block.getLong(internalPosition, offset);
    }

    @Override
    public Slice getSliceUnchecked(int internalPosition, int offset, int length)
    {
        assert block != null : "block is not loaded";
        return block.getSlice(internalPosition, offset, length);
    }

    @Override
    public int getSliceLengthUnchecked(int internalPosition)
    {
        assert block != null : "block is not loaded";
        return block.getSliceLength(internalPosition);
    }

    @Override
    public Block getBlockUnchecked(int internalPosition)
    {
        assert block != null : "block is not loaded";
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
        assert block != null : "block is not loaded";
        return block.isNull(internalPosition);
    }

    @Override
    public Block getUnderlyingValueBlock()
    {
        return block.getUnderlyingValueBlock();
    }

    @Override
    public int getUnderlyingValuePosition(int position)
    {
        return block.getUnderlyingValuePosition(position);
    }

    @Override
    public Block appendNull()
    {
        throw new UnsupportedOperationException("LazyBlock does not support appendNull()");
    }
}
