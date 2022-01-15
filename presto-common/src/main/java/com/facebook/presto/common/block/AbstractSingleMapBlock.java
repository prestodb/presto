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

import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;

public abstract class AbstractSingleMapBlock
        implements Block
{
    abstract int getOffset();

    abstract Block getRawKeyBlock();

    abstract Block getRawValueBlock();

    private int getAbsolutePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid: " + position);
        }
        return position + getOffset();
    }

    @Override
    public boolean isNull(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            if (getRawKeyBlock().isNull(position / 2)) {
                throw new IllegalStateException("Map key is null at position: " + position);
            }
            return false;
        }
        else {
            return getRawValueBlock().isNull(position / 2);
        }
    }

    @Override
    public byte getByte(int position)
    {
        return getByteUnchecked(getAbsolutePosition(position));
    }

    @Override
    public short getShort(int position)
    {
        return getShortUnchecked(getAbsolutePosition(position));
    }

    @Override
    public int getInt(int position)
    {
        return getIntUnchecked(getAbsolutePosition(position));
    }

    @Override
    public long getLong(int position)
    {
        return getLongUnchecked(getAbsolutePosition(position));
    }

    @Override
    public long getLong(int position, int offset)
    {
        return getLongUnchecked(getAbsolutePosition(position), offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return getSliceUnchecked(getAbsolutePosition(position), offset, length);
    }

    @Override
    public int getSliceLength(int position)
    {
        return getSliceLengthUnchecked(getAbsolutePosition(position));
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getRawKeyBlock().compareTo(position / 2, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
        }
        else {
            return getRawValueBlock().compareTo(position / 2, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
        }
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getRawKeyBlock().bytesEqual(position / 2, offset, otherSlice, otherOffset, length);
        }
        else {
            return getRawValueBlock().bytesEqual(position / 2, offset, otherSlice, otherOffset, length);
        }
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getRawKeyBlock().bytesCompare(position / 2, offset, length, otherSlice, otherOffset, otherLength);
        }
        else {
            return getRawValueBlock().bytesCompare(position / 2, offset, length, otherSlice, otherOffset, otherLength);
        }
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            getRawKeyBlock().writeBytesTo(position / 2, offset, length, blockBuilder);
        }
        else {
            getRawValueBlock().writeBytesTo(position / 2, offset, length, blockBuilder);
        }
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getRawKeyBlock().equals(position / 2, offset, otherBlock, otherPosition, otherOffset, length);
        }
        else {
            return getRawValueBlock().equals(position / 2, offset, otherBlock, otherPosition, otherOffset, length);
        }
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getRawKeyBlock().hash(position / 2, offset, length);
        }
        else {
            return getRawValueBlock().hash(position / 2, offset, length);
        }
    }

    @Override
    public Block getBlock(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getRawKeyBlock().getBlock(position / 2);
        }
        else {
            return getRawValueBlock().getBlock(position / 2);
        }
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            getRawKeyBlock().writePositionTo(position / 2, blockBuilder);
        }
        else {
            getRawValueBlock().writePositionTo(position / 2, blockBuilder);
        }
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            getRawKeyBlock().writePositionTo(position / 2, output);
        }
        else {
            getRawValueBlock().writePositionTo(position / 2, output);
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getRawKeyBlock().getSingleValueBlock(position / 2);
        }
        else {
            return getRawValueBlock().getSingleValueBlock(position / 2);
        }
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getRawKeyBlock().getEstimatedDataSizeForStats(position / 2);
        }
        else {
            return getRawValueBlock().getEstimatedDataSizeForStats(position / 2);
        }
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByteUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        if (internalPosition % 2 == 0) {
            return getRawKeyBlock().getByte(internalPosition / 2);
        }
        return getRawValueBlock().getByte(internalPosition / 2);
    }

    @Override
    public short getShortUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        if (internalPosition % 2 == 0) {
            return getRawKeyBlock().getShort(internalPosition / 2);
        }
        return getRawValueBlock().getShort(internalPosition / 2);
    }

    @Override
    public int getIntUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        if (internalPosition % 2 == 0) {
            return getRawKeyBlock().getInt(internalPosition / 2);
        }
        return getRawValueBlock().getInt(internalPosition / 2);
    }

    @Override
    public long getLongUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        if (internalPosition % 2 == 0) {
            return getRawKeyBlock().getLong(internalPosition / 2);
        }
        return getRawValueBlock().getLong(internalPosition / 2);
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        if (internalPosition % 2 == 0) {
            return getRawKeyBlock().getLong(internalPosition / 2, offset);
        }
        return getRawValueBlock().getLong(internalPosition / 2, offset);
    }

    @Override
    public Slice getSliceUnchecked(int internalPosition, int offset, int length)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        if (internalPosition % 2 == 0) {
            return getRawKeyBlock().getSlice(internalPosition / 2, offset, length);
        }
        return getRawValueBlock().getSlice(internalPosition / 2, offset, length);
    }

    @Override
    public int getSliceLengthUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        if (internalPosition % 2 == 0) {
            return getRawKeyBlock().getSliceLength(internalPosition / 2);
        }
        return getRawValueBlock().getSliceLength(internalPosition / 2);
    }

    @Override
    public Block getBlockUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        if (internalPosition % 2 == 0) {
            return getRawKeyBlock().getBlock(internalPosition / 2);
        }
        return getRawValueBlock().getBlock(internalPosition / 2);
    }

    @Override
    public int getOffsetBase()
    {
        return getOffset();
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, getOffset(), getPositionCount());
        // Keys are presumed to be non-null
        if (internalPosition % 2 == 0) {
            return false;
        }
        return getRawValueBlock().isNull(internalPosition / 2);
    }
}
