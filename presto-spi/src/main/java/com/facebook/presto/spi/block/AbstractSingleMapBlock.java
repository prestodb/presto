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

package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;

public abstract class AbstractSingleMapBlock
        implements Block
{
    private final int offset;
    private final Block keyBlock;
    private final Block valueBlock;

    public AbstractSingleMapBlock(int offset, Block keyBlock, Block valueBlock)
    {
        this.offset = offset;
        this.keyBlock = keyBlock;
        this.valueBlock = valueBlock;
    }

    private int getAbsolutePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
        return position + offset;
    }

    @Override
    public boolean isNull(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            if (keyBlock.isNull(position / 2)) {
                throw new IllegalStateException("Map key is null");
            }
            return false;
        }
        else {
            return valueBlock.isNull(position / 2);
        }
    }

    @Override
    public byte getByte(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.getByte(position / 2, offset);
        }
        else {
            return valueBlock.getByte(position / 2, offset);
        }
    }

    @Override
    public short getShort(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.getShort(position / 2, offset);
        }
        else {
            return valueBlock.getShort(position / 2, offset);
        }
    }

    @Override
    public int getInt(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.getInt(position / 2, offset);
        }
        else {
            return valueBlock.getInt(position / 2, offset);
        }
    }

    @Override
    public long getLong(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.getLong(position / 2, offset);
        }
        else {
            return valueBlock.getLong(position / 2, offset);
        }
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.getSlice(position / 2, offset, length);
        }
        else {
            return valueBlock.getSlice(position / 2, offset, length);
        }
    }

    @Override
    public int getSliceLength(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.getSliceLength(position / 2);
        }
        else {
            return valueBlock.getSliceLength(position / 2);
        }
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.compareTo(position / 2, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
        }
        else {
            return valueBlock.compareTo(position / 2, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
        }
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.bytesEqual(position / 2, offset, otherSlice, otherOffset, length);
        }
        else {
            return valueBlock.bytesEqual(position / 2, offset, otherSlice, otherOffset, length);
        }
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.bytesCompare(position / 2, offset, length, otherSlice, otherOffset, otherLength);
        }
        else {
            return valueBlock.bytesCompare(position / 2, offset, length, otherSlice, otherOffset, otherLength);
        }
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            keyBlock.writeBytesTo(position / 2, offset, length, blockBuilder);
        }
        else {
            valueBlock.writeBytesTo(position / 2, offset, length, blockBuilder);
        }
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.equals(position / 2, offset, otherBlock, otherPosition, otherOffset, length);
        }
        else {
            return valueBlock.equals(position / 2, offset, otherBlock, otherPosition, otherOffset, length);
        }
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.hash(position / 2, offset, length);
        }
        else {
            return valueBlock.hash(position / 2, offset, length);
        }
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.getObject(position / 2, clazz);
        }
        else {
            return valueBlock.getObject(position / 2, clazz);
        }
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            keyBlock.writePositionTo(position / 2, blockBuilder);
        }
        else {
            valueBlock.writePositionTo(position / 2, blockBuilder);
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return keyBlock.getSingleValueBlock(position / 2);
        }
        else {
            return valueBlock.getSingleValueBlock(position / 2);
        }
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
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
}
