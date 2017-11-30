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

import java.util.List;

public abstract class AbstractSingleMapBlock
        implements Block
{
    abstract int getOffset();

    abstract Block getKeyBlock();

    abstract Block getValueBlock();

    private int getAbsolutePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
        return position + getOffset();
    }

    @Override
    public boolean isNull(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            if (getKeyBlock().isNull(position / 2)) {
                throw new IllegalStateException("Map key is null");
            }
            return false;
        }
        else {
            return getValueBlock().isNull(position / 2);
        }
    }

    @Override
    public byte getByte(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().getByte(position / 2, offset);
        }
        else {
            return getValueBlock().getByte(position / 2, offset);
        }
    }

    @Override
    public short getShort(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().getShort(position / 2, offset);
        }
        else {
            return getValueBlock().getShort(position / 2, offset);
        }
    }

    @Override
    public int getInt(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().getInt(position / 2, offset);
        }
        else {
            return getValueBlock().getInt(position / 2, offset);
        }
    }

    @Override
    public long getLong(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().getLong(position / 2, offset);
        }
        else {
            return getValueBlock().getLong(position / 2, offset);
        }
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().getSlice(position / 2, offset, length);
        }
        else {
            return getValueBlock().getSlice(position / 2, offset, length);
        }
    }

    @Override
    public int getSliceLength(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().getSliceLength(position / 2);
        }
        else {
            return getValueBlock().getSliceLength(position / 2);
        }
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().compareTo(position / 2, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
        }
        else {
            return getValueBlock().compareTo(position / 2, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
        }
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().bytesEqual(position / 2, offset, otherSlice, otherOffset, length);
        }
        else {
            return getValueBlock().bytesEqual(position / 2, offset, otherSlice, otherOffset, length);
        }
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().bytesCompare(position / 2, offset, length, otherSlice, otherOffset, otherLength);
        }
        else {
            return getValueBlock().bytesCompare(position / 2, offset, length, otherSlice, otherOffset, otherLength);
        }
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            getKeyBlock().writeBytesTo(position / 2, offset, length, blockBuilder);
        }
        else {
            getValueBlock().writeBytesTo(position / 2, offset, length, blockBuilder);
        }
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().equals(position / 2, offset, otherBlock, otherPosition, otherOffset, length);
        }
        else {
            return getValueBlock().equals(position / 2, offset, otherBlock, otherPosition, otherOffset, length);
        }
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().hash(position / 2, offset, length);
        }
        else {
            return getValueBlock().hash(position / 2, offset, length);
        }
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().getObject(position / 2, clazz);
        }
        else {
            return getValueBlock().getObject(position / 2, clazz);
        }
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            getKeyBlock().writePositionTo(position / 2, blockBuilder);
        }
        else {
            getValueBlock().writePositionTo(position / 2, blockBuilder);
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return getKeyBlock().getSingleValueBlock(position / 2);
        }
        else {
            return getValueBlock().getSingleValueBlock(position / 2);
        }
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyPositions(List<Integer> positions)
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
