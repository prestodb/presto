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

public abstract class AbstractArrayElementBlock
        implements Block
{
    protected final int start;

    protected AbstractArrayElementBlock(int start)
    {
        this.start = start;
    }

    protected abstract BlockBuilder getBlock();

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public int getSliceLength(int position)
    {
        checkReadablePosition(position);
        return getBlock().getSliceLength(position + start);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        checkReadablePosition(position);
        return getBlock().getByte(position + start, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkReadablePosition(position);
        return getBlock().getShort(position + start, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkReadablePosition(position);
        return getBlock().getInt(position + start, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        return getBlock().getLong(position + start, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return getBlock().getSlice(position + start, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        checkReadablePosition(position);
        return getBlock().getObject(position + start, clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        checkReadablePosition(position);
        return getBlock().bytesEqual(position + start, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        return getBlock().bytesCompare(position + start, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        getBlock().writeBytesTo(position + start, offset, length, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        getBlock().writePositionTo(position + start, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        checkReadablePosition(position);
        return getBlock().equals(position + start, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return getBlock().hash(position + start, offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        checkReadablePosition(leftPosition);
        return getBlock().compareTo(leftPosition + start, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return getBlock().getSingleValueBlock(position + start);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return getBlock().isNull(position + start);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        // ArrayElementBlockEncoding does not exist
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getRegion(int position, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRegionSizeInBytes(int position, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        throw new UnsupportedOperationException();
    }
}
