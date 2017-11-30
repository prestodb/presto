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

public abstract class AbstractSingleRowBlock
        implements Block
{
    // in AbstractSingleRowBlock, offset is position-based (consider as cell-based), not entry-based.
    protected final int startOffset;

    protected final int numFields;

    protected abstract Block getFieldBlock(int fieldIndex);

    protected AbstractSingleRowBlock(int startOffset, int numFields)
    {
        this.startOffset = startOffset;
        this.numFields = numFields;
    }

    private int getAbsolutePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
        return position + startOffset;
    }

    @Override
    public boolean isNull(int position)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).isNull(position / numFields);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).getByte(position / numFields, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).getShort(position / numFields, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).getInt(position / numFields, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).getLong(position / numFields, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).getSlice(position / numFields, offset, length);
    }

    @Override
    public int getSliceLength(int position)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).getSliceLength(position / numFields);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).compareTo(position / numFields, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).bytesEqual(position / numFields, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).bytesCompare(position / numFields, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        position = getAbsolutePosition(position);
        getFieldBlock(position % numFields).writeBytesTo(position / numFields, offset, length, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).equals(position / numFields, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).hash(position / numFields, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).getObject(position / numFields, clazz);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        position = getAbsolutePosition(position);
        getFieldBlock(position % numFields).writePositionTo(position / numFields, blockBuilder);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        position = getAbsolutePosition(position);
        return getFieldBlock(position % numFields).getSingleValueBlock(position / numFields);
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
