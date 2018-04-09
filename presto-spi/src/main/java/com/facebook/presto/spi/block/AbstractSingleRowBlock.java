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

public abstract class AbstractSingleRowBlock
        implements Block
{
    protected final int rowIndex;

    protected AbstractSingleRowBlock(int rowIndex)
    {
        this.rowIndex = rowIndex;
    }

    protected abstract Block getFieldBlock(int fieldIndex);

    private void checkFieldIndex(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid: " + position);
        }
    }

    @Override
    public boolean isNull(int position)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).isNull(rowIndex);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).getByte(rowIndex, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).getShort(rowIndex, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).getInt(rowIndex, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).getLong(rowIndex, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).getSlice(rowIndex, offset, length);
    }

    @Override
    public int getSliceLength(int position)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).getSliceLength(rowIndex);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).compareTo(rowIndex, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).bytesEqual(rowIndex, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).bytesCompare(rowIndex, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        checkFieldIndex(position);
        getFieldBlock(position).writeBytesTo(rowIndex, offset, length, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).equals(rowIndex, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).hash(rowIndex, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).getObject(rowIndex, clazz);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkFieldIndex(position);
        getFieldBlock(position).writePositionTo(rowIndex, blockBuilder);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkFieldIndex(position);
        return getFieldBlock(position).getSingleValueBlock(rowIndex);
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
