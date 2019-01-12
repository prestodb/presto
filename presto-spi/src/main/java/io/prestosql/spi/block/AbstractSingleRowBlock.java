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

package io.prestosql.spi.block;

import io.airlift.slice.Slice;

public abstract class AbstractSingleRowBlock
        implements Block
{
    protected final int rowIndex;

    protected AbstractSingleRowBlock(int rowIndex)
    {
        this.rowIndex = rowIndex;
    }

    protected abstract Block getRawFieldBlock(int fieldIndex);

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
        return getRawFieldBlock(position).isNull(rowIndex);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).getByte(rowIndex, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).getShort(rowIndex, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).getInt(rowIndex, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).getLong(rowIndex, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).getSlice(rowIndex, offset, length);
    }

    @Override
    public int getSliceLength(int position)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).getSliceLength(rowIndex);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).compareTo(rowIndex, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).bytesEqual(rowIndex, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).bytesCompare(rowIndex, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        checkFieldIndex(position);
        getRawFieldBlock(position).writeBytesTo(rowIndex, offset, length, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).equals(rowIndex, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).hash(rowIndex, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).getObject(rowIndex, clazz);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkFieldIndex(position);
        getRawFieldBlock(position).writePositionTo(rowIndex, blockBuilder);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).getSingleValueBlock(rowIndex);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkFieldIndex(position);
        return getRawFieldBlock(position).getEstimatedDataSizeForStats(rowIndex);
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
}
