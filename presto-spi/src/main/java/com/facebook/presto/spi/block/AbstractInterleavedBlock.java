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

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractInterleavedBlock
        implements Block
{
    private final int columns;

    protected abstract Block getBlock(int blockIndex);

    protected abstract int toAbsolutePosition(int position);

    @Override
    public abstract InterleavedBlockEncoding getEncoding();

    protected AbstractInterleavedBlock(int columns)
    {
        if (columns <= 0) {
            throw new IllegalArgumentException("Number of blocks in InterleavedBlock must be positive");
        }
        this.columns = columns;
    }

    int getBlockCount()
    {
        return columns;
    }

    Block[] computeSerializableSubBlocks()
    {
        InterleavedBlock interleavedBlock = (InterleavedBlock) sliceRange(0, getPositionCount(), false);
        Block[] result = new Block[interleavedBlock.getBlockCount()];
        for (int i = 0; i < result.length; i++) {
            result[i] = interleavedBlock.getBlock(i);
        }
        return result;
    }

    /**
     * Can only be called after the child class is initialized enough that getBlock will return the right value
     */
    protected InterleavedBlockEncoding computeBlockEncoding()
    {
        BlockEncoding[] individualBlockEncodings = new BlockEncoding[columns];
        for (int i = 0; i < columns; i++) {
            Block block = getBlock(i);
            individualBlockEncodings[i] = block.getEncoding();
        }
        return new InterleavedBlockEncoding(individualBlockEncodings);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        getBlock(blockIndex).writePositionTo(positionInBlock, blockBuilder);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getByte(positionInBlock, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getShort(positionInBlock, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getInt(positionInBlock, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getLong(positionInBlock, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getSlice(positionInBlock, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getObject(positionInBlock, clazz);
    }

    @Override
    public int getLength(int position)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getLength(positionInBlock);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).equals(positionInBlock, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).bytesEqual(positionInBlock, offset, otherSlice, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).hash(positionInBlock, offset, length);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).compareTo(positionInBlock, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).bytesCompare(positionInBlock, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        getBlock(blockIndex).writeBytesTo(positionInBlock, offset, length, blockBuilder);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        // return the underlying block directly, as it is unnecessary to wrap around it if there's only one block
        return getBlock(blockIndex).getSingleValueBlock(positionInBlock);
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        if (positions.size() % columns != 0) {
            throw new IllegalArgumentException("Positions.size (" + positions.size() + ") is not evenly dividable by columns (" + columns + ")");
        }
        int positionsPerColumn = positions.size() / columns;

        List<List<Integer>> valuePositions = new ArrayList<>(columns);
        for (int i = 0; i < columns; i++) {
            valuePositions.add(new ArrayList<>(positionsPerColumn));
        }
        int ordinal = 0;
        for (int position : positions) {
            position = toAbsolutePosition(position);
            if (ordinal % columns != position % columns) {
                throw new IllegalArgumentException("Position (" + position + ") is not congruent to ordinal (" + ordinal + ") modulo columns (" + columns + ")");
            }
            valuePositions.get(position % columns).add(position / columns);
            ordinal++;
        }
        Block[] blocks = new Block[columns];
        for (int i = 0; i < columns; i++) {
            blocks[i] = getBlock(i).copyPositions(valuePositions.get(i));
        }
        return new InterleavedBlock(blocks);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        validateRange(position, length);
        return sliceRange(position, length, true);
    }

    protected void validateRange(int position, int length)
    {
        int positionCount = getPositionCount();
        if (position < 0 || length < 0 || position + length > positionCount || position % columns != 0 || length % columns != 0) {
            throw new IndexOutOfBoundsException("Invalid position (" + position + "), length (" + length + ") in InterleavedBlock with " + positionCount + " positions and " + columns + " columns");
        }
    }

    protected Block sliceRange(int position, int length, boolean compact)
    {
        position = toAbsolutePosition(position);
        Block[] resultBlocks = new Block[columns];
        int positionInBlock = position / columns;
        int subBlockLength = length / columns;
        for (int blockIndex = 0; blockIndex < columns; blockIndex++) {
            if (compact) {
                resultBlocks[blockIndex] = getBlock((blockIndex + position) % columns).copyRegion(positionInBlock, subBlockLength);
            }
            else {
                resultBlocks[blockIndex] = getBlock((blockIndex + position) % columns).getRegion(positionInBlock, subBlockLength);
            }
        }
        return new InterleavedBlock(resultBlocks);
    }

    @Override
    public boolean isNull(int position)
    {
        position = toAbsolutePosition(position);
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).isNull(positionInBlock);
    }
}
