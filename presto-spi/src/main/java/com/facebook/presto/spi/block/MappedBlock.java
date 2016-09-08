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

import static java.util.stream.Collectors.toList;

/**
 * This block maps underlying block positions into new one.
 * For example it can hide every other position.
 */
public class MappedBlock
        implements Block
{
    private final Block block;
    private final int[] positionsMapping;
    private final int mappingOffset;
    private final int mappingLength;

    public MappedBlock(Block block, int[] positionsMapping)
    {
        this(block, positionsMapping, 0, positionsMapping.length);
    }

    public MappedBlock(Block block, int[] positionsMapping, int mappingOffset, int mappingLength)
    {
        this.block = block;
        this.positionsMapping = positionsMapping;
        this.mappingOffset = mappingOffset;
        this.mappingLength = mappingLength;
    }

    @Override
    public int getLength(int position)
    {
        return block.getLength(mapPosition(position));
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return block.getByte(mapPosition(position), offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return block.getShort(mapPosition(position), offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return block.getInt(mapPosition(position), offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return block.getLong(mapPosition(position), offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return block.getSlice(mapPosition(position), offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        return block.getObject(mapPosition(position), clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return block.bytesEqual(mapPosition(position), offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return block.bytesCompare(mapPosition(position), offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        block.writeBytesTo(mapPosition(position), offset, length, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        block.writePositionTo(mapPosition(position), blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return block.equals(mapPosition(position), offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        return block.hash(mapPosition(position), offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return block.compareTo(mapPosition(leftPosition), leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return block.getSingleValueBlock(mapPosition(position));
    }

    @Override
    public int getPositionCount()
    {
        return mappingLength;
    }

    @Override
    public int getSizeInBytes()
    {
        return block.getSizeInBytes();
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return block.getRetainedSizeInBytes();
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new MappedBlockEncoding(block.getEncoding());
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        List<Integer> mappedPositions = positions.stream().map(this::mapPosition).collect(toList());
        return block.copyPositions(mappedPositions);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        return new MappedBlock(block, positionsMapping, mappingOffset + positionOffset, length);
    }

    @Override
    public Block copyRegion(int startPosition, int length)
    {
        List<Integer> mappedPositions = new ArrayList<>(length);
        for (int position = startPosition; position < startPosition + length; position++) {
            mappedPositions.add(mapPosition(position));
        }
        return block.copyPositions(mappedPositions);
    }

    @Override
    public boolean isNull(int position)
    {
        return block.isNull(mapPosition(position));
    }

    @Override
    public void assureLoaded()
    {
        block.assureLoaded();
    }

    private int mapPosition(int position)
    {
        checkReadablePosition(position);
        return positionsMapping[position + mappingOffset];
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
