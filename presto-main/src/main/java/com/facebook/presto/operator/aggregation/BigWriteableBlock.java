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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.LongBigArray;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static java.util.Objects.requireNonNull;

public class BigWriteableBlock
        implements BlockBuilder
{
    private final List<Block> blocks = new ArrayList<>();
    private final LongBigArray addresses = new LongBigArray();
    private final Type type;
    private int completedBlockSize;
    private int nextPosition;
    private BlockBuilder currentBuilder;

    public BigWriteableBlock(Type type, int expectedSize)
    {
        this.type = requireNonNull(type, "type is null");
        this.currentBuilder = type.createBlockBuilder(new BlockBuilderStatus(), expectedSize);
        blocks.add(currentBuilder);
    }

    @Override
    public BlockBuilder writeByte(int value)
    {
        currentBuilder.writeByte(value);
        return this;
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        currentBuilder.writeShort(value);
        return this;
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        currentBuilder.writeInt(value);
        return this;
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        currentBuilder.writeLong(value);
        return this;
    }

    @Override
    public BlockBuilder writeFloat(float v)
    {
        currentBuilder.writeFloat(v);
        return this;
    }

    @Override
    public BlockBuilder writeDouble(double value)
    {
        currentBuilder.writeDouble(value);
        return this;
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        currentBuilder.writeBytes(source, sourceIndex, length);
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        currentBuilder.closeEntry();
        recordAddress();
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        currentBuilder.appendNull();
        recordAddress();
        return this;
    }

    private void recordAddress()
    {
        addresses.ensureCapacity(nextPosition + 1);
        addresses.set(nextPosition, encodeSyntheticAddress(blocks.size() - 1, currentBuilder.getPositionCount() - 1));
        nextPosition++;

        // Start a new block is necessary
        if (currentBuilder.isFull()) {
            Block block = currentBuilder.build();
            completedBlockSize += block.getSizeInBytes();
            // currentBuilder is already at the end of the arrays, so replace it
            blocks.set(blocks.size() - 1, block);
            // Assume that the next block will be about the same size
            currentBuilder = type.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount(), block.getSizeInBytes() / block.getPositionCount());
        }
    }

    @Override
    public Block build()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty()
    {
        return blocks.size() == 1 && currentBuilder.isEmpty();
    }

    @Override
    public boolean isFull()
    {
        // Conceptually this would return "false", but if it's ever called this is probably being used in the wrong context
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLength(int position)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).getLength(blockPosition);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).getByte(blockPosition, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).getShort(blockPosition, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).getInt(blockPosition, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).getLong(blockPosition, offset);
    }

    @Override
    public float getFloat(int position, int offset)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).getFloat(blockPosition, offset);
    }

    @Override
    public double getDouble(int position, int offset)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).getDouble(blockPosition, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).getSlice(blockPosition, offset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).bytesEqual(blockPosition, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).bytesCompare(blockPosition, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        blocks.get(block).writeBytesTo(blockPosition, offset, length, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).equals(blockPosition, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public int hash(int position, int offset, int length)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).hash(blockPosition, offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        int block = decodeSliceIndex(addresses.get(leftPosition));
        int blockPosition = decodePosition(addresses.get(leftPosition));
        return blocks.get(block).compareTo(blockPosition, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).getSingleValueBlock(blockPosition);
    }

    @Override
    public int getPositionCount()
    {
        return nextPosition;
    }

    @Override
    public int getSizeInBytes()
    {
        return completedBlockSize + currentBuilder.getSizeInBytes();
    }

    @Override
    public BlockEncoding getEncoding()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int position)
    {
        int block = decodeSliceIndex(addresses.get(position));
        int blockPosition = decodePosition(addresses.get(position));
        return blocks.get(block).isNull(blockPosition);
    }

    @Override
    public void assureLoaded()
    {
        currentBuilder.assureLoaded();
    }
}
