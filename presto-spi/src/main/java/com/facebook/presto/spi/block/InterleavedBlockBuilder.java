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

import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class InterleavedBlockBuilder
        extends AbstractInterleavedBlock
        implements BlockBuilder
{
    // TODO: This does not account for the size of the blockEncoding field
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(InterleavedBlockBuilder.class).instanceSize();

    private final BlockBuilder[] blockBuilders;
    private final InterleavedBlockEncoding blockEncoding;

    private int positionCount;
    private int currentBlockIndex;
    private int sizeInBytes;
    private int startSize;
    private int retainedSizeInBytes;
    private int startRetainedSize;

    public InterleavedBlockBuilder(List<Type> types, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                types.stream()
                        .map(t -> t.createBlockBuilder(blockBuilderStatus, roundUpDivide(expectedEntries, types.size())))
                        .toArray(BlockBuilder[]::new)
        );
    }

    public InterleavedBlockBuilder(List<Type> types, BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        this(
                types.stream()
                        .map(t -> t.createBlockBuilder(blockBuilderStatus, roundUpDivide(expectedEntries, types.size()), expectedBytesPerEntry))
                        .toArray(BlockBuilder[]::new)
        );
    }

    private static int roundUpDivide(int dividend, int divisor)
    {
        return (dividend + divisor - 1) / divisor;
    }

    /**
     * Caller of this private constructor is responsible for making sure every element in `blockBuilders` is constructed with the same `blockBuilderStatus`
     */
    private InterleavedBlockBuilder(BlockBuilder[] blockBuilders)
    {
        super(blockBuilders.length);
        this.blockBuilders = requireNonNull(blockBuilders, "blockBuilders is null");
        this.blockEncoding = computeBlockEncoding();
        this.positionCount = 0;
        this.sizeInBytes = 0;
        this.retainedSizeInBytes = INSTANCE_SIZE;
        for (BlockBuilder blockBuilder : blockBuilders) {
            this.sizeInBytes += blockBuilder.getSizeInBytes();
            this.retainedSizeInBytes += blockBuilder.getRetainedSizeInBytes();
        }
        this.startSize = -1;
        this.startRetainedSize = -1;
    }

    @Override
    protected Block getBlock(int blockIndex)
    {
        if (blockIndex < 0) {
            throw new IllegalArgumentException("position is not valid");
        }

        return blockBuilders[blockIndex];
    }

    @Override
    protected int computePosition(int position)
    {
        return position;
    }

    @Override
    public InterleavedBlockEncoding getEncoding()
    {
        return blockEncoding;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    private void recordStartSizesIfNecessary(BlockBuilder blockBuilder)
    {
        if (startSize < 0) {
            startSize = blockBuilder.getSizeInBytes();
        }
        if (startRetainedSize < 0) {
            startRetainedSize = blockBuilder.getRetainedSizeInBytes();
        }
    }

    @Override
    public BlockBuilder writeByte(int value)
    {
        BlockBuilder blockBuilder = blockBuilders[currentBlockIndex];
        recordStartSizesIfNecessary(blockBuilder);
        blockBuilder.writeByte(value);
        return this;
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        BlockBuilder blockBuilder = blockBuilders[currentBlockIndex];
        recordStartSizesIfNecessary(blockBuilder);
        blockBuilder.writeShort(value);
        return this;
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        BlockBuilder blockBuilder = blockBuilders[currentBlockIndex];
        recordStartSizesIfNecessary(blockBuilder);
        blockBuilder.writeInt(value);
        return this;
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        BlockBuilder blockBuilder = blockBuilders[currentBlockIndex];
        recordStartSizesIfNecessary(blockBuilder);
        blockBuilder.writeLong(value);
        return this;
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        BlockBuilder blockBuilder = blockBuilders[currentBlockIndex];
        recordStartSizesIfNecessary(blockBuilder);
        blockBuilder.writeBytes(source, sourceIndex, length);
        return this;
    }

    @Override
    public BlockBuilder writeObject(Object value)
    {
        BlockBuilder blockBuilder = blockBuilders[currentBlockIndex];
        recordStartSizesIfNecessary(blockBuilder);
        blockBuilder.writeObject(value);
        return this;
    }

    @Override
    public BlockBuilder beginBlockEntry()
    {
        BlockBuilder blockBuilder = blockBuilders[currentBlockIndex];
        recordStartSizesIfNecessary(blockBuilder);
        return blockBuilder.beginBlockEntry();
    }

    @Override
    public BlockBuilder closeEntry()
    {
        BlockBuilder blockBuilder = blockBuilders[currentBlockIndex];
        if (startSize < 0 || startRetainedSize < 0) {
            throw new IllegalStateException("closeEntry called before anything is written");
        }
        blockBuilder.closeEntry();
        entryAdded();
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        BlockBuilder blockBuilder = blockBuilders[currentBlockIndex];
        if (startSize >= 0 || startRetainedSize >= 0) {
            throw new IllegalStateException("appendNull called when some entry has been written");
        }
        startSize = blockBuilder.getSizeInBytes();
        startRetainedSize = blockBuilder.getRetainedSizeInBytes();
        blockBuilder.appendNull();
        entryAdded();
        return this;
    }

    private void entryAdded()
    {
        BlockBuilder blockBuilder = blockBuilders[currentBlockIndex];
        sizeInBytes += blockBuilder.getSizeInBytes() - startSize;
        retainedSizeInBytes += blockBuilder.getRetainedSizeInBytes() - startRetainedSize;
        startSize = -1;
        startRetainedSize = -1;

        positionCount++;
        currentBlockIndex++;
        if (currentBlockIndex == getBlockCount()) {
            currentBlockIndex = 0;
        }

        // All bytes added have been reported to blockBuilderStatus by child block builders. No report to blockBuilderStatus necessary here.
    }

    @Override
    public Block getRegion(int position, int length)
    {
        validateRange(position, length);
        return sliceRange(position, length, false);
    }

    @Override
    public InterleavedBlock build()
    {
        Block[] blocks = new Block[getBlockCount()];
        for (int i = 0; i < getBlockCount(); i++) {
            blocks[i] = blockBuilders[i].build();
        }
        return new InterleavedBlock(blocks);
    }

    @Override
    public void reset(BlockBuilderStatus blockBuilderStatus)
    {
        this.positionCount = 0;

        this.sizeInBytes = 0;
        this.retainedSizeInBytes = INSTANCE_SIZE;
        for (BlockBuilder blockBuilder : blockBuilders) {
            blockBuilder.reset(blockBuilderStatus);
            this.sizeInBytes += blockBuilder.getSizeInBytes();
            this.retainedSizeInBytes += blockBuilder.getRetainedSizeInBytes();
        }

        this.startSize = -1;
        this.startRetainedSize = -1;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("InterleavedBlock{");
        sb.append("columns=").append(getBlockCount());
        sb.append(", positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }
}
