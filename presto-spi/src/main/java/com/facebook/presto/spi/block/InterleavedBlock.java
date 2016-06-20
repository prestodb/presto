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

import org.openjdk.jol.info.ClassLayout;

import java.util.concurrent.atomic.AtomicInteger;

public class InterleavedBlock
        extends AbstractInterleavedBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(InterleavedBlock.class).instanceSize();

    private final Block[] blocks;
    private final InterleavedBlockEncoding blockEncoding;
    private final int start;
    private final int positionCount;
    private final int retainedSizeInBytes;

    private final AtomicInteger sizeInBytes;

    public InterleavedBlock(Block[] blocks)
    {
        super(blocks.length);
        this.blocks = blocks;

        // Aside from calculating sizeInBytes, retainedSizeInBytes, and positionCount,
        // the loop below verifies that the position count of sub-blocks in the InterleavedBlock
        // * differs by at most one
        // * is non-ascending
        int sizeInBytes = 0;
        int retainedSizeInBytes = INSTANCE_SIZE;
        int positionCount = 0;
        if (blocks.length != 0) {
            int firstSubBlockPositionCount = blocks[0].getPositionCount();
            boolean subBlockHasDifferentSize = false;
            for (int i = 0; i < getBlockCount(); i++) {
                sizeInBytes += blocks[i].getSizeInBytes();
                retainedSizeInBytes += blocks[i].getRetainedSizeInBytes();
                positionCount += blocks[i].getPositionCount();

                if (subBlockHasDifferentSize) {
                    if (firstSubBlockPositionCount - 1 != blocks[i].getPositionCount()) {
                        throw new IllegalArgumentException(
                                "length of sub blocks differ by at least 2 or is not non-ascending: block 0: " + firstSubBlockPositionCount + ", block " + i + ": " + blocks[i].getPositionCount());
                    }
                }
                else {
                    if (firstSubBlockPositionCount != blocks[i].getPositionCount()) {
                        if (firstSubBlockPositionCount - 1 == blocks[i].getPositionCount()) {
                            subBlockHasDifferentSize = true;
                        }
                        else {
                            throw new IllegalArgumentException("length of sub blocks differ by at least 2: block 0: " + firstSubBlockPositionCount + ", block " + i + ": " + blocks[i].getPositionCount());
                        }
                    }
                }
            }
        }

        this.blockEncoding = computeBlockEncoding();
        this.start = 0;
        this.positionCount = positionCount;
        this.sizeInBytes = new AtomicInteger(sizeInBytes);
        this.retainedSizeInBytes = retainedSizeInBytes;
    }

    private InterleavedBlock(Block[] blocks, int start, int positionCount, int retainedSizeInBytes, InterleavedBlockEncoding blockEncoding)
    {
        super(blocks.length);
        this.blocks = blocks;
        this.start = start;
        this.positionCount = positionCount;
        this.retainedSizeInBytes = retainedSizeInBytes;
        this.blockEncoding = blockEncoding;
        this.sizeInBytes = new AtomicInteger(-1);
    }

    @Override
    public Block getRegion(int position, int length)
    {
        if (position < 0 || length < 0 || position + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position (" + position + "), length (" + length + ") in block with " + positionCount + " positions");
        }
        return new InterleavedBlock(blocks, computePosition(position), length, retainedSizeInBytes, blockEncoding);
    }

    @Override
    protected Block getBlock(int blockIndex)
    {
        if (blockIndex < 0) {
            throw new IllegalArgumentException("position is not valid");
        }

        return blocks[blockIndex];
    }

    @Override
    protected int computePosition(int position)
    {
        return position + start;
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
        int sizeInBytes = this.sizeInBytes.get();
        if (sizeInBytes < 0) {
            sizeInBytes = 0;
            for (int i = 0; i < getBlockCount(); i++) {
                sizeInBytes += blocks[i].getSizeInBytes();
            }
            this.sizeInBytes.set(sizeInBytes);
        }
        return sizeInBytes;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("InterleavedBlock{");
        sb.append("columns=").append(getBlockCount());
        sb.append(", positionCount=").append(getPositionCount() / getBlockCount());
        sb.append('}');
        return sb.toString();
    }
}
