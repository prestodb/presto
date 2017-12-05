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
package com.facebook.presto.connector.thrift.util;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockEncoding;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.primitives.Ints.max;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.min;
import static java.util.Arrays.asList;
import static java.util.Arrays.binarySearch;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.internal.guava.base.Preconditions.checkArgument;

public class ConcatBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ConcatBlock.class).instanceSize();
    Block[] blocks;
    int[] positions;
    int positionCount;
    long sizeInBytes;
    long retainedSize;

    public ConcatBlock(List<Block> blocks)
    {
        this(blocks.stream().toArray(Block[]::new));
    }

    public ConcatBlock(Block[] blocks)
    {
        checkArgument(blocks.length > 0, "blocks cannot be empty");
        requireNonNull(blocks, "blocks is null");
        this.blocks = blocks;
        int[] positions = new int[blocks.length + 1];
        positions[0] = 0;
        long retainedSize = 0;
        for (int i = 1; i <= blocks.length; i++) {
            positions[i] = positions[i - 1] + blocks[i - 1].getPositionCount();
            sizeInBytes += blocks[i - 1].getSizeInBytes();
            retainedSize += blocks[i - 1].getRetainedSizeInBytes();
        }
        this.positions = positions;
        this.positionCount = positions[blocks.length];
        this.retainedSize = INSTANCE_SIZE + retainedSize + sizeOf(positions);
    }

    private int getBlockIndex(int position)
    {
        if (position < 0 || position >= positionCount) {
            throw new IllegalArgumentException("Invalid position " + position + " in block with " + positionCount + " positions");
        }
        int p = binarySearch(positions, position);
        return p >= 0 ? p : (-p - 2);
    }

    private Block getBlock(int position)
    {
        return blocks[getBlockIndex(position)];
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        throw new PrestoException(NOT_SUPPORTED, "ConcatBlock does not support write");
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        int blockIdx = getBlockIndex(position);
        int positionInBlock = position - positions[blockIdx];
        return blocks[blockIdx].getSingleValueBlock((positionInBlock));
    }

    @Override
    public int getSliceLength(int position)
    {
        int blockIdx = getBlockIndex(position);
        int positionInBlock = position - positions[blockIdx];
        return blocks[blockIdx].getSliceLength(positionInBlock);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        int blockIdx = getBlockIndex(position);
        int positionInBlock = position - positions[blockIdx];
        return blocks[blockIdx].getByte(positionInBlock, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        int blockIdx = getBlockIndex(position);
        int positionInBlock = position - positions[blockIdx];
        return blocks[blockIdx].getShort(positionInBlock, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        int blockIdx = getBlockIndex(position);
        int positionInBlock = position - positions[blockIdx];
        return blocks[blockIdx].getInt(positionInBlock, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        int blockIdx = getBlockIndex(position);
        int positionInBlock = position - positions[blockIdx];
        return blocks[blockIdx].getLong(positionInBlock, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        int blockIdx = getBlockIndex(position);
        int positionInBlock = position - positions[blockIdx];
        return blocks[blockIdx].getSlice(positionInBlock, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        int blockIdx = getBlockIndex(position);
        int positionInBlock = position - positions[blockIdx];
        return blocks[blockIdx].getObject(positionInBlock, clazz);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return getRegion(position, length).getSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSize;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        for (Block block : blocks) {
            consumer.accept(block, block.getRetainedSizeInBytes());
        }
        consumer.accept(positions, (long) positions.length);
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        int idx = getBlockIndex(position);
        return blocks[idx].bytesEqual(position - positions[idx], offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        int idx = getBlockIndex(position);
        return blocks[idx].bytesCompare(position - positions[idx], offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        int idx = getBlockIndex(position);
        return blocks[idx].equals(position - positions[idx], offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        int idx = getBlockIndex(leftPosition);
        return blocks[idx].compareTo(leftPosition - positions[idx], leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        int idx = getBlockIndex(position);
        blocks[idx].writeBytesTo(position - positions[idx], offset, length, blockBuilder);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new ConcatBlockEncoding(blocks[0].getEncoding());
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        Map<Integer, List<Integer>> masks = positions.stream()
                .collect(Collectors.groupingBy(i -> getBlockIndex(i)));
        Block[] result = new Block[masks.size()];
        int blockId = 0;
        for (Map.Entry<Integer, List<Integer>> entry : masks.entrySet()) {
            int idx = entry.getKey();
            int[] mask = entry.getValue().stream().mapToInt(i -> i - this.positions[idx]).toArray();
            result[blockId++] = (blocks[idx].getPositions(mask));
        }
        return new ConcatBlock(result);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        int startBlockIdx = getBlockIndex(positionOffset);
        int endBlockIdx = getBlockIndex(positionOffset + length - 1);
        Block[] result = new Block[endBlockIdx - startBlockIdx + 1];
        int blockId = 0;
        for (int i = startBlockIdx; i < positions.length - 1; i++) {
            int start = max(positions[i], positionOffset);
            int end = min(positions[i + 1], positionOffset + length);
            if (start < end) {
                result[blockId++] = (blocks[i].getRegion(start - positions[i], end - start));
            }
            if (end == positionOffset + length) {
                break;
            }
        }
        return new ConcatBlock(result);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        return getRegion(position, length);
    }

    @Override
    public boolean isNull(int position)
    {
        int blockIdx = getBlockIndex(position);
        int positionInBlock = position - positions[blockIdx];
        return blocks[blockIdx].isNull(positionInBlock);
    }

    public List<Block> getBlocks()
    {
        return asList(blocks);
    }
}
