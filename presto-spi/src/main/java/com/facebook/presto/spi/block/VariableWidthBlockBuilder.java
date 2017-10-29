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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static com.facebook.presto.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static com.facebook.presto.spi.block.BlockUtil.calculateBlockResetBytes;
import static com.facebook.presto.spi.block.BlockUtil.calculateBlockResetSize;
import static com.facebook.presto.spi.block.BlockUtil.compactArray;
import static com.facebook.presto.spi.block.BlockUtil.compactOffsets;
import static com.facebook.presto.spi.block.BlockUtil.compactSlice;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.min;

public class VariableWidthBlockBuilder
        extends AbstractVariableWidthBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlockBuilder.class).instanceSize();

    private BlockBuilderStatus blockBuilderStatus;

    private boolean initialized;
    private int initialEntryCount;
    private int initialSliceOutputSize;

    private SliceOutput sliceOutput = new DynamicSliceOutput(0);

    // it is assumed that the offsets array is one position longer than the valueIsNull array
    private boolean[] valueIsNull = new boolean[0];
    private int[] offsets = new int[1];

    private int positions;
    private int currentEntrySize;

    private long arraysRetainedSizeInBytes;

    public VariableWidthBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        this.blockBuilderStatus = blockBuilderStatus;

        initialEntryCount = expectedEntries;
        initialSliceOutputSize = min(expectedBytes, MAX_ARRAY_SIZE);

        updateArraysDataSize();
    }

    @Override
    protected int getPositionOffset(int position)
    {
        if (position >= positions) {
            throw new IllegalArgumentException("position " + position + " must be less than position count " + positions);
        }
        return getOffset(position);
    }

    @Override
    public int getSliceLength(int position)
    {
        if (position >= positions) {
            throw new IllegalArgumentException("position " + position + " must be less than position count " + positions);
        }
        return getOffset((position + 1)) - getOffset(position);
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        return sliceOutput.getUnderlyingSlice();
    }

    @Override
    public int getPositionCount()
    {
        return positions;
    }

    @Override
    public long getSizeInBytes()
    {
        long arraysSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positions;
        return sliceOutput.size() + arraysSizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " length " + length + " in block with " + positionCount + " positions");
        }
        long arraysSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) length;
        return getOffset(positionOffset + length) - getOffset(positionOffset) + arraysSizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sliceOutput.getRetainedSize() + arraysRetainedSizeInBytes;
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(sliceOutput, sliceOutput.getRetainedSize());
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        int finalLength = positions.stream().mapToInt(this::getSliceLength).sum();
        SliceOutput newSlice = Slices.allocate(finalLength).getOutput();
        int[] newOffsets = new int[positions.size() + 1];
        boolean[] newValueIsNull = new boolean[positions.size()];

        for (int i = 0; i < positions.size(); i++) {
            int position = positions.get(i);
            if (isEntryNull(position)) {
                newValueIsNull[i] = true;
            }
            else {
                newSlice.appendBytes(sliceOutput.getUnderlyingSlice().getBytes(getPositionOffset(position), getSliceLength(position)));
            }
            newOffsets[i + 1] = newSlice.size();
        }
        return new VariableWidthBlock(positions.size(), newSlice.slice(), newOffsets, newValueIsNull);
    }

    @Override
    public BlockBuilder writeByte(int value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeByte(value);
        currentEntrySize += SIZE_OF_BYTE;
        return this;
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeShort(value);
        currentEntrySize += SIZE_OF_SHORT;
        return this;
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeInt(value);
        currentEntrySize += SIZE_OF_INT;
        return this;
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeLong(value);
        currentEntrySize += SIZE_OF_LONG;
        return this;
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeBytes(source, sourceIndex, length);
        currentEntrySize += length;
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        entryAdded(currentEntrySize, false);
        currentEntrySize = 0;
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        entryAdded(0, true);
        return this;
    }

    private void entryAdded(int bytesWritten, boolean isNull)
    {
        if (!initialized) {
            initializeCapacity();
        }
        if (valueIsNull.length <= positions) {
            growCapacity();
        }

        valueIsNull[positions] = isNull;
        offsets[positions + 1] = sliceOutput.size();

        positions++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(SIZE_OF_BYTE + SIZE_OF_INT + bytesWritten);
        }
    }

    private void growCapacity()
    {
        int newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateArraysDataSize();
    }

    private void initializeCapacity()
    {
        if (positions != 0 || currentEntrySize != 0) {
            throw new IllegalStateException(getClass().getSimpleName() + " was used before initialization");
        }
        initialized = true;
        valueIsNull = new boolean[initialEntryCount];
        offsets = new int[initialEntryCount + 1];
        sliceOutput = new DynamicSliceOutput(initialSliceOutputSize);
        updateArraysDataSize();
    }

    private void updateArraysDataSize()
    {
        arraysRetainedSizeInBytes = sizeOf(valueIsNull) + sizeOf(offsets);
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull[position];
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        return new VariableWidthBlock(positionOffset, length, sliceOutput.slice(), offsets, valueIsNull);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        int[] newOffsets = compactOffsets(offsets, positionOffset, length);
        boolean[] newValueIsNull = compactArray(valueIsNull, positionOffset, length);
        Slice slice = compactSlice(sliceOutput.getUnderlyingSlice(), offsets[positionOffset], newOffsets[length]);

        return new VariableWidthBlock(length, slice, newOffsets, newValueIsNull);
    }

    @Override
    public Block build()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return new VariableWidthBlock(positions, sliceOutput.slice(), offsets, valueIsNull);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        int currentSizeInBytes = positions == 0 ? positions : (getOffset(positions) - getOffset(0));
        return new VariableWidthBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positions), calculateBlockResetBytes(currentSizeInBytes));
    }

    private int getOffset(int position)
    {
        return offsets[position];
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlockBuilder{");
        sb.append("positionCount=").append(positions);
        sb.append(", size=").append(sliceOutput.size());
        sb.append('}');
        return sb.toString();
    }
}
