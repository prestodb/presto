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

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.spi.block.BlockUtil.calculateBlockResetSize;
import static com.facebook.presto.spi.block.BlockUtil.intSaturatedCast;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class VariableWidthBlockBuilder
        extends AbstractVariableWidthBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlockBuilder.class).instanceSize() + BlockBuilderStatus.INSTANCE_SIZE;

    private BlockBuilderStatus blockBuilderStatus;
    private SliceOutput sliceOutput;

    // it is assumed that the offsets array is one position longer than the valueIsNull array
    private boolean[] valueIsNull;
    private int[] offsets;

    private int positions;
    private int currentEntrySize;

    private long arraysRetainedSizeInBytes;

    public VariableWidthBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        this.blockBuilderStatus = requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");
        this.sliceOutput = new DynamicSliceOutput(expectedBytesPerEntry * expectedEntries);
        this.valueIsNull = new boolean[expectedEntries];
        this.offsets = new int[expectedEntries + 1];

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
    public int getSizeInBytes()
    {
        long arraysSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positions;
        return intSaturatedCast(sliceOutput.size() + arraysSizeInBytes);
    }

    @Override
    public int getRegionSizeInBytes(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " length " + length + " in block with " + positionCount + " positions");
        }
        long arraysSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) length;
        return intSaturatedCast(getOffset(positionOffset + length) - getOffset(positionOffset) + arraysSizeInBytes);
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return intSaturatedCast(INSTANCE_SIZE + sliceOutput.getRetainedSize() + arraysRetainedSizeInBytes);
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
        sliceOutput.writeByte(value);
        currentEntrySize += SIZE_OF_BYTE;
        return this;
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        sliceOutput.writeShort(value);
        currentEntrySize += SIZE_OF_SHORT;
        return this;
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        sliceOutput.writeInt(value);
        currentEntrySize += SIZE_OF_INT;
        return this;
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        sliceOutput.writeLong(value);
        currentEntrySize += SIZE_OF_LONG;
        return this;
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
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
        if (valueIsNull.length <= positions) {
            growCapacity();
        }

        valueIsNull[positions] = isNull;
        offsets[positions + 1] = sliceOutput.size();

        positions++;

        blockBuilderStatus.addBytes(SIZE_OF_BYTE + SIZE_OF_INT + bytesWritten);
    }

    private void growCapacity()
    {
        int newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateArraysDataSize();
    }

    private void updateArraysDataSize()
    {
        arraysRetainedSizeInBytes = intSaturatedCast(sizeOf(valueIsNull) + sizeOf(offsets));
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

        int[] newOffsets = Arrays.copyOfRange(offsets, positionOffset, positionOffset + length + 1);
        boolean[] newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length);
        return new VariableWidthBlock(length, sliceOutput.slice(), newOffsets, newValueIsNull);
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
    public void reset(BlockBuilderStatus blockBuilderStatus)
    {
        this.blockBuilderStatus = requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");

        int newSize = calculateBlockResetSize(positions);
        valueIsNull = new boolean[newSize];
        offsets = new int[newSize + 1];
        sliceOutput = new DynamicSliceOutput(calculateBlockResetSize(sliceOutput.size()));

        positions = 0;
        currentEntrySize = 0;

        updateArraysDataSize();
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        int expectedBytesPerEntry = positions == 0 ? positions : (getOffset(positions) - getOffset(0)) / positions;
        return new VariableWidthBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positions), expectedBytesPerEntry);
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
