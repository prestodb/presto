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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.prestosql.spi.block.BlockUtil.calculateBlockResetBytes;
import static io.prestosql.spi.block.BlockUtil.calculateBlockResetSize;
import static io.prestosql.spi.block.BlockUtil.checkArrayRange;
import static io.prestosql.spi.block.BlockUtil.checkValidPosition;
import static io.prestosql.spi.block.BlockUtil.checkValidPositions;
import static io.prestosql.spi.block.BlockUtil.checkValidRegion;
import static io.prestosql.spi.block.BlockUtil.compactArray;
import static io.prestosql.spi.block.BlockUtil.compactOffsets;
import static io.prestosql.spi.block.BlockUtil.compactSlice;
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

    private boolean hasNullValue;
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
        checkValidPosition(position, positions);
        return getOffset(position);
    }

    @Override
    public int getSliceLength(int position)
    {
        checkValidPosition(position, positions);
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
        checkValidRegion(positionCount, positionOffset, length);
        long arraysSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) length;
        return getOffset(positionOffset + length) - getOffset(positionOffset) + arraysSizeInBytes;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        checkValidPositions(positions, getPositionCount());
        long sizeInBytes = 0;
        int usedPositionCount = 0;
        for (int i = 0; i < positions.length; ++i) {
            if (positions[i]) {
                usedPositionCount++;
                sizeInBytes += getOffset(i + 1) - getOffset(i);
            }
        }
        return sizeInBytes + (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount;
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
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int finalLength = 0;
        for (int i = offset; i < offset + length; i++) {
            finalLength += getSliceLength(positions[i]);
        }
        SliceOutput newSlice = Slices.allocate(finalLength).getOutput();
        int[] newOffsets = new int[length + 1];
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = new boolean[length];
        }

        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (isEntryNull(position)) {
                newValueIsNull[i] = true;
            }
            else {
                newSlice.writeBytes(sliceOutput.getUnderlyingSlice(), getPositionOffset(position), getSliceLength(position));
            }
            newOffsets[i + 1] = newSlice.size();
        }
        return new VariableWidthBlock(0, length, newSlice.slice(), newOffsets, newValueIsNull);
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

        hasNullValue = true;
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
    public boolean mayHaveNull()
    {
        return hasNullValue;
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
        checkValidRegion(positionCount, positionOffset, length);

        return new VariableWidthBlock(positionOffset, length, sliceOutput.slice(), offsets, hasNullValue ? valueIsNull : null);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, positionOffset, length);

        int[] newOffsets = compactOffsets(offsets, positionOffset, length);
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = compactArray(valueIsNull, positionOffset, length);
        }
        Slice slice = compactSlice(sliceOutput.getUnderlyingSlice(), offsets[positionOffset], newOffsets[length]);

        return new VariableWidthBlock(0, length, slice, newOffsets, newValueIsNull);
    }

    @Override
    public Block build()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return new VariableWidthBlock(0, positions, sliceOutput.slice(), offsets, hasNullValue ? valueIsNull : null);
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
