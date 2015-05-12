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

import java.util.Objects;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

public class VariableWidthBlockBuilder
        extends AbstractVariableWidthBlock
        implements BlockBuilder
{
    private final BlockBuilderStatus blockBuilderStatus;
    private final SliceOutput sliceOutput;
    private final SliceOutput valueIsNull;
    private final SliceOutput offsets;

    private int positions;
    private int currentEntrySize;

    public VariableWidthBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        this(blockBuilderStatus, (int) (blockBuilderStatus.getMaxBlockSizeInBytes() * 1.2));
    }

    public VariableWidthBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedSizeInBytes)
    {
        this.blockBuilderStatus = Objects.requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");
        this.sliceOutput = new DynamicSliceOutput(expectedSizeInBytes);
        this.valueIsNull = new DynamicSliceOutput(1024);
        this.offsets = new DynamicSliceOutput(1024 * SIZE_OF_INT);

        offsets.appendInt(0);
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
    public int getLength(int position)
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
        long size = sliceOutput.getUnderlyingSlice().length() + offsets.getUnderlyingSlice().length() + valueIsNull.getUnderlyingSlice().length();
        if (size > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) size;
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
    public BlockBuilder writeFloat(float value)
    {
        sliceOutput.writeFloat(value);
        currentEntrySize += SIZE_OF_FLOAT;
        return this;
    }

    @Override
    public BlockBuilder writeDouble(double value)
    {
        sliceOutput.writeDouble(value);
        currentEntrySize += SIZE_OF_DOUBLE;
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
        positions++;

        valueIsNull.appendByte(isNull ? 1 : 0);
        offsets.appendInt(sliceOutput.size());

        blockBuilderStatus.addBytes(SIZE_OF_BYTE + SIZE_OF_INT + bytesWritten);
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull.getUnderlyingSlice().getByte(position) != 0;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        Slice newOffsets = offsets.getUnderlyingSlice().slice(positionOffset * SIZE_OF_INT, (length + 1) * SIZE_OF_INT);
        Slice newValueIsNull = valueIsNull.getUnderlyingSlice().slice(positionOffset, length);
        return new VariableWidthBlock(length, sliceOutput.slice(), newOffsets, newValueIsNull);
    }

    @Override
    public Block build()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return new VariableWidthBlock(positions, sliceOutput.slice(), offsets.slice(), valueIsNull.slice());
    }

    private int getOffset(int position)
    {
        return offsets.getUnderlyingSlice().getInt(position * SIZE_OF_INT);
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
