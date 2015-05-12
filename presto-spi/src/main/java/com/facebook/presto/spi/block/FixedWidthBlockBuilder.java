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

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

public class FixedWidthBlockBuilder
        extends AbstractFixedWidthBlock
        implements BlockBuilder
{
    private final BlockBuilderStatus blockBuilderStatus;
    private final SliceOutput sliceOutput;
    private final SliceOutput valueIsNull;
    private int positionCount;

    private int currentEntrySize;

    public FixedWidthBlockBuilder(int fixedSize, BlockBuilderStatus blockBuilderStatus)
    {
        this(fixedSize, blockBuilderStatus, blockBuilderStatus.getMaxBlockSizeInBytes());
    }

    public FixedWidthBlockBuilder(int fixedSize, BlockBuilderStatus blockBuilderStatus, int expectedSize)
    {
        super(fixedSize);

        this.blockBuilderStatus = blockBuilderStatus;
        this.sliceOutput = new DynamicSliceOutput(expectedSize);
        this.valueIsNull = new DynamicSliceOutput(1024);
    }

    public FixedWidthBlockBuilder(int fixedSize, int positionCount)
    {
        super(fixedSize);

        Slice slice = Slices.allocate(fixedSize * positionCount);

        this.blockBuilderStatus = new BlockBuilderStatus();
        this.sliceOutput = slice.getOutput();

        this.valueIsNull = Slices.allocate(positionCount).getOutput();
    }

    @Override
    protected Slice getRawSlice()
    {
        return sliceOutput.getUnderlyingSlice();
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        long size = getRawSlice().length() + valueIsNull.getUnderlyingSlice().length();
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
        if (currentEntrySize != fixedSize) {
            throw new IllegalStateException("Expected entry size to be exactly " + fixedSize + " but was " + currentEntrySize);
        }

        entryAdded(false);
        currentEntrySize = 0;
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        // fixed width is always written regardless of null flag
        sliceOutput.writeZero(fixedSize);

        entryAdded(true);

        return this;
    }

    private void entryAdded(boolean isNull)
    {
        valueIsNull.appendByte(isNull ? 1 : 0);

        positionCount++;
        blockBuilderStatus.addBytes(Byte.BYTES + fixedSize);
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

        Slice newSlice = sliceOutput.slice().slice(positionOffset * fixedSize, length * fixedSize);
        Slice newValueIsNull = valueIsNull.slice().slice(positionOffset, length);
        return new FixedWidthBlock(fixedSize, length, newSlice, newValueIsNull);
    }

    @Override
    public Block build()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return new FixedWidthBlock(fixedSize, positionCount, sliceOutput.slice(), valueIsNull.slice());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("FixedWidthBlockBuilder{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", fixedSize=").append(fixedSize);
        sb.append(", size=").append(sliceOutput.size());
        sb.append('}');
        return sb.toString();
    }
}
