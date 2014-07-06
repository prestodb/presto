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

import com.facebook.presto.spi.type.FixedWidthType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.Arrays;

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
    private boolean[] valueIsNull;
    private int positionCount;

    private int currentEntrySize;

    public FixedWidthBlockBuilder(FixedWidthType type, BlockBuilderStatus blockBuilderStatus)
    {
        super(type);

        this.blockBuilderStatus = blockBuilderStatus;
        this.sliceOutput = new DynamicSliceOutput(blockBuilderStatus.getMaxBlockSizeInBytes());
        this.valueIsNull = new boolean[1024];
    }

    public FixedWidthBlockBuilder(FixedWidthType type, int positionCount)
    {
        super(type);

        Slice slice = Slices.allocate(fixedSize * positionCount);

        this.blockBuilderStatus = new BlockBuilderStatus(slice.length(), slice.length());
        this.sliceOutput = slice.getOutput();

        this.valueIsNull = new boolean[positionCount];
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
    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    @Override
    public boolean isFull()
    {
        return blockBuilderStatus.isFull();
    }

    @Override
    public int getSizeInBytes()
    {
        long size = getRawSlice().length() + SizeOf.sizeOf(valueIsNull);
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
        if (positionCount == valueIsNull.length - 1) {
            valueIsNull = Arrays.copyOf(valueIsNull, valueIsNull.length * 2);
        }
        valueIsNull[positionCount] = isNull;

        positionCount++;
        blockBuilderStatus.addBytes(fixedSize);
        if (sliceOutput.size() >= blockBuilderStatus.getMaxBlockSizeInBytes()) {
            blockBuilderStatus.setFull();
        }
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

        Slice newSlice = sliceOutput.slice().slice(positionOffset * fixedSize, length * fixedSize);
        boolean[] newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length);
        return new FixedWidthBlock(type, length, newSlice, newValueIsNull);
    }

    @Override
    public Block build()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return new FixedWidthBlock(type, positionCount, sliceOutput.slice(), valueIsNull);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("FixedWidthBlockBuilder{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", size=").append(sliceOutput.size());
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }
}
