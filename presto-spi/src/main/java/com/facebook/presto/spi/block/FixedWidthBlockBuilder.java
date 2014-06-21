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

public class FixedWidthBlockBuilder
        extends AbstractFixedWidthBlock
        implements BlockBuilder
{
    private final BlockBuilderStatus blockBuilderStatus;
    private final SliceOutput sliceOutput;
    private boolean[] valueIsNull;
    private int positionCount;

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

        Slice slice = Slices.allocate(entrySize * positionCount);

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
    public BlockBuilder appendBoolean(boolean value)
    {
        type.writeBoolean(sliceOutput, value);
        entryAdded(false);
        return this;
    }

    @Override
    public BlockBuilder appendLong(long value)
    {
        type.writeLong(sliceOutput, value);
        entryAdded(false);
        return this;
    }

    @Override
    public BlockBuilder appendDouble(double value)
    {
        type.writeDouble(sliceOutput, value);
        entryAdded(false);
        return this;
    }

    @Override
    public BlockBuilder appendSlice(Slice value)
    {
        return appendSlice(value, 0, value.length());
    }

    @Override
    public BlockBuilder appendSlice(Slice value, int offset, int length)
    {
        if (length != type.getFixedSize()) {
            throw new IllegalArgumentException("length must be " + type.getFixedSize() + " but is " + length);
        }

        type.writeSlice(sliceOutput, value, offset);

        entryAdded(false);

        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        // fixed width is always written regardless of null flag
        sliceOutput.writeZero(type.getFixedSize());

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
        blockBuilderStatus.addBytes(entrySize);
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

        Slice newSlice = sliceOutput.slice().slice(positionOffset * entrySize, length * entrySize);
        return new FixedWidthBlock(type, length, newSlice, valueIsNull);
    }

    @Override
    public Block build()
    {
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
