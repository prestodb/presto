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

import com.facebook.presto.spi.type.VariableWidthType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;
import java.util.Objects;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public class VariableWidthBlockBuilder
        extends AbstractVariableWidthBlock
        implements BlockBuilder
{
    private final BlockBuilderStatus blockBuilderStatus;
    private final SliceOutput sliceOutput;

    private int positions;
    private int[] offsets = new int[1024];
    private boolean[] valueIsNull = new boolean[1024];

    public VariableWidthBlockBuilder(VariableWidthType type, BlockBuilderStatus blockBuilderStatus)
    {
        super(type);

        this.blockBuilderStatus = Objects.requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");
        this.sliceOutput = new DynamicSliceOutput((int) (blockBuilderStatus.getMaxBlockSizeInBytes() * 1.2));
    }

    @Override
    protected int getPositionOffset(int position)
    {
        if (position >= positions) {
            throw new IllegalArgumentException("position " + position + " must be less than position count " + positions);
        }
        return offsets[position];
    }

    @Override
    protected Slice getRawSlice()
    {
        return sliceOutput.getUnderlyingSlice();
    }

    @Override
    public int getPositionCount()
    {
        return positions;
    }

    @Override
    public boolean isEmpty()
    {
        return positions == 0;
    }

    @Override
    public boolean isFull()
    {
        return blockBuilderStatus.isFull();
    }

    @Override
    public int getSizeInBytes()
    {
        long size = getRawSlice().length() + SizeOf.sizeOf(offsets) + SizeOf.sizeOf(valueIsNull);
        if (size > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) size;
    }

    @Override
    public BlockBuilder appendBoolean(boolean value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder appendLong(long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder appendDouble(double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder appendSlice(Slice value)
    {
        return appendSlice(value, 0, value.length());
    }

    @Override
    public BlockBuilder appendSlice(Slice value, int offset, int length)
    {
        recordNewPosition(false);

        int bytesWritten = type.writeSlice(sliceOutput, value, offset, length);

        entryAdded(bytesWritten);

        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        recordNewPosition(true);
        entryAdded(0);

        return this;
    }

    private void entryAdded(int bytesWritten)
    {
        blockBuilderStatus.addBytes(SIZE_OF_BYTE + bytesWritten);
        if (sliceOutput.size() >= blockBuilderStatus.getMaxBlockSizeInBytes()) {
            blockBuilderStatus.setFull();
        }
    }

    private void recordNewPosition(boolean isNull)
    {
        if (positions == offsets.length) {
            offsets = Arrays.copyOf(offsets, offsets.length * 2);
            valueIsNull = Arrays.copyOf(valueIsNull, valueIsNull.length * 2);
        }

        offsets[positions] = sliceOutput.size();
        valueIsNull[positions] = isNull;

        positions++;
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

        int[] newOffsets = Arrays.copyOfRange(offsets, positionOffset, positionOffset + length);
        boolean[] newValueIsNull = Arrays.copyOfRange(valueIsNull, positionCount, positionOffset + length);
        return new VariableWidthBlock(type, length, sliceOutput.slice(), newOffsets, newValueIsNull);
    }

    @Override
    public Block build()
    {
        return new VariableWidthBlock(type, positions, sliceOutput.slice(), Arrays.copyOf(offsets, positions), Arrays.copyOf(valueIsNull, positions));
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlockBuilder{");
        sb.append("positionCount=").append(positions);
        sb.append(", size=").append(sliceOutput.size());
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }
}
