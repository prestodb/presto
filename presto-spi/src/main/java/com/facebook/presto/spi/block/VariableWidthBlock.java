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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.spi.block.BlockValidationUtil.checkValidPositions;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class VariableWidthBlock
        extends AbstractVariableWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlock.class).instanceSize();

    private final int positionCount;
    private final Slice slice;
    private final Slice offsets;
    private final Slice valueIsNull;

    public VariableWidthBlock(int positionCount, Slice slice, Slice offsets, Slice valueIsNull)
    {
        this.positionCount = positionCount;
        this.slice = slice;

        if (offsets.length() < (positionCount + 1) * SIZE_OF_INT) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        this.offsets = offsets;

        if (valueIsNull.length() < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;
    }

    @Override
    protected final int getPositionOffset(int position)
    {
        return offsets.getInt(position * SIZE_OF_INT);
    }

    @Override
    public int getLength(int position)
    {
        return getPositionOffset(position + 1) - getPositionOffset(position);
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull.getByte(position) != 0;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        long size = slice.length() + offsets.length() + valueIsNull.length();
        if (size > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) size;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + slice.getRetainedSize() + offsets.getRetainedSize() + valueIsNull.getRetainedSize();
        if (size > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) size;
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        checkValidPositions(positions, positionCount);

        int finalLength = positions.stream().mapToInt(this::getLength).sum();
        SliceOutput newSlice = Slices.allocate(finalLength).getOutput();
        SliceOutput newOffsets = Slices.allocate((positions.size() * SIZE_OF_INT) + SIZE_OF_INT).getOutput();
        SliceOutput newValueIsNull = Slices.allocate(positions.size()).getOutput();

        newOffsets.appendInt(0);
        for (int position : positions) {
            boolean isNull = isEntryNull(position);
            newValueIsNull.appendByte(isNull ? 1 : 0);
            if (!isNull) {
                newSlice.appendBytes(slice.getBytes(getPositionOffset(position), getLength(position)));
            }
            newOffsets.appendInt(newSlice.size());
        }
        return new VariableWidthBlock(positions.size(), newSlice.slice(), newOffsets.slice(), newValueIsNull.slice());
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        return slice;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        Slice newOffsets = offsets.slice(positionOffset * SIZE_OF_INT, (length + 1) * SIZE_OF_INT);
        Slice newValueIsNull = valueIsNull.slice(positionOffset, length);
        return new VariableWidthBlock(length, slice, newOffsets, newValueIsNull);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        SliceOutput newOffsets = Slices.allocate((length * SIZE_OF_INT) + SIZE_OF_INT).getOutput();
        newOffsets.appendInt(0);
        int sliceLength = 0;
        for (int position = positionOffset; position < positionOffset + length; position++) {
            sliceLength += getLength(position);
            newOffsets.appendInt(sliceLength);
        }

        Slice newSlice = Slices.copyOf(slice, getPositionOffset(positionOffset), sliceLength);
        Slice newValueIsNull = Slices.copyOf(valueIsNull, positionOffset, length);
        return new VariableWidthBlock(length, newSlice, newOffsets.slice(), newValueIsNull);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append(", slice=").append(slice);
        sb.append('}');
        return sb.toString();
    }
}
