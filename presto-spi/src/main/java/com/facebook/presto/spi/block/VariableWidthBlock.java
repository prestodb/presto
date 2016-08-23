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

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.spi.block.BlockUtil.checkValidPositions;
import static com.facebook.presto.spi.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.spi.block.BlockUtil.intSaturatedCast;
import static io.airlift.slice.SizeOf.sizeOf;

public class VariableWidthBlock
        extends AbstractVariableWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlock.class).instanceSize();

    private final int arrayOffset;
    private final int positionCount;
    private final Slice slice;
    private final int[] offsets;
    private final boolean[] valueIsNull;

    private final int retainedSizeInBytes;
    private final int sizeInBytes;

    public VariableWidthBlock(int positionCount, Slice slice, int[] offsets, boolean[] valueIsNull)
    {
        this(0, positionCount, slice, offsets, valueIsNull);
    }

    VariableWidthBlock(int arrayOffset, int positionCount, Slice slice, int[] offsets, boolean[] valueIsNull)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (slice == null) {
            throw new IllegalArgumentException("slice is null");
        }
        this.slice = slice;

        if (offsets.length - arrayOffset < (positionCount + 1)) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        this.offsets = offsets;

        if (valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        sizeInBytes = intSaturatedCast(offsets[arrayOffset + positionCount] - offsets[arrayOffset] + ((Integer.BYTES + Byte.BYTES) * (long) positionCount));
        retainedSizeInBytes = intSaturatedCast(INSTANCE_SIZE + slice.getRetainedSize() + sizeOf(valueIsNull) + sizeOf(offsets));
    }

    @Override
    protected final int getPositionOffset(int position)
    {
        return offsets[position + arrayOffset];
    }

    @Override
    public int getLength(int position)
    {
        checkReadablePosition(position);
        return getPositionOffset(position + 1) - getPositionOffset(position);
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull[position + arrayOffset];
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        checkValidPositions(positions, positionCount);

        int finalLength = positions.stream().mapToInt(this::getLength).sum();
        SliceOutput newSlice = Slices.allocate(finalLength).getOutput();
        int[] newOffsets = new int[positions.size() + 1];
        boolean[] newValueIsNull = new boolean[positions.size()];

        for (int i = 0; i < positions.size(); i++) {
            int position = positions.get(i);
            if (isEntryNull(position)) {
                newValueIsNull[i] = true;
            }
            else {
                newSlice.appendBytes(slice.getBytes(getPositionOffset(position), getLength(position)));
            }
            newOffsets[i + 1] = newSlice.size();
        }
        return new VariableWidthBlock(positions.size(), newSlice.slice(), newOffsets, newValueIsNull);
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        return slice;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new VariableWidthBlock(positionOffset + arrayOffset, length, slice, offsets, valueIsNull);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += arrayOffset;

        int[] newOffsets = Arrays.copyOfRange(offsets, positionOffset, positionOffset + length + 1);
        // set new offsets to start from beginning of slice (since we are copying)
        for (int i = 0; i < newOffsets.length; i++) {
            newOffsets[i] -= offsets[positionOffset];
        }

        Slice newSlice = Slices.copyOf(slice, offsets[positionOffset], newOffsets[length]);
        boolean[] newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length);
        return new VariableWidthBlock(length, newSlice, newOffsets, newValueIsNull);
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
