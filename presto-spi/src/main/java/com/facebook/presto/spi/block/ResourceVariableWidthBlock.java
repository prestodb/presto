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

import com.facebook.presto.spi.block.array.BooleanArray;
import com.facebook.presto.spi.block.array.IntArray;
import com.facebook.presto.spi.block.resource.BlockResourceContext;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.spi.block.BlockUtil.checkValidPositions;
import static com.facebook.presto.spi.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.spi.block.BlockUtil.intSaturatedCast;

public class ResourceVariableWidthBlock
        extends AbstractVariableWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ResourceVariableWidthBlock.class).instanceSize();

    private final int arrayOffset;
    private final int positionCount;
    private final Slice slice;
    private final IntArray offsets;
    private final BooleanArray valueIsNull;
    private final BlockResourceContext resourceContext;

    private final int retainedSizeInBytes;
    private final int sizeInBytes;

    public static ResourceVariableWidthBlock convert(VariableWidthBlock variableWidthBlock, BlockResourceContext resourceContext)
    {
        int positionCount = variableWidthBlock.positionCount;
        Slice slice = resourceContext.copyOf(variableWidthBlock.slice, variableWidthBlock.arrayOffset, positionCount);
        BooleanArray newValueIsNull = resourceContext.newBooleanArray(positionCount);
        IntArray newOffsets = resourceContext.newIntArray(positionCount);
        for (int p = 0; p < positionCount; p++) {
            newOffsets.set(p, variableWidthBlock.getPositionOffset(p));
            newValueIsNull.set(p, variableWidthBlock.isEntryNull(p));
        }
        return new ResourceVariableWidthBlock(variableWidthBlock.positionCount, slice, newOffsets, newValueIsNull, resourceContext);
    }

    public ResourceVariableWidthBlock(int positionCount, Slice slice, IntArray offsets, BooleanArray valueIsNull, BlockResourceContext resourceContext)
    {
        this(0, positionCount, slice, offsets, valueIsNull, resourceContext);
    }

    ResourceVariableWidthBlock(int arrayOffset, int positionCount, Slice slice, IntArray offsets, BooleanArray valueIsNull, BlockResourceContext resourceContext)
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

        if (offsets.length() - arrayOffset < (positionCount + 1)) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        this.offsets = offsets;

        if (valueIsNull.length() - arrayOffset < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        sizeInBytes = intSaturatedCast(slice.length() + ((Integer.BYTES + Byte.BYTES) * (long) positionCount));
        retainedSizeInBytes = intSaturatedCast(INSTANCE_SIZE + slice.getRetainedSize() + valueIsNull.sizeOf() + offsets.sizeOf());
        this.resourceContext = resourceContext;
    }

    @Override
    protected final int getPositionOffset(int position)
    {
        return offsets.get(position + arrayOffset);
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
        return valueIsNull.get(position + arrayOffset);
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
        IntArray newOffsets = resourceContext.newIntArray(positions.size() + 1);
        BooleanArray newValueIsNull = resourceContext.newBooleanArray(positions.size());

        for (int i = 0; i < positions.size(); i++) {
            int position = positions.get(i);
            if (isEntryNull(position)) {
                newValueIsNull.set(i, true);
            }
            else {
                newSlice.appendBytes(slice.getBytes(getPositionOffset(position), getLength(position)));
            }
            newOffsets.set(i + 1, newSlice.size());
        }
        return new ResourceVariableWidthBlock(positions.size(), newSlice.slice(), newOffsets, newValueIsNull, resourceContext);
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

        return new ResourceVariableWidthBlock(positionOffset + arrayOffset, length, slice, offsets, valueIsNull, resourceContext);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += arrayOffset;

        IntArray newOffsets = resourceContext.copyOfRangeIntArray(offsets, positionOffset, positionOffset + length + 1);
        // set new offsets to start from beginning of slice (since we are copying)
        for (int i = 0; i < newOffsets.length(); i++) {
            newOffsets.set(i, newOffsets.get(i) - offsets.get(positionOffset));
        }

        Slice newSlice = resourceContext.copyOfSlice(slice, offsets.get(positionOffset), newOffsets.get(length));
        BooleanArray newValueIsNull = resourceContext.copyOfRangeBooleanArray(valueIsNull, positionOffset, positionOffset + length);
        return new ResourceVariableWidthBlock(length, newSlice, newOffsets, newValueIsNull, resourceContext);
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
