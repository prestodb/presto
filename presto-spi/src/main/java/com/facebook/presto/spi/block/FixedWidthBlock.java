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
import java.util.Objects;

import static com.facebook.presto.spi.block.BlockValidationUtil.checkValidPositions;

public class FixedWidthBlock
        extends AbstractFixedWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedWidthBlock.class).instanceSize();

    private final int positionCount;
    private final Slice slice;
    private final Slice valueIsNull;

    public FixedWidthBlock(int fixedSize, int positionCount, Slice slice, Slice valueIsNull)
    {
        super(fixedSize);

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        this.slice = Objects.requireNonNull(slice, "slice is null");
        if (slice.length() < fixedSize * positionCount) {
            throw new IllegalArgumentException("slice length is less n positionCount * fixedSize");
        }

        if (valueIsNull.length() < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;
    }

    @Override
    protected Slice getRawSlice()
    {
        return slice;
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
        long size = getRawSlice().length() + valueIsNull.length();
        if (size > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) size;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + getRawSlice().getRetainedSize() + valueIsNull.getRetainedSize();
        if (size > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) size;
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        checkValidPositions(positions, positionCount);

        SliceOutput newSlice = Slices.allocate(positions.size() * fixedSize).getOutput();
        SliceOutput newValueIsNull = Slices.allocate(positions.size()).getOutput();

        for (int position : positions) {
            newSlice.writeBytes(slice, position * fixedSize, fixedSize);
            newValueIsNull.writeByte(valueIsNull.getByte(position));
        }
        return new FixedWidthBlock(fixedSize, positions.size(), newSlice.slice(), newValueIsNull.slice());
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        Slice newSlice = slice.slice(positionOffset * fixedSize, length * fixedSize);
        Slice newValueIsNull = valueIsNull.slice(positionOffset, length);
        return new FixedWidthBlock(fixedSize, length, newSlice, newValueIsNull);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        Slice newSlice = Slices.copyOf(slice, positionOffset * fixedSize, length * fixedSize);
        Slice newValueIsNull = Slices.copyOf(valueIsNull, positionOffset, length);
        return new FixedWidthBlock(fixedSize, length, newSlice, newValueIsNull);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("FixedWidthBlock{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", fixedSize=").append(fixedSize);
        sb.append(", slice=").append(slice);
        sb.append('}');
        return sb.toString();
    }
}
