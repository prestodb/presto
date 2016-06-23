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

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.spi.block.BlockUtil.checkValidPositions;
import static com.facebook.presto.spi.block.BlockUtil.intSaturatedCast;
import static io.airlift.slice.Slices.wrappedBooleanArray;
import static java.util.Objects.requireNonNull;

public class LazyFixedWidthBlock
        extends AbstractFixedWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LazyFixedWidthBlock.class).instanceSize();

    private final int positionCount;
    private LazyBlockLoader<LazyFixedWidthBlock> loader;
    private Slice slice;
    private boolean[] valueIsNull;

    public LazyFixedWidthBlock(int fixedSize, int positionCount, LazyBlockLoader<LazyFixedWidthBlock> loader)
    {
        super(fixedSize);

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        this.loader = requireNonNull(loader);
    }

    LazyFixedWidthBlock(int fixedSize, int positionCount, LazyBlockLoader<LazyFixedWidthBlock> loader, Slice slice, boolean[] valueIsNull)
    {
        super(fixedSize);
        this.positionCount = positionCount;
        this.loader = loader;
        this.slice = slice;
        this.valueIsNull = valueIsNull;
    }

    @Override
    protected Slice getRawSlice()
    {
        assureLoaded();
        return slice;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        assureLoaded();
        return valueIsNull[position];
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        return intSaturatedCast((positionCount * fixedSize) + SizeOf.sizeOf(valueIsNull));
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        // TODO: This should account for memory used by the loader.
        long size = INSTANCE_SIZE + SizeOf.sizeOf(valueIsNull);
        if (slice != null) {
            size += slice.getRetainedSize();
        }
        return intSaturatedCast(size);
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        checkValidPositions(positions, positionCount);

        assureLoaded();
        SliceOutput newSlice = Slices.allocate(positions.size() * fixedSize).getOutput();
        SliceOutput newValueIsNull = Slices.allocate(positions.size()).getOutput();

        for (int position : positions) {
            newValueIsNull.appendByte(valueIsNull[position] ? 1 : 0);
            newSlice.appendBytes(getRawSlice().getBytes(position * fixedSize, fixedSize));
        }
        return new FixedWidthBlock(fixedSize, positions.size(), newSlice.slice(), newValueIsNull.slice());
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        assureLoaded();
        Slice newSlice = slice.slice(positionOffset * fixedSize, length * fixedSize);
        boolean[] newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length);
        return new FixedWidthBlock(fixedSize, length, newSlice, wrappedBooleanArray(newValueIsNull));
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        assureLoaded();
        Slice newSlice = Slices.copyOf(slice, positionOffset * fixedSize, length * fixedSize);
        boolean[] newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length);
        return new FixedWidthBlock(fixedSize, length, newSlice, wrappedBooleanArray(newValueIsNull));
    }

    @Override
    public void assureLoaded()
    {
        if (slice != null) {
            return;
        }
        loader.load(this);

        if (slice == null) {
            throw new IllegalArgumentException("Lazy block loader did not load this block");
        }

        // clear reference to loader to free resources, since load was successful
        loader = null;
    }

    public void setRawSlice(Slice slice)
    {
        if (slice.length() < positionCount * fixedSize) {
            throw new IllegalArgumentException("slice is not large enough to hold all positions");
        }
        this.slice = slice;
    }

    public void setNullVector(boolean[] valueIsNull)
    {
        if (valueIsNull.length < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("FixedWidthBlock{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", slice=").append(slice == null ? "not loaded" : slice);
        sb.append('}');
        return sb.toString();
    }
}
