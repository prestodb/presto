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

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.BiConsumer;

import static com.facebook.presto.spi.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.spi.block.BlockUtil.checkValidPosition;
import static com.facebook.presto.spi.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.spi.block.BlockUtil.compactSlice;
import static java.util.Objects.requireNonNull;

public class FixedWidthBlock
        extends AbstractFixedWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedWidthBlock.class).instanceSize();

    private final int positionCount;
    private final Slice slice;
    @Nullable
    private final Slice valueIsNull;

    public FixedWidthBlock(int fixedSize, int positionCount, Slice slice, Optional<Slice> valueIsNull)
    {
        this(fixedSize, positionCount, slice, valueIsNull.orElse(null));
    }

    FixedWidthBlock(int fixedSize, int positionCount, Slice slice, Slice valueIsNull)
    {
        super(fixedSize);

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        this.slice = requireNonNull(slice, "slice is null");
        if (slice.length() < fixedSize * positionCount) {
            throw new IllegalArgumentException("slice length is less n positionCount * fixedSize");
        }

        if (valueIsNull != null && valueIsNull.length() < positionCount) {
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
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull != null && valueIsNull.getByte(position) != 0;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return (fixedSize + Byte.BYTES) * (long) positionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getRawSlice().getRetainedSize() + (valueIsNull == null ? 0 : valueIsNull.getRetainedSize());
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(slice, slice.getRetainedSize());
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, valueIsNull.getRetainedSize());
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        SliceOutput newSlice = Slices.allocate(length * fixedSize).getOutput();
        SliceOutput newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = Slices.allocate(length).getOutput();
        }

        for (int i = offset; i < offset + length; ++i) {
            int position = positions[i];
            checkValidPosition(position, positionCount);
            newSlice.writeBytes(slice, position * fixedSize, fixedSize);
            if (valueIsNull != null) {
                newValueIsNull.writeByte(valueIsNull.getByte(position));
            }
        }
        return new FixedWidthBlock(fixedSize, length, newSlice.slice(), newValueIsNull == null ? null : newValueIsNull.slice());
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        Slice newSlice = slice.slice(positionOffset * fixedSize, length * fixedSize);
        Slice newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = valueIsNull.slice(positionOffset, length);
        }
        return new FixedWidthBlock(fixedSize, length, newSlice, newValueIsNull);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        Slice newSlice = compactSlice(slice, positionOffset * fixedSize, length * fixedSize);
        Slice newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = compactSlice(valueIsNull, positionOffset, length);
        }

        if (newSlice == slice && newValueIsNull == valueIsNull) {
            return this;
        }
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
