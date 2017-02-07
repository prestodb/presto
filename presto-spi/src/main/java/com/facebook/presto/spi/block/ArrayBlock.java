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

import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.spi.block.BlockUtil.intSaturatedCast;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class ArrayBlock
        extends AbstractArrayBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayBlock.class).instanceSize();

    private final int arrayOffset;
    private final int positionCount;
    private final boolean[] valueIsNull;
    private final Block values;
    private final int[] offsets;

    private int sizeInBytes;
    private final int retainedSizeInBytes;

    public ArrayBlock(int positionCount, boolean[] valueIsNull, int[] offsets, Block values)
    {
        this(0, positionCount, valueIsNull, offsets, values);
    }

    ArrayBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets, Block values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        requireNonNull(valueIsNull, "valueIsNull is null");
        if (valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        requireNonNull(offsets, "offsets is null");
        if (offsets.length - arrayOffset < positionCount + 1) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        this.offsets = offsets;

        this.values = requireNonNull(values);

        sizeInBytes = -1;
        retainedSizeInBytes = intSaturatedCast(INSTANCE_SIZE + values.getRetainedSizeInBytes() + sizeOf(offsets) + sizeOf(valueIsNull));
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        // this is racy but is safe because sizeInBytes is an int and the calculation is stable
        if (sizeInBytes < 0) {
            calculateSize();
        }
        return sizeInBytes;
    }

    private void calculateSize()
    {
        int valueStart = offsets[arrayOffset];
        int valueEnd = offsets[arrayOffset + positionCount];
        sizeInBytes = values.getRegion(valueStart, valueEnd - valueStart).getSizeInBytes() + ((Integer.BYTES + Byte.BYTES) * this.positionCount);
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    protected Block getValues()
    {
        return values;
    }

    @Override
    protected int[] getOffsets()
    {
        return offsets;
    }

    @Override
    protected int getOffsetBase()
    {
        return arrayOffset;
    }

    @Override
    protected boolean[] getValueIsNull()
    {
        return valueIsNull;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }
}
