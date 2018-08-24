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

import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;
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

    private volatile long sizeInBytes;
    private final long retainedSizeInBytes;

    /**
     * Create an array block directly from columnar nulls, values, and offsets into the values.
     * A null array must have no entries.
     */
    public static Block fromElementBlock(int positionCount, boolean[] valueIsNull, int[] arrayOffset, Block values)
    {
        validateConstructorArguments(0, positionCount, valueIsNull, arrayOffset, values);
        // for performance reasons per element checks are only performed on the public construction
        for (int i = 0; i < positionCount; i++) {
            int offset = arrayOffset[i];
            int length = arrayOffset[i + 1] - offset;
            if (length < 0) {
                throw new IllegalArgumentException(format("Offset is not monotonically ascending. offsets[%s]=%s, offsets[%s]=%s", i, arrayOffset[i], i + 1, arrayOffset[i + 1]));
            }
            if (valueIsNull[i] && length != 0) {
                throw new IllegalArgumentException("A null array must have zero entries");
            }
        }
        return new ArrayBlock(0, positionCount, valueIsNull, arrayOffset, values);
    }

    /**
     * Create an array block directly without per element validations.
     */
    static ArrayBlock createArrayBlockInternal(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets, Block values)
    {
        validateConstructorArguments(arrayOffset, positionCount, valueIsNull, offsets, values);
        return new ArrayBlock(arrayOffset, positionCount, valueIsNull, offsets, values);
    }

    private static void validateConstructorArguments(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets, Block values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        requireNonNull(valueIsNull, "valueIsNull is null");
        if (valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }

        requireNonNull(offsets, "offsets is null");
        if (offsets.length - arrayOffset < positionCount + 1) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }

        requireNonNull(values, "values is null");
    }

    /**
     * Use createArrayBlockInternal or fromElementBlock instead of this method.  The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private ArrayBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets, Block values)
    {
        // caller must check arguments with validateConstructorArguments
        this.arrayOffset = arrayOffset;
        this.positionCount = positionCount;
        this.valueIsNull = valueIsNull;
        this.offsets = offsets;
        this.values = requireNonNull(values);

        sizeInBytes = -1;
        retainedSizeInBytes = INSTANCE_SIZE + values.getRetainedSizeInBytes() + sizeOf(offsets) + sizeOf(valueIsNull);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes < 0) {
            calculateSize();
        }
        return sizeInBytes;
    }

    private void calculateSize()
    {
        int valueStart = offsets[arrayOffset];
        int valueEnd = offsets[arrayOffset + positionCount];
        sizeInBytes = values.getRegionSizeInBytes(valueStart, valueEnd - valueStart) + ((Integer.BYTES + Byte.BYTES) * (long) this.positionCount);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(values, values.getRetainedSizeInBytes());
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    protected Block getRawElementBlock()
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

    @Override
    public Block getLoadedBlock()
    {
        Block loadedValuesBlock = values.getLoadedBlock();

        if (loadedValuesBlock == values) {
            return this;
        }
        return createArrayBlockInternal(
                arrayOffset,
                positionCount,
                valueIsNull,
                offsets,
                loadedValuesBlock);
    }
}
