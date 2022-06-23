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
package com.facebook.presto.common.block;

import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.common.block.BlockUtil.appendNullToIsNullArray;
import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public class LongArrayBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongArrayBlock.class).instanceSize();
    public static final int SIZE_IN_BYTES_PER_POSITION = Long.BYTES + Byte.BYTES;

    private final int arrayOffset;
    private final int positionCount;
    @Nullable
    private final boolean[] valueIsNull;
    private final long[] values;

    private final long retainedSizeInBytes;

    public LongArrayBlock(int positionCount, Optional<boolean[]> valueIsNull, long[] values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    LongArrayBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    }

    @Override
    public long getSizeInBytes()
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.of(SIZE_IN_BYTES_PER_POSITION);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] usedPositions, int usedPositionCount)
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) usedPositionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : Long.BYTES;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(values, sizeOf(values));
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, sizeOf(valueIsNull));
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return getLongUnchecked(position + arrayOffset);
    }

    @Override
    @Deprecated
    // TODO: Remove when we fix intermediate types on aggregations.
    public int getInt(int position)
    {
        checkReadablePosition(position);
        return toIntExact(values[position + arrayOffset]);
    }

    @Override
    @Deprecated
    // TODO: Remove when we fix intermediate types on aggregations.
    public short getShort(int position)
    {
        checkReadablePosition(position);

        short value = (short) (values[position + arrayOffset]);
        if (value != values[position + arrayOffset]) {
            throw new ArithmeticException("short overflow");
        }
        return value;
    }

    @Override
    @Deprecated
    // TODO: Remove when we fix intermediate types on aggregations.
    public byte getByte(int position)
    {
        checkReadablePosition(position);

        byte value = (byte) (values[position + arrayOffset]);
        if (value != values[position + arrayOffset]) {
            throw new ArithmeticException("byte overflow");
        }
        return value;
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull != null && isNullUnchecked(position + arrayOffset);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeLong(values[position + arrayOffset]);
        blockBuilder.closeEntry();
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        if (isNull(position)) {
            output.writeByte(0);
        }
        else {
            output.writeByte(1);
            output.writeLong(values[position + arrayOffset]);
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new LongArrayBlock(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new long[] {values[position + arrayOffset]});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        long[] newValues = new long[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull[position + arrayOffset];
            }
            newValues[i] = values[position + arrayOffset];
        }
        return new LongArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new LongArrayBlock(positionOffset + arrayOffset, length, valueIsNull, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += arrayOffset;
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, positionOffset, length);
        long[] newValues = compactArray(values, positionOffset, length);

        if (newValueIsNull == valueIsNull && newValues == values) {
            return this;
        }
        return new LongArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return LongArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        return format("LongArrayBlock(%d){positionCount=%d}", hashCode(), getPositionCount());
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public int getOffsetBase()
    {
        return arrayOffset;
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return valueIsNull[internalPosition];
    }

    @Override
    public long getLongUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return values[internalPosition];
    }

    @Override
    public Block appendNull()
    {
        boolean[] newValueIsNull = appendNullToIsNullArray(valueIsNull, arrayOffset, positionCount);
        long[] newValues = ensureCapacity(values, arrayOffset + positionCount + 1, SMALL, PRESERVE);

        return new LongArrayBlock(arrayOffset, positionCount + 1, newValueIsNull, newValues);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LongArrayBlock other = (LongArrayBlock) obj;
        return this.arrayOffset == other.arrayOffset &&
                this.positionCount == other.positionCount &&
                Arrays.equals(this.valueIsNull, other.valueIsNull) &&
                Arrays.equals(this.values, other.values) &&
                this.retainedSizeInBytes == other.retainedSizeInBytes;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(arrayOffset,
                positionCount,
                Arrays.hashCode(valueIsNull),
                Arrays.hashCode(values),
                retainedSizeInBytes);
    }
}
