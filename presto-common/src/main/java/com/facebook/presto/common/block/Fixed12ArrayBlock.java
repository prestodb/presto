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
import jakarta.annotation.Nullable;
import org.openjdk.jol.info.ClassLayout;

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
import static java.lang.String.format;

// 12-byte fixed-width block for high-precision timestamps (p > 6).
// Each position stores 3 consecutive ints: [high32(epochMicros), low32(epochMicros), picosOfMicro].
// getLong(position, 0) returns epochMicros; getInt(position) returns picosOfMicro.
public class Fixed12ArrayBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Fixed12ArrayBlock.class).instanceSize();
    public static final int FIXED12_BYTES = Integer.BYTES * 3;
    public static final int SIZE_IN_BYTES_PER_POSITION = FIXED12_BYTES + Byte.BYTES;

    private static final int INTS_PER_POSITION = 3;

    private final int positionOffset;
    private final int positionCount;
    @Nullable
    private final boolean[] valueIsNull;
    private final int[] values;

    private final long retainedSizeInBytes;

    public Fixed12ArrayBlock(int positionCount, Optional<boolean[]> valueIsNull, int[] values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    Fixed12ArrayBlock(int positionOffset, int positionCount, boolean[] valueIsNull, int[] values)
    {
        if (positionOffset < 0) {
            throw new IllegalArgumentException("positionOffset is negative");
        }
        this.positionOffset = positionOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - (positionOffset * INTS_PER_POSITION) < positionCount * INTS_PER_POSITION) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        if (valueIsNull != null && valueIsNull.length - positionOffset < positionCount) {
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
        return isNull(position) ? 0 : FIXED12_BYTES;
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

    // Returns epochMicros at position; offset must be 0.
    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be 0");
        }
        return getEpochMicros(position + positionOffset);
    }

    @Override
    public long getLong(int position)
    {
        return getLong(position, 0);
    }

    // Returns picosOfMicro at position.
    @Override
    public int getInt(int position)
    {
        checkReadablePosition(position);
        return getPicosOfMicro(position + positionOffset);
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
        return valueIsNull != null && isNullUnchecked(position + positionOffset);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int internalPosition = position + positionOffset;
        blockBuilder.writeLong(getEpochMicros(internalPosition))
                .writeInt(getPicosOfMicro(internalPosition))
                .closeEntry();
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        if (isNull(position)) {
            output.writeByte(0);
        }
        else {
            int internalPosition = position + positionOffset;
            output.writeByte(1);
            output.writeLong(getEpochMicros(internalPosition));
            output.writeInt(getPicosOfMicro(internalPosition));
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        int internalPosition = position + positionOffset;
        return new Fixed12ArrayBlock(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new int[] {
                        values[internalPosition * INTS_PER_POSITION],
                        values[internalPosition * INTS_PER_POSITION + 1],
                        values[internalPosition * INTS_PER_POSITION + 2]});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        int[] newValues = new int[length * INTS_PER_POSITION];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            int internalPosition = position + positionOffset;
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull[internalPosition];
            }
            newValues[i * INTS_PER_POSITION] = values[internalPosition * INTS_PER_POSITION];
            newValues[i * INTS_PER_POSITION + 1] = values[internalPosition * INTS_PER_POSITION + 1];
            newValues[i * INTS_PER_POSITION + 2] = values[internalPosition * INTS_PER_POSITION + 2];
        }
        return new Fixed12ArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        return new Fixed12ArrayBlock(positionOffset + this.positionOffset, length, valueIsNull, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += this.positionOffset;
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, positionOffset, length);
        int[] newValues = compactArray(values, positionOffset * INTS_PER_POSITION, length * INTS_PER_POSITION);

        if (newValueIsNull == valueIsNull && newValues == values) {
            return this;
        }
        return new Fixed12ArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return Fixed12ArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        return format("Fixed12ArrayBlock(%d){positionCount=%d}", hashCode(), getPositionCount());
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        assert offset == 0 : "offset must be 0";
        return getEpochMicros(internalPosition);
    }

    @Override
    public long getLongUnchecked(int internalPosition)
    {
        return getLongUnchecked(internalPosition, 0);
    }

    @Override
    public int getIntUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return getPicosOfMicro(internalPosition);
    }

    @Override
    public int getOffsetBase()
    {
        return positionOffset;
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return valueIsNull[internalPosition];
    }

    @Override
    public Block appendNull()
    {
        boolean[] newValueIsNull = appendNullToIsNullArray(valueIsNull, positionOffset, positionCount);
        int[] newValues = ensureCapacity(values, (positionOffset + positionCount + 1) * INTS_PER_POSITION, SMALL, PRESERVE);
        return new Fixed12ArrayBlock(positionOffset, positionCount + 1, newValueIsNull, newValues);
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
        Fixed12ArrayBlock other = (Fixed12ArrayBlock) obj;
        return this.positionOffset == other.positionOffset &&
                this.positionCount == other.positionCount &&
                Arrays.equals(this.valueIsNull, other.valueIsNull) &&
                Arrays.equals(this.values, other.values) &&
                this.retainedSizeInBytes == other.retainedSizeInBytes;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(positionOffset, positionCount, Arrays.hashCode(valueIsNull), Arrays.hashCode(values), retainedSizeInBytes);
    }

    // Reconstruct epochMicros from two ints at internalPosition.
    // & 0xFFFFFFFFL prevents sign extension of the low 32-bit word.
    private long getEpochMicros(int internalPosition)
    {
        int base = internalPosition * INTS_PER_POSITION;
        return ((long) values[base] << 32) | (values[base + 1] & 0xFFFFFFFFL);
    }

    private int getPicosOfMicro(int internalPosition)
    {
        return values[internalPosition * INTS_PER_POSITION + 2];
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
