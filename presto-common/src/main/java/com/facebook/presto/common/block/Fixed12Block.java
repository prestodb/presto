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

import static com.facebook.presto.common.block.BlockUtil.appendNullToIsNullArray;
import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;

/**
 * A fixed-width block that stores 12 bytes per position using an int[] array
 * with 3 ints per position. The layout stores a long (as two ints in little-endian
 * order) followed by an int, for a total of 12 bytes per entry.
 * <p>
 * This block type is designed for types that require more than 8 bytes but less than
 * 16 bytes per value, such as LongTimestamp (epoch micros + picos of micro).
 */
public class Fixed12Block
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Fixed12Block.class).instanceSize();
    public static final int FIXED12_BYTES = Long.BYTES + Integer.BYTES;
    public static final int SIZE_IN_BYTES_PER_POSITION = FIXED12_BYTES + Byte.BYTES;
    static final int INT_LONGS_PER_ENTRY = 3; // 3 ints = 12 bytes = 1 long + 1 int

    private final int positionOffset;
    private final int positionCount;
    @Nullable
    private final boolean[] valueIsNull;
    private final int[] values;

    private final long retainedSizeInBytes;

    public Fixed12Block(int positionCount, Optional<boolean[]> valueIsNull, int[] values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    Fixed12Block(int positionOffset, int positionCount, boolean[] valueIsNull, int[] values)
    {
        if (positionOffset < 0) {
            throw new IllegalArgumentException("positionOffset is negative");
        }
        this.positionOffset = positionOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - (positionOffset * INT_LONGS_PER_ENTRY) < positionCount * INT_LONGS_PER_ENTRY) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        if (valueIsNull != null && valueIsNull.length - positionOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    }

    /**
     * Gets the first component (long) stored at the given position.
     * The long is reconstructed from two ints stored in little-endian order.
     */
    public long getFixed12First(int position)
    {
        checkReadablePosition(position);
        return getFixed12FirstUnchecked(values, position + positionOffset);
    }

    /**
     * Gets the second component (int) stored at the given position.
     */
    public int getFixed12Second(int position)
    {
        checkReadablePosition(position);
        return getFixed12SecondUnchecked(values, position + positionOffset);
    }

    /**
     * Gets the first component (long) at the given internal position without bounds checking.
     * The long is reconstructed from two ints stored in little-endian order.
     * Package-private for use by Fixed12BlockBuilder and Fixed12BlockEncoding.
     */
    static long getFixed12FirstUnchecked(int[] values, int internalPosition)
    {
        int baseIndex = internalPosition * INT_LONGS_PER_ENTRY;
        // Reconstruct long from two ints (little-endian: low 32 bits first, high 32 bits second)
        return (values[baseIndex] & 0xFFFFFFFFL) | ((long) values[baseIndex + 1] << 32);
    }

    /**
     * Gets the second component (int) at the given internal position without bounds checking.
     * Package-private for use by Fixed12BlockBuilder.
     */
    static int getFixed12SecondUnchecked(int[] values, int internalPosition)
    {
        int baseIndex = internalPosition * INT_LONGS_PER_ENTRY;
        return values[baseIndex + 2];
    }

    /**
     * Encodes a long and int into the int[] array at the specified position.
     */
    public static void encodeFixed12(long first, int second, int[] target, int position)
    {
        int baseIndex = position * INT_LONGS_PER_ENTRY;
        target[baseIndex] = (int) first; // low 32 bits
        target[baseIndex + 1] = (int) (first >> 32); // high 32 bits
        target[baseIndex + 2] = second;
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

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset == 0) {
            return getFixed12FirstUnchecked(values, position + positionOffset);
        }
        throw new IllegalArgumentException("offset must be 0");
    }

    @Override
    public int getInt(int position)
    {
        checkReadablePosition(position);
        return getFixed12SecondUnchecked(values, position + positionOffset);
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
        int internalPos = position + positionOffset;
        long first = getFixed12FirstUnchecked(values, internalPos);
        int second = getFixed12SecondUnchecked(values, internalPos);
        if (blockBuilder instanceof Fixed12BlockBuilder) {
            ((Fixed12BlockBuilder) blockBuilder).writeFixed12(first, second);
        }
        else {
            blockBuilder.writeLong(first);
            blockBuilder.writeInt(second);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        if (isNull(position)) {
            output.writeByte(0);
        }
        else {
            output.writeByte(1);
            int internalPos = position + positionOffset;
            int baseIndex = internalPos * INT_LONGS_PER_ENTRY;
            output.writeInt(values[baseIndex]);
            output.writeInt(values[baseIndex + 1]);
            output.writeInt(values[baseIndex + 2]);
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        int internalPos = position + positionOffset;
        int baseIndex = internalPos * INT_LONGS_PER_ENTRY;
        return new Fixed12Block(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new int[] {values[baseIndex], values[baseIndex + 1], values[baseIndex + 2]});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        int[] newValues = new int[length * INT_LONGS_PER_ENTRY];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull[position + positionOffset];
            }
            int srcBase = (position + positionOffset) * INT_LONGS_PER_ENTRY;
            int dstBase = i * INT_LONGS_PER_ENTRY;
            newValues[dstBase] = values[srcBase];
            newValues[dstBase + 1] = values[srcBase + 1];
            newValues[dstBase + 2] = values[srcBase + 2];
        }
        return new Fixed12Block(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        return new Fixed12Block(positionOffset + this.positionOffset, length, valueIsNull, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        int absoluteOffset = positionOffset + this.positionOffset;
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, absoluteOffset, length);
        int[] newValues = compactIntArray(values, absoluteOffset * INT_LONGS_PER_ENTRY, length * INT_LONGS_PER_ENTRY);

        if (newValueIsNull == valueIsNull && newValues == values) {
            return this;
        }
        return new Fixed12Block(0, length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return Fixed12BlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        return format("Fixed12Block(%d){positionCount=%d}", hashCode(), getPositionCount());
    }

    @Override
    public Block appendNull()
    {
        boolean[] newValueIsNull = appendNullToIsNullArray(valueIsNull, positionOffset, positionCount);
        int[] newValues = Arrays.copyOf(values, Math.max(values.length, (positionOffset + positionCount + 1) * INT_LONGS_PER_ENTRY));
        return new Fixed12Block(positionOffset, positionCount + 1, newValueIsNull, newValues);
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        assert offset == 0 : "offset must be 0";
        return getFixed12FirstUnchecked(values, internalPosition);
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

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
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

        Fixed12Block other = (Fixed12Block) obj;
        if (this.positionCount != other.positionCount) {
            return false;
        }

        int thisStart = this.positionOffset;
        int thisEnd = thisStart + this.positionCount;
        int otherStart = other.positionOffset;
        int otherEnd = otherStart + other.positionCount;

        // Compare nulls over the visible range only
        if (this.valueIsNull != null && other.valueIsNull != null) {
            if (!Arrays.equals(this.valueIsNull, thisStart, thisEnd,
                    other.valueIsNull, otherStart, otherEnd)) {
                return false;
            }
        }
        else if (this.valueIsNull != null || other.valueIsNull != null) {
            // One has null tracking and the other doesn't; check if the one with tracking has any nulls
            boolean[] nonNullArray = (this.valueIsNull != null) ? this.valueIsNull : other.valueIsNull;
            int start = (this.valueIsNull != null) ? thisStart : otherStart;
            int end = (this.valueIsNull != null) ? thisEnd : otherEnd;
            for (int i = start; i < end; i++) {
                if (nonNullArray[i]) {
                    return false;
                }
            }
        }

        int thisValuesStart = this.positionOffset * INT_LONGS_PER_ENTRY;
        int thisValuesEnd = thisValuesStart + this.positionCount * INT_LONGS_PER_ENTRY;
        int otherValuesStart = other.positionOffset * INT_LONGS_PER_ENTRY;
        int otherValuesEnd = otherValuesStart + other.positionCount * INT_LONGS_PER_ENTRY;

        return Arrays.equals(this.values, thisValuesStart, thisValuesEnd,
                other.values, otherValuesStart, otherValuesEnd);
    }

    @Override
    public int hashCode()
    {
        int thisStart = this.positionOffset;
        int thisEnd = thisStart + this.positionCount;
        int thisValuesStart = this.positionOffset * INT_LONGS_PER_ENTRY;
        int thisValuesEnd = thisValuesStart + this.positionCount * INT_LONGS_PER_ENTRY;

        int result = Objects.hash(positionCount);
        // Hash only the visible range of the arrays
        for (int i = thisStart; valueIsNull != null && i < thisEnd; i++) {
            result = 31 * result + Boolean.hashCode(valueIsNull[i]);
        }
        for (int i = thisValuesStart; i < thisValuesEnd; i++) {
            result = 31 * result + Integer.hashCode(values[i]);
        }
        return result;
    }

    private static int[] compactIntArray(int[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }
}
