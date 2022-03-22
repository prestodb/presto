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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.common.block.BlockUtil.appendNullToIsNullArray;
import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.facebook.presto.common.block.BlockUtil.countUsedPositions;
import static com.facebook.presto.common.block.BlockUtil.getNum128Integers;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Integer.bitCount;
import static java.lang.String.format;

public class Int128ArrayBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Int128ArrayBlock.class).instanceSize();
    public static final int INT128_BYTES = Long.BYTES + Long.BYTES;

    private final int positionOffset;
    private final int positionCount;
    @Nullable
    private final boolean[] valueIsNull;
    private final long[] values;

    private final long sizeInBytes;
    private final long retainedSizeInBytes;

    public Int128ArrayBlock(int positionCount, Optional<boolean[]> valueIsNull, long[] values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    Int128ArrayBlock(int positionOffset, int positionCount, boolean[] valueIsNull, long[] values)
    {
        if (positionOffset < 0) {
            throw new IllegalArgumentException("positionOffset is negative");
        }
        this.positionOffset = positionOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - (positionOffset * 2) < positionCount * 2) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        if (valueIsNull != null && valueIsNull.length - positionOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        sizeInBytes = (INT128_BYTES + Byte.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return (INT128_BYTES + Byte.BYTES) * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (INT128_BYTES + Byte.BYTES) * (long) countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : INT128_BYTES;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(values, sizeOf(values));
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, sizeOf(valueIsNull));
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
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
        if (offset != 0 && offset != 8) {
            throw new IllegalArgumentException("offset must be 0 or 8");
        }
        return getLongUnchecked(position + positionOffset, offset);
    }

    /**
     * Get the Slice starting at {@code this.positionOffset + offset} in the value at {@code position} with {@code length} bytes.
     *
     * @param position The logical position of the 128-bit integer in the values array.
     *                 For example, position = 0 refers to the 128-bit integer at values[2] and values[3] if this.positionOffset = 1.
     * @param offset The offset to the position in the unit of 128-bit integers.
     *               For example, offset = 1 means the next position (one 128-bit integer or 16 bytes) to the specified position.
     *               This means we always compare bytes starting at 128-bit integer boundaries.
     * @param length The length in bytes. It has to be a multiple of 16.
     */
    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkValidRegion(positionCount, offset, length / SIZE_OF_LONG / 2);
        return getSliceUnchecked(position + positionOffset, offset, length);
    }

    @Override
    public int getSliceLength(int position)
    {
        return SIZE_OF_LONG * 2;
    }

    /**
     * Get the Slice starting at {@code offset} in the value at {@code internalPosition} with {@code length} bytes.
     *
     * @param internalPosition The physical position of the 128-bit integer in the values array.
     *                         For example, internalPosition = 1 refers to the 128-bit integer at values[2] and values[3]
     * @param offset The offset to the position in the unit of 128-bit integers.
     *                       For example, offset = 1 means the next position (one 128-bit integer or 16 bytes) to the specified position.
     *                       This means we always compare bytes starting at 128-bit integer boundaries.
     * @param length The length in bytes. It has to be a multiple of 16.
     */
    @Override
    public Slice getSliceUnchecked(int internalPosition, int offset, int length)
    {
        int num128Integers = getNum128Integers(length);
        return Slices.wrappedLongArray(values, (internalPosition + offset) * 2, num128Integers * 2);
    }

    @Override
    public int getSliceLengthUnchecked(int position)
    {
        return SIZE_OF_LONG * 2;
    }

    /**
     * Is the byte sequences at the {@code position + offset} position in the 128-bit values of {@code length} bytes equal
     * to the byte sequence at {@code otherOffset} in {@code otherSlice}.
     *
     * @param position The position of 128-bit integer.
     * @param offset The offset to the position in the unit of 128-bit integers.
     * For example, offset = 1 means the next position (one 128-bit integer or 16 bytes) to the specified position.
     * This means we always compare starting at 128-bit integer boundaries.
     * @param otherSlice The slice to compare to.
     * @param otherOffset The offset in bytes to the start of otherSlice.
     * @param length The length to compare in bytes. It has to be a multiple of 16.
     * @return True if the bytes are the same, false otherwise.
     */
    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        int num128Integers = getNum128Integers(length);
        checkValidRegion(positionCount, position + offset, num128Integers);
        if (otherOffset < 0 || length < 0 || otherOffset + length > otherSlice.length()) {
            throw new IllegalArgumentException(format("otherOffset %d, length %d are invalid for otherSlice with length %d", otherOffset, length, otherSlice.length()));
        }

        int currentPosition = (position + offset + positionOffset) * 2;
        for (int i = 0; i < num128Integers; i++) {
            if (values[currentPosition] != otherSlice.getLong(otherOffset) || values[currentPosition + 1] != otherSlice.getLong(otherOffset + SIZE_OF_LONG)) {
                return false;
            }

            currentPosition += 2;
            otherOffset += SIZE_OF_LONG * 2;
        }

        return true;
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
        blockBuilder.writeLong(values[(position + positionOffset) * 2]);
        blockBuilder.writeLong(values[((position + positionOffset) * 2) + 1]);
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
            output.writeLong(values[(position + positionOffset) * 2]);
            output.writeLong(values[((position + positionOffset) * 2) + 1]);
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new Int128ArrayBlock(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new long[] {
                        values[(position + positionOffset) * 2],
                        values[((position + positionOffset) * 2) + 1]});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        long[] newValues = new long[length * 2];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull[position + positionOffset];
            }
            newValues[i * 2] = values[(position + positionOffset) * 2];
            newValues[(i * 2) + 1] = values[((position + positionOffset) * 2) + 1];
        }
        return new Int128ArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new Int128ArrayBlock(positionOffset + this.positionOffset, length, valueIsNull, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += this.positionOffset;
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, positionOffset, length);
        long[] newValues = compactArray(values, positionOffset * 2, length * 2);

        if (newValueIsNull == valueIsNull && newValues == values) {
            return this;
        }
        return new Int128ArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return Int128ArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        return format("Int128ArrayBlock(%d){positionCount=%d}", hashCode(), getPositionCount());
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        assert offset == 0 || offset == 8 : "offset must be 0 or 8";
        return values[internalPosition * 2 + bitCount(offset)];
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
        long[] newValues = ensureCapacity(values, (positionOffset + positionCount + 1) * 2, SMALL, PRESERVE);
        return new Int128ArrayBlock(positionOffset, positionCount + 1, newValueIsNull, newValues);
    }
}
