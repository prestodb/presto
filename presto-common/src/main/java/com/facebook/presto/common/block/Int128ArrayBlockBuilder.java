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
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.block.BlockUtil.calculateBlockResetSize;
import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.facebook.presto.common.block.BlockUtil.countUsedPositions;
import static com.facebook.presto.common.block.BlockUtil.getNum128Integers;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;
import static com.facebook.presto.common.block.Int128ArrayBlock.INT128_BYTES;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Integer.bitCount;
import static java.lang.Math.max;
import static java.lang.String.format;

public class Int128ArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Int128ArrayBlockBuilder.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new Int128ArrayBlock(0, 1, new boolean[] {true}, new long[2]);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private long[] values = new long[0];

    private long retainedSizeInBytes;

    private int entryPositionCount;

    public Int128ArrayBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        values[(positionCount * 2) + entryPositionCount] = value;
        entryPositionCount++;

        hasNonNullValue = true;
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        if (entryPositionCount != 2) {
            throw new IllegalStateException("Expected entry size to be exactly " + INT128_BYTES + " bytes but was " + (entryPositionCount * SIZE_OF_LONG));
        }

        positionCount++;
        entryPositionCount = 0;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + INT128_BYTES);
        }
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }
        if (entryPositionCount != 0) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        valueIsNull[positionCount] = true;

        hasNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + INT128_BYTES);
        }
        return this;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        return new Int128ArrayBlock(0, positionCount, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return new Int128ArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positionCount));
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new Int128ArrayBlockBuilder(blockBuilderStatus, max(calculateBlockResetSize(positionCount), expectedEntries));
    }

    private void growCapacity()
    {
        int newSize;
        if (initialized) {
            newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        values = Arrays.copyOf(values, newSize * 2);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }

    @Override
    public long getSizeInBytes()
    {
        return (INT128_BYTES + Byte.BYTES) * (long) positionCount;
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
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
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
        if (offset == 0) {
            return values[position * 2];
        }
        if (offset == 8) {
            return values[(position * 2) + 1];
        }
        throw new IllegalArgumentException("offset must be 0 or 8");
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
        return getSliceUnchecked(position + getOffsetBase(), offset, length);
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

        int currentPosition = (position + offset + getOffsetBase()) * 2;
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
        return hasNullValue;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull[position];
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeLong(values[position * 2]);
        blockBuilder.writeLong(values[(position * 2) + 1]);
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
            output.writeLong(values[position * 2]);
            output.writeLong(values[(position * 2) + 1]);
        }
    }

    @Override
    public BlockBuilder readPositionFrom(SliceInput input)
    {
        boolean isNull = input.readByte() == 0;
        if (isNull) {
            appendNull();
        }
        else {
            writeLong(input.readLong());
            writeLong(input.readLong());
            closeEntry();
        }
        return this;
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new Int128ArrayBlock(
                0,
                1,
                valueIsNull[position] ? new boolean[] {true} : null,
                new long[] {
                        values[position * 2],
                        values[(position * 2) + 1]});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, length);
        }
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = new boolean[length];
        }
        long[] newValues = new long[length * 2];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (hasNullValue) {
                newValueIsNull[i] = valueIsNull[position];
            }
            newValues[i * 2] = values[(position * 2)];
            newValues[(i * 2) + 1] = values[(position * 2) + 1];
        }
        return new Int128ArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, length);
        }
        return new Int128ArrayBlock(positionOffset, length, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, length);
        }
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = compactArray(valueIsNull, positionOffset, length);
        }
        long[] newValues = compactArray(values, positionOffset * 2, length * 2);
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
        return format("Int128ArrayBlockBuilder(%d){positionCount=%d}", hashCode(), getPositionCount());
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
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return valueIsNull[internalPosition];
    }

    @Override
    public int getOffsetBase()
    {
        return 0;
    }
}
