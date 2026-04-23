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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import jakarta.annotation.Nullable;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static com.facebook.presto.common.block.BlockUtil.calculateBlockResetSize;
import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;
import static com.facebook.presto.common.block.Fixed12Block.FIXED12_BYTES;
import static com.facebook.presto.common.block.Fixed12Block.INT_LONGS_PER_ENTRY;
import static com.facebook.presto.common.block.Fixed12Block.encodeFixed12;
import static com.facebook.presto.common.block.Fixed12Block.getFixed12FirstUnchecked;
import static com.facebook.presto.common.block.Fixed12Block.getFixed12SecondUnchecked;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.String.format;

/**
 * Builder for {@link Fixed12Block}. Each entry stores 12 bytes: a long (8 bytes)
 * followed by an int (4 bytes).
 */
public class Fixed12BlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Fixed12BlockBuilder.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new Fixed12Block(0, 1, new boolean[] {true}, new int[3]);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length proportionally
    private boolean[] valueIsNull = new boolean[0];
    private int[] values = new int[0];

    private long retainedSizeInBytes;

    public Fixed12BlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);
        updateDataSize();
    }

    /**
     * Write a 12-byte fixed entry (long + int).
     */
    public Fixed12BlockBuilder writeFixed12(long first, int second)
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        encodeFixed12(first, second, values, positionCount);
        hasNonNullValue = true;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + FIXED12_BYTES);
        }
        return this;
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        throw new UnsupportedOperationException("Fixed12BlockBuilder does not support writeLong directly. Use writeFixed12(long, int) instead.");
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        throw new UnsupportedOperationException("Fixed12BlockBuilder does not support writeInt directly. Use writeFixed12(long, int) instead.");
    }

    @Override
    public BlockBuilder closeEntry()
    {
        // No-op: writeFixed12 handles position advancement
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        valueIsNull[positionCount] = true;
        hasNullValue = true;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + FIXED12_BYTES);
        }
        return this;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        return new Fixed12Block(0, positionCount, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return new Fixed12BlockBuilder(blockBuilderStatus, calculateBlockResetSize(positionCount));
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new Fixed12BlockBuilder(blockBuilderStatus, max(calculateBlockResetSize(positionCount), expectedEntries));
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
        values = Arrays.copyOf(values, newSize * INT_LONGS_PER_ENTRY);
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
        return Fixed12Block.SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.of(Fixed12Block.SIZE_IN_BYTES_PER_POSITION);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return Fixed12Block.SIZE_IN_BYTES_PER_POSITION * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] usedPositions, int usedPositionCount)
    {
        return Fixed12Block.SIZE_IN_BYTES_PER_POSITION * (long) usedPositionCount;
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
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
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
            return getFixed12FirstUnchecked(values, position);
        }
        throw new IllegalArgumentException("offset must be 0");
    }

    @Override
    public int getInt(int position)
    {
        checkReadablePosition(position);
        return getFixed12SecondUnchecked(values, position);
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
        long first = getFixed12FirstUnchecked(values, position);
        int second = getFixed12SecondUnchecked(values, position);
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
            int baseIndex = position * INT_LONGS_PER_ENTRY;
            output.writeInt(values[baseIndex]);
            output.writeInt(values[baseIndex + 1]);
            output.writeInt(values[baseIndex + 2]);
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
            int low = input.readInt();
            int high = input.readInt();
            int second = input.readInt();
            long first = (low & 0xFFFFFFFFL) | ((long) high << 32);
            writeFixed12(first, second);
        }
        return this;
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        int baseIndex = position * INT_LONGS_PER_ENTRY;
        return new Fixed12Block(
                0,
                1,
                valueIsNull[position] ? new boolean[] {true} : null,
                new int[] {values[baseIndex], values[baseIndex + 1], values[baseIndex + 2]});
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
        int[] newValues = new int[length * INT_LONGS_PER_ENTRY];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (hasNullValue) {
                newValueIsNull[i] = valueIsNull[position];
            }
            int srcBase = position * INT_LONGS_PER_ENTRY;
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

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, length);
        }
        return new Fixed12Block(positionOffset, length, hasNullValue ? valueIsNull : null, values);
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
        int[] newValues = compactIntArray(values, positionOffset * INT_LONGS_PER_ENTRY, length * INT_LONGS_PER_ENTRY);
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
        return format("Fixed12BlockBuilder(%d){positionCount=%d}", hashCode(), getPositionCount());
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        assert offset == 0 : "offset must be 0";
        return getFixed12FirstUnchecked(values, internalPosition);
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

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    private static int[] compactIntArray(int[] array, int index, int length)
    {
        if (index == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, index, index + length);
    }
}
