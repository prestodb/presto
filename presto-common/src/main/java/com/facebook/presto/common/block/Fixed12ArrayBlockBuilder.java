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

import com.facebook.presto.common.TimestampConstants;
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
import static com.facebook.presto.common.block.Fixed12ArrayBlock.FIXED12_BYTES;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.String.format;

/**
 * Each non-null entry requires writeLong(epochMicros) -> writeInt(picosOfMicro) -> closeEntry() in order.
 * Null entries use appendNull() alone. Violations throw IllegalStateException immediately.
 */
public class Fixed12ArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Fixed12ArrayBlockBuilder.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new Fixed12ArrayBlock(0, 1, new boolean[] {true}, new int[3]);

    private static final int INTS_PER_POSITION = Fixed12ArrayBlock.INTS_PER_POSITION;

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    private boolean[] valueIsNull = new boolean[0];
    private int[] values = new int[0];

    private long retainedSizeInBytes;

    private int entryFieldCount;

    public Fixed12ArrayBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);
        updateDataSize();
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        if (entryFieldCount != 0) {
            throw new IllegalStateException("writeLong must be the first write for each entry");
        }
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }
        Fixed12ArrayBlock.packEpochMicros(values, positionCount * INTS_PER_POSITION, value);
        entryFieldCount = 1;
        hasNonNullValue = true;
        return this;
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        if (entryFieldCount != 1) {
            throw new IllegalStateException("writeInt must follow writeLong for each entry");
        }
        TimestampConstants.checkPicosOfMicro(value);
        values[positionCount * INTS_PER_POSITION + 2] = value;
        entryFieldCount = 2;
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        if (entryFieldCount != 2) {
            throw new IllegalStateException(
                    "Each entry requires writeLong(epochMicros) then writeInt(picosOfMicro) before closeEntry()");
        }
        positionCount++;
        entryFieldCount = 0;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + FIXED12_BYTES);
        }
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (entryFieldCount != 0) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }
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
        if (entryFieldCount != 0) {
            throw new IllegalStateException("Current entry is not complete — call writeInt and closeEntry before building");
        }
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        return new Fixed12ArrayBlock(0, positionCount, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return new Fixed12ArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positionCount));
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new Fixed12ArrayBlockBuilder(blockBuilderStatus, max(calculateBlockResetSize(positionCount), expectedEntries));
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
        values = Arrays.copyOf(values, newSize * INTS_PER_POSITION);
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
        return Fixed12ArrayBlock.SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.of(Fixed12ArrayBlock.SIZE_IN_BYTES_PER_POSITION);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return Fixed12ArrayBlock.SIZE_IN_BYTES_PER_POSITION * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] usedPositions, int usedPositionCount)
    {
        return Fixed12ArrayBlock.SIZE_IN_BYTES_PER_POSITION * (long) usedPositionCount;
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
    public long getLong(int position)
    {
        return getLong(position, 0);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be 0");
        }
        return getEpochMicros(position);
    }

    @Override
    public int getInt(int position)
    {
        checkReadablePosition(position);
        return values[position * INTS_PER_POSITION + 2];
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
        blockBuilder.writeLong(getEpochMicros(position))
                .writeInt(values[position * INTS_PER_POSITION + 2])
                .closeEntry();
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        if (isNull(position)) {
            output.writeByte(0);
        }
        else {
            output.writeByte(1);
            output.writeLong(getEpochMicros(position));
            output.writeInt(values[position * INTS_PER_POSITION + 2]);
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
            writeInt(input.readInt());
            closeEntry();
        }
        return this;
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new Fixed12ArrayBlock(
                0,
                1,
                valueIsNull[position] ? new boolean[] {true} : null,
                new int[] {
                        values[position * INTS_PER_POSITION],
                        values[position * INTS_PER_POSITION + 1],
                        values[position * INTS_PER_POSITION + 2]});
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
        int[] newValues = new int[length * INTS_PER_POSITION];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (hasNullValue) {
                newValueIsNull[i] = valueIsNull[position];
            }
            newValues[i * INTS_PER_POSITION] = values[position * INTS_PER_POSITION];
            newValues[i * INTS_PER_POSITION + 1] = values[position * INTS_PER_POSITION + 1];
            newValues[i * INTS_PER_POSITION + 2] = values[position * INTS_PER_POSITION + 2];
        }
        return new Fixed12ArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, length);
        }
        return new Fixed12ArrayBlock(positionOffset, length, hasNullValue ? valueIsNull : null, values);
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
        int[] newValues = compactArray(values, positionOffset * INTS_PER_POSITION, length * INTS_PER_POSITION);
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
        return format("Fixed12ArrayBlockBuilder(%d){positionCount=%d}", hashCode(), getPositionCount());
    }

    @Override
    public long getLongUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return getEpochMicros(internalPosition);
    }

    @Override
    public long getLongUnchecked(int internalPosition, int offset)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        assert offset == 0 : "offset must be 0";
        return getEpochMicros(internalPosition);
    }

    @Override
    public int getIntUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return values[internalPosition * INTS_PER_POSITION + 2];
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

    private long getEpochMicros(int position)
    {
        return Fixed12ArrayBlock.unpackEpochMicros(values, position * INTS_PER_POSITION);
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
