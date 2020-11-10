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
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.block.BlockUtil.calculateBlockResetSize;
import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.countUsedPositions;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.String.format;

public class IntArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IntArrayBlockBuilder.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new IntArrayBlock(0, 1, new boolean[] {true}, new int[1]);

    @Nullable
    private BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private int[] values = new int[0];

    private long retainedSizeInBytes;

    public IntArrayBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        if (values.length <= positionCount) {
            growCapacity();
        }

        values[positionCount] = value;

        hasNonNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + Integer.BYTES);
        }
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (values.length <= positionCount) {
            growCapacity();
        }

        valueIsNull[positionCount] = true;

        hasNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + Integer.BYTES);
        }
        return this;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        return new IntArrayBlock(0, positionCount, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return new IntArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positionCount));
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new IntArrayBlockBuilder(blockBuilderStatus, max(calculateBlockResetSize(positionCount), expectedEntries));
    }

    private void growCapacity()
    {
        int newSize;
        if (initialized) {
            newSize = BlockUtil.calculateNewArraySize(values.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        values = Arrays.copyOf(values, newSize);
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
        return (Integer.BYTES + Byte.BYTES) * (long) positionCount;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return (Integer.BYTES + Byte.BYTES) * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (Integer.BYTES + Byte.BYTES) * (long) countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : Integer.BYTES;
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
    public int getInt(int position)
    {
        checkReadablePosition(position);
        return values[position];
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
        blockBuilder.writeInt(values[position]);
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
            output.writeInt(values[position]);
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
            writeInt(input.readInt());
            closeEntry();
        }
        return this;
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new IntArrayBlock(
                0,
                1,
                valueIsNull[position] ? new boolean[] {true} : null,
                new int[] {values[position]});
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
        int[] newValues = new int[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (hasNullValue) {
                newValueIsNull[i] = valueIsNull[position];
            }
            newValues[i] = values[position];
        }
        return new IntArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, length);
        }
        return new IntArrayBlock(positionOffset, length, hasNullValue ? valueIsNull : null, values);
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
            newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length);
        }
        int[] newValues = Arrays.copyOfRange(values, positionOffset, positionOffset + length);
        return new IntArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return IntArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        return format("IntArrayBlockBuilder(%d){positionCount=%d}", hashCode(), getPositionCount());
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return valueIsNull[internalPosition];
    }

    @Override
    public int getIntUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return values[internalPosition];
    }

    @Override
    public int getOffsetBase()
    {
        return 0;
    }
}
