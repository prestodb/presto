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
package io.prestosql.spi.block;

import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.BlockUtil.calculateBlockResetSize;
import static io.prestosql.spi.block.BlockUtil.checkArrayRange;
import static io.prestosql.spi.block.BlockUtil.checkValidRegion;
import static io.prestosql.spi.block.BlockUtil.countUsedPositions;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;

public class LongArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongArrayBlockBuilder.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new LongArrayBlock(0, 1, new boolean[] {true}, new long[1]);

    @Nullable
    private BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private long[] values = new long[0];

    private long retainedSizeInBytes;

    public LongArrayBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        if (values.length <= positionCount) {
            growCapacity();
        }

        values[positionCount] = value;

        hasNonNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + Long.BYTES);
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
            blockBuilderStatus.addBytes(Byte.BYTES + Long.BYTES);
        }
        return this;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        return new LongArrayBlock(0, positionCount, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return new LongArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positionCount));
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
        return (Long.BYTES + Byte.BYTES) * (long) positionCount;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return (Long.BYTES + Byte.BYTES) * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (Long.BYTES + Byte.BYTES) * (long) countUsedPositions(positions);
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
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }
        return values[position];
    }

    @Override
    @Deprecated
    // TODO: Remove when we fix intermediate types on aggregations.
    public int getInt(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }
        return toIntExact(values[position]);
    }

    @Override
    @Deprecated
    // TODO: Remove when we fix intermediate types on aggregations.
    public short getShort(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }

        short value = (short) (values[position]);
        if (value != values[position]) {
            throw new ArithmeticException("short overflow");
        }
        return value;
    }

    @Override
    @Deprecated
    // TODO: Remove when we fix intermediate types on aggregations.
    public byte getByte(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }

        byte value = (byte) (values[position]);
        if (value != values[position]) {
            throw new ArithmeticException("byte overflow");
        }
        return value;
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
        blockBuilder.writeLong(values[position]);
        blockBuilder.closeEntry();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new LongArrayBlock(
                0,
                1,
                valueIsNull[position] ? new boolean[] {true} : null,
                new long[] {values[position]});
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
        long[] newValues = new long[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (hasNullValue) {
                newValueIsNull[i] = valueIsNull[position];
            }
            newValues[i] = values[position];
        }
        return new LongArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, length);
        }
        return new LongArrayBlock(positionOffset, length, hasNullValue ? valueIsNull : null, values);
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
        long[] newValues = Arrays.copyOfRange(values, positionOffset, positionOffset + length);
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
        StringBuilder sb = new StringBuilder("LongArrayBlockBuilder{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
