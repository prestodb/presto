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

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.spi.block.BlockUtil.calculateBlockResetSize;
import static com.facebook.presto.spi.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.spi.block.BlockUtil.intSaturatedCast;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class ShortArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ShortArrayBlockBuilder.class).instanceSize();

    private BlockBuilderStatus blockBuilderStatus;

    private int positionCount;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull;
    private short[] values;

    private int retainedSizeInBytes;

    public ShortArrayBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");
        this.values = new short[expectedEntries];
        this.valueIsNull = new boolean[expectedEntries];

        updateDataSize();
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        if (values.length <= positionCount) {
            growCapacity();
        }

        values[positionCount] = (short) value;

        positionCount++;
        blockBuilderStatus.addBytes(Byte.BYTES + Short.BYTES);
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

        positionCount++;
        blockBuilderStatus.addBytes(Byte.BYTES + Short.BYTES);
        return this;
    }

    @Override
    public Block build()
    {
        return new ShortArrayBlock(positionCount, valueIsNull, values);
    }

    @Override
    public void reset(BlockBuilderStatus blockBuilderStatus)
    {
        this.blockBuilderStatus = requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");

        int newSize = calculateBlockResetSize(positionCount);
        valueIsNull = new boolean[newSize];
        values = new short[newSize];

        positionCount = 0;

        updateDataSize();
    }

    private void growCapacity()
    {
        int newSize = BlockUtil.calculateNewArraySize(values.length);
        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        values = Arrays.copyOf(values, newSize);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = intSaturatedCast(INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values));
    }

    // Copied from ShortArrayBlock
    @Override
    public int getSizeInBytes()
    {
        return intSaturatedCast((Short.BYTES + Byte.BYTES) * (long) positionCount);
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getLength(int position)
    {
        return Short.BYTES;
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }
        return values[position];
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
        blockBuilder.writeShort(values[position]);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new ShortArrayBlock(
                1,
                new boolean[] {valueIsNull[position]},
                new short[] {values[position]});
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        boolean[] newValueIsNull = new boolean[positions.size()];
        short[] newValues = new short[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            int position = positions.get(i);
            checkReadablePosition(position);
            newValueIsNull[i] = valueIsNull[position];
            newValues[i] = values[position];
        }
        return new ShortArrayBlock(positions.size(), newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new ShortArrayBlock(positionOffset, length, valueIsNull, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        boolean[] newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length);
        short[] newValues = Arrays.copyOfRange(values, positionOffset, positionOffset + length);
        return new ShortArrayBlock(length, newValueIsNull, newValues);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new ShortArrayBlockEncoding();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ShortArrayBlockBuilder{");
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
