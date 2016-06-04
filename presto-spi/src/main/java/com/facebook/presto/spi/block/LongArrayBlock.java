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

import static com.facebook.presto.spi.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.spi.block.BlockUtil.intSaturatedCast;
import static io.airlift.slice.SizeOf.sizeOf;

public class LongArrayBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongArrayBlock.class).instanceSize();

    private final int arrayOffset;
    private final int positionCount;
    private final boolean[] valueIsNull;
    private final long[] values;

    private final int sizeInBytes;
    private final int retainedSizeInBytes;

    public LongArrayBlock(int positionCount, boolean[] valueIsNull, long[] values)
    {
        this(0, positionCount, valueIsNull, values);
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

        if (valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        sizeInBytes = intSaturatedCast((Long.BYTES + Byte.BYTES) * (long) positionCount);
        retainedSizeInBytes = intSaturatedCast(INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values));
    }

    @Override
    public int getSizeInBytes()
    {
        return sizeInBytes;
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
        return Long.BYTES;
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }
        return values[position + arrayOffset];
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull[position + arrayOffset];
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeLong(values[position + arrayOffset]);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new LongArrayBlock(
                1,
                new boolean[] {valueIsNull[position + arrayOffset]},
                new long[] {values[position + arrayOffset]});
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        boolean[] newValueIsNull = new boolean[positions.size()];
        long[] newValues = new long[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            int position = positions.get(i);
            checkReadablePosition(position);
            newValueIsNull[i] = valueIsNull[position + arrayOffset];
            newValues[i] = values[position + arrayOffset];
        }
        return new LongArrayBlock(positions.size(), newValueIsNull, newValues);
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
        boolean[] newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length);
        long[] newValues = Arrays.copyOfRange(values, positionOffset, positionOffset + length);
        return new LongArrayBlock(length, newValueIsNull, newValues);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new LongArrayBlockEncoding();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("LongArrayBlock{");
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
