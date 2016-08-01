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

import com.facebook.presto.spi.block.array.BooleanArray;
import com.facebook.presto.spi.block.array.LongArray;
import com.facebook.presto.spi.block.resource.BlockResourceContext;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.spi.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.spi.block.BlockUtil.intSaturatedCast;

public class ResourceLongArrayBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ResourceLongArrayBlock.class).instanceSize();
    private final int arrayOffset;
    private final int positionCount;
    private final BooleanArray valueIsNull;
    private final LongArray values;
    private final BlockResourceContext resourceContext;
    private final int sizeInBytes;
    private final int retainedSizeInBytes;

    public static Block convert(LongArrayBlock block, BlockResourceContext resourceContext)
    {
        int arrayOffset = block.arrayOffset;
        int positionCount = block.positionCount;
        boolean[] valueIsNull = block.valueIsNull;
        long[] values = block.values;
        BooleanArray newValueIsNull = resourceContext.newBooleanArray(valueIsNull.length);
        LongArray newValues = resourceContext.newLongArray(values.length);
        for (int i = 0; i < values.length; i++) {
            newValues.set(i, values[i]);
        }
        for (int i = 0; i < valueIsNull.length; i++) {
            newValueIsNull.set(i, valueIsNull[i]);
        }
        return new ResourceLongArrayBlock(arrayOffset, positionCount, newValueIsNull, newValues, resourceContext);
    }

    public ResourceLongArrayBlock(int positionCount, BooleanArray valueIsNull, LongArray values, BlockResourceContext resourceContext)
    {
        this(0, positionCount, valueIsNull, values, resourceContext);
    }

    ResourceLongArrayBlock(int arrayOffset, int positionCount, BooleanArray valueIsNull, LongArray values, BlockResourceContext resourceContext)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;
        if (values.length() - arrayOffset < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;
        if (valueIsNull.length() - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;
        sizeInBytes = intSaturatedCast((Long.BYTES + Byte.BYTES) * (long) positionCount);
        retainedSizeInBytes = intSaturatedCast(INSTANCE_SIZE + valueIsNull.getRetainedSize() + values.getRetainedSize());
        this.resourceContext = resourceContext;
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
        return values.get(position + arrayOffset);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull.get(position + arrayOffset);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeLong(values.get(position + arrayOffset));
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        LongArray newLongArray = resourceContext.newLongArray(1);
        BooleanArray newBooleanArray = resourceContext.newBooleanArray(1);
        newBooleanArray.set(0, valueIsNull.get(position + arrayOffset));
        newLongArray.set(0, values.get(position + arrayOffset));
        return new ResourceLongArrayBlock(
                1,
                newBooleanArray,
                newLongArray,
                resourceContext);
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        BooleanArray newValueIsNull = resourceContext.newBooleanArray(positions.size());
        LongArray newValues = resourceContext.newLongArray(positions.size());
        for (int i = 0; i < positions.size(); i++) {
            int position = positions.get(i);
            checkReadablePosition(position);
            newValueIsNull.set(i, valueIsNull.get(position + arrayOffset));
            newValues.set(i, values.get(position + arrayOffset));
        }
        return new ResourceLongArrayBlock(positions.size(), newValueIsNull, newValues, resourceContext);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        return new ResourceLongArrayBlock(positionOffset + arrayOffset, length, valueIsNull, values, resourceContext);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        positionOffset += arrayOffset;
        BooleanArray newValueIsNull = resourceContext.copyOfRangeBooleanArray(valueIsNull, positionOffset, positionOffset + length);
        LongArray newValues = resourceContext.copyOfRangeLongArray(values, positionOffset, positionOffset + length);
        return new ResourceLongArrayBlock(length, newValueIsNull, newValues, resourceContext);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new LongArrayBlockEncoding();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ResourceLongArrayBlock{");
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
