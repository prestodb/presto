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

import javax.annotation.Nullable;

import static com.facebook.presto.common.block.ArrayBlock.createArrayBlockInternal;
import static com.facebook.presto.common.block.BlockUtil.appendNullToIsNullArray;
import static com.facebook.presto.common.block.BlockUtil.appendNullToOffsetsArray;
import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidPositions;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.facebook.presto.common.block.BlockUtil.compactOffsets;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;
import static com.facebook.presto.common.block.MapBlockBuilder.verify;

public abstract class AbstractArrayBlock
        implements Block
{
    protected abstract Block getRawElementBlock();

    protected abstract int[] getOffsets();

    public abstract int getOffsetBase();

    /**
     * @return the underlying valueIsNull array, or null when all values are guaranteed to be non-null
     */
    @Nullable
    protected abstract boolean[] getValueIsNull();

    final int getOffset(int position)
    {
        return getOffsets()[position + getOffsetBase()];
    }

    @Override
    public String getEncodingName()
    {
        return ArrayBlockEncoding.NAME;
    }

    @Override
    public final Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newOffsets = new int[length + 1];
        boolean[] newValueIsNull = mayHaveNull() ? new boolean[length] : null;
        // First traversal will populate newValueIsNull mask (if present) and
        // newOffsets to determine the total number of value positions involved
        for (int i = 0; i < length; i++) {
            int position = positions[i + offset];
            newOffsets[i + 1] = newOffsets[i] + (getOffset(position + 1) - getOffset(position));
            if (isNull(position)) {
                // newValueIsNull will be present unless mayHaveNull() and isNull() implementations are inconsistent
                newValueIsNull[i] = true;
            }
        }

        int totalElements = newOffsets[length];
        if (totalElements == 0) {
            // No elements selected, copy a zero-length region from the elements block
            Block newValues = getRawElementBlock().copyRegion(0, 0);
            return createArrayBlockInternal(0, length, newValueIsNull, newOffsets, newValues);
        }

        // Second traversal will use the newOffsets and total element positions to
        // populate a correctly sized values position selection array
        int[] elementPositions = new int[totalElements];
        int currentOffset = 0;
        for (int i = 0; i < length; i++) {
            int elementsLength = newOffsets[i + 1] - newOffsets[i];
            if (elementsLength > 0) {
                int valuesStartOffset = getOffset(positions[i + offset]);
                for (int j = 0; j < elementsLength; j++) {
                    elementPositions[currentOffset++] = valuesStartOffset + j;
                }
            }
        }
        verify(currentOffset == elementPositions.length, "unexpected element positions");
        Block newValues = getRawElementBlock().copyPositions(elementPositions, 0, elementPositions.length);
        return createArrayBlockInternal(0, length, newValueIsNull, newOffsets, newValues);
    }

    @Override
    public Block getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        return createArrayBlockInternal(
                position + getOffsetBase(),
                length,
                getValueIsNull(),
                getOffsets(),
                getRawElementBlock());
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int valueStart = getOffsets()[getOffsetBase() + position];
        int valueEnd = getOffsets()[getOffsetBase() + position + length];

        return getRawElementBlock().getRegionSizeInBytes(valueStart, valueEnd - valueStart) + ((Integer.BYTES + Byte.BYTES) * (long) length);
    }

    @Override
    public long getRegionLogicalSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int valueStart = getOffsets()[getOffsetBase() + position];
        int valueEnd = getOffsets()[getOffsetBase() + position + length];

        return getRawElementBlock().getRegionLogicalSizeInBytes(valueStart, valueEnd - valueStart) + ((Integer.BYTES + Byte.BYTES) * (long) length);
    }

    @Override
    public long getApproximateRegionLogicalSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int valueStart = getOffset(position);
        int valueEnd = getOffset(position + length);

        return getRawElementBlock().getApproximateRegionLogicalSizeInBytes(valueStart, valueEnd - valueStart) + (Integer.BYTES + Byte.BYTES) * length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        checkValidPositions(positions, getPositionCount());
        boolean[] used = new boolean[getRawElementBlock().getPositionCount()];
        int usedPositionCount = 0;
        for (int i = 0; i < positions.length; ++i) {
            if (positions[i]) {
                usedPositionCount++;
                int valueStart = getOffsets()[getOffsetBase() + i];
                int valueEnd = getOffsets()[getOffsetBase() + i + 1];
                for (int j = valueStart; j < valueEnd; ++j) {
                    used[j] = true;
                }
            }
        }
        return getRawElementBlock().getPositionsSizeInBytes(used) + ((Integer.BYTES + Byte.BYTES) * (long) usedPositionCount);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + length);
        Block newValues = getRawElementBlock().copyRegion(startValueOffset, endValueOffset - startValueOffset);

        int[] newOffsets = compactOffsets(getOffsets(), position + getOffsetBase(), length);
        boolean[] valueIsNull = getValueIsNull();
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, position + getOffsetBase(), length);

        if (newValues == getRawElementBlock() && newOffsets == getOffsets() && newValueIsNull == valueIsNull) {
            return this;
        }
        return createArrayBlockInternal(0, length, newValueIsNull, newOffsets, newValues);
    }

    @Override
    public Block getBlock(int position)
    {
        checkReadablePosition(position);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);
        return getRawElementBlock().getRegion(startValueOffset, endValueOffset - startValueOffset);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.appendStructureInternal(this, position);
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        if (isNull(position)) {
            output.writeByte(0);
        }
        else {
            int startValueOffset = getOffset(position);
            int endValueOffset = getOffset(position + 1);
            int numberOfElements = endValueOffset - startValueOffset;

            output.writeByte(1);
            output.writeInt(numberOfElements);
            Block rawElementBlock = getRawElementBlock();
            for (int i = startValueOffset; i < endValueOffset; i++) {
                rawElementBlock.writePositionTo(i, output);
            }
        }
    }

    protected Block getSingleValueBlockInternal(int position)
    {
        checkReadablePosition(position);

        int startValueOffset = getOffset(position);
        int valueLength = getOffset(position + 1) - startValueOffset;
        Block newValues = getRawElementBlock().copyRegion(startValueOffset, valueLength);

        return createArrayBlockInternal(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new int[] {0, valueLength},
                newValues);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkReadablePosition(position);

        if (isNull(position)) {
            return 0;
        }

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);

        Block rawElementBlock = getRawElementBlock();
        long size = 0;
        for (int i = startValueOffset; i < endValueOffset; i++) {
            size += rawElementBlock.getEstimatedDataSizeForStats(i);
        }
        return size;
    }

    @Override
    public boolean mayHaveNull()
    {
        return getValueIsNull() != null;
    }

    public <T> T apply(ArrayBlockFunction<T> function, int position)
    {
        checkReadablePosition(position);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);
        return function.apply(getRawElementBlock(), startValueOffset, endValueOffset - startValueOffset);
    }

    protected final void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    public interface ArrayBlockFunction<T>
    {
        T apply(Block block, int startPosition, int length);
    }

    @Override
    public Block getBlockUnchecked(int internalPosition)
    {
        int startValueOffset = getOffsets()[internalPosition];
        int endValueOffset = getOffsets()[internalPosition + 1];
        return getRawElementBlock().getRegion(startValueOffset, endValueOffset - startValueOffset);
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return getValueIsNull()[internalPosition];
    }

    @Override
    public Block appendNull()
    {
        boolean[] valueIsNull = appendNullToIsNullArray(getValueIsNull(), getOffsetBase(), getPositionCount());
        int[] offsets = appendNullToOffsetsArray(getOffsets(), getOffsetBase(), getPositionCount());

        return createArrayBlockInternal(
                getOffsetBase(),
                getPositionCount() + 1,
                valueIsNull,
                offsets,
                getRawElementBlock());
    }
}
