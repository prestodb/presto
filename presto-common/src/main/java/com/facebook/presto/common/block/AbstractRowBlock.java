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

import static com.facebook.presto.common.block.BlockUtil.appendNullToIsNullArray;
import static com.facebook.presto.common.block.BlockUtil.appendNullToOffsetsArray;
import static com.facebook.presto.common.block.BlockUtil.arraySame;
import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidPositions;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.facebook.presto.common.block.BlockUtil.compactOffsets;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;
import static com.facebook.presto.common.block.RowBlock.createRowBlockInternal;

public abstract class AbstractRowBlock
        implements Block
{
    protected final int numFields;

    protected abstract Block[] getRawFieldBlocks();

    protected abstract int[] getFieldBlockOffsets();

    public abstract int getOffsetBase();

    /**
     * @return the underlying rowIsNull array, or null when all rows are guaranteed to be non-null
     */
    @Nullable
    protected abstract boolean[] getRowIsNull();

    // the offset in each field block, it can also be viewed as the "entry-based" offset in the RowBlock
    protected final int getFieldBlockOffset(int position)
    {
        return getFieldBlockOffsets()[position + getOffsetBase()];
    }

    protected AbstractRowBlock(int numFields)
    {
        if (numFields <= 0) {
            throw new IllegalArgumentException("Number of fields in RowBlock must be positive");
        }
        this.numFields = numFields;
    }

    @Override
    public String getEncodingName()
    {
        return RowBlockEncoding.NAME;
    }

    @Override
    public final Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newOffsets = new int[length + 1];
        int[] fieldBlockPositions = new int[length];
        boolean[] newRowIsNull = null;
        int fieldBlockPositionCount;
        if (mayHaveNull()) {
            newRowIsNull = new boolean[length];
            fieldBlockPositionCount = 0;
            for (int i = 0; i < length; i++) {
                newOffsets[i] = fieldBlockPositionCount;
                int position = positions[offset + i];
                if (isNull(position)) {
                    newRowIsNull[i] = true;
                }
                else {
                    fieldBlockPositions[fieldBlockPositionCount++] = getFieldBlockOffset(position);
                }
            }
            // Record last offset position
            newOffsets[length] = fieldBlockPositionCount;
            if (fieldBlockPositionCount == length) {
                // No nulls encountered, discard the null mask
                newRowIsNull = null;
            }
        }
        else {
            // No nulls are present
            fieldBlockPositionCount = fieldBlockPositions.length;
            for (int i = 0; i < fieldBlockPositions.length; i++) {
                newOffsets[i] = i; // No nulls, all offsets are just their index mapping
                int position = positions[offset + i];
                checkReadablePosition(position);
                fieldBlockPositions[i] = getFieldBlockOffset(position);
            }
            // Record last offset position
            newOffsets[fieldBlockPositions.length] = fieldBlockPositions.length;
        }

        Block[] newBlocks = new Block[numFields];
        Block[] oldBlocks = getRawFieldBlocks();
        for (int i = 0; i < newBlocks.length; i++) {
            newBlocks[i] = oldBlocks[i].copyPositions(fieldBlockPositions, 0, fieldBlockPositionCount);
        }
        return createRowBlockInternal(0, length, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public Block getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        return createRowBlockInternal(position + getOffsetBase(), length, getRowIsNull(), getFieldBlockOffsets(), getRawFieldBlocks());
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + length);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;

        long regionSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) length;
        for (int i = 0; i < numFields; i++) {
            regionSizeInBytes += getRawFieldBlocks()[i].getRegionSizeInBytes(startFieldBlockOffset, fieldBlockLength);
        }
        return regionSizeInBytes;
    }

    @Override
    public long getRegionLogicalSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + length);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;

        long regionLogicalSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) length;
        for (int i = 0; i < numFields; i++) {
            regionLogicalSizeInBytes += getRawFieldBlocks()[i].getRegionLogicalSizeInBytes(startFieldBlockOffset, fieldBlockLength);
        }
        return regionLogicalSizeInBytes;
    }

    @Override
    public long getApproximateRegionLogicalSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int fieldBlockLength = getFieldBlockOffset(position + length) - startFieldBlockOffset;

        long approximateLogicalSizeInBytes = (Integer.BYTES + Byte.BYTES) * length; // offsets and rowIsNull
        for (int i = 0; i < numFields; i++) {
            approximateLogicalSizeInBytes += getRawFieldBlocks()[i].getApproximateRegionLogicalSizeInBytes(startFieldBlockOffset, fieldBlockLength);
        }

        return approximateLogicalSizeInBytes;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        checkValidPositions(positions, getPositionCount());

        int usedPositionCount = 0;
        boolean[] fieldPositions = new boolean[getRawFieldBlocks()[0].getPositionCount()];
        for (int i = 0; i < positions.length; i++) {
            if (positions[i]) {
                usedPositionCount++;
                int startFieldBlockOffset = getFieldBlockOffset(i);
                int endFieldBlockOffset = getFieldBlockOffset(i + 1);
                for (int j = startFieldBlockOffset; j < endFieldBlockOffset; j++) {
                    fieldPositions[j] = true;
                }
            }
        }
        long sizeInBytes = 0;
        for (int j = 0; j < numFields; j++) {
            sizeInBytes += getRawFieldBlocks()[j].getPositionsSizeInBytes(fieldPositions);
        }
        return sizeInBytes + (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount;
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + length);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;
        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = getRawFieldBlocks()[i].copyRegion(startFieldBlockOffset, fieldBlockLength);
        }

        int[] newOffsets = compactOffsets(getFieldBlockOffsets(), position + getOffsetBase(), length);
        boolean[] rowIsNull = getRowIsNull();
        boolean[] newRowIsNull = rowIsNull == null ? null : compactArray(rowIsNull, position + getOffsetBase(), length);

        if (arraySame(newBlocks, getRawFieldBlocks()) && newOffsets == getFieldBlockOffsets() && newRowIsNull == rowIsNull) {
            return this;
        }
        return createRowBlockInternal(0, length, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public Block getBlock(int position)
    {
        checkReadablePosition(position);

        return new SingleRowBlock(getFieldBlockOffset(position), getRawFieldBlocks());
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
            output.writeByte(1);
            int fieldBlockOffset = getFieldBlockOffset(position);
            Block[] fieldBlocks = getRawFieldBlocks();
            for (int i = 0; i < numFields; i++) {
                fieldBlocks[i].writePositionTo(fieldBlockOffset, output);
            }
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + 1);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;
        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = getRawFieldBlocks()[i].copyRegion(startFieldBlockOffset, fieldBlockLength);
        }
        boolean[] newRowIsNull = isNull(position) ? new boolean[] {true} : null;
        int[] newOffsets = new int[] {0, fieldBlockLength};

        return createRowBlockInternal(0, 1, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkReadablePosition(position);

        if (isNull(position)) {
            return 0;
        }

        Block[] rawFieldBlocks = getRawFieldBlocks();
        long size = 0;
        for (int i = 0; i < numFields; i++) {
            size += rawFieldBlocks[i].getEstimatedDataSizeForStats(getFieldBlockOffset(position));
        }
        return size;
    }

    @Override
    public boolean mayHaveNull()
    {
        return getRowIsNull() != null;
    }

    protected final void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public Block getBlockUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return new SingleRowBlock(getFieldBlockOffsets()[internalPosition], getRawFieldBlocks());
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return getRowIsNull()[internalPosition];
    }

    @Override
    public Block appendNull()
    {
        boolean[] rowIsNull = appendNullToIsNullArray(getRowIsNull(), getOffsetBase(), getPositionCount());
        int[] offsets = appendNullToOffsetsArray(getFieldBlockOffsets(), getOffsetBase(), getPositionCount());

        return createRowBlockInternal(getOffsetBase(), getPositionCount() + 1, rowIsNull, offsets, getRawFieldBlocks());
    }
}
