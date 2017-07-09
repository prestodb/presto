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

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.block.BlockUtil.arraySame;
import static com.facebook.presto.spi.block.BlockUtil.compactArray;
import static com.facebook.presto.spi.block.BlockUtil.compactOffsets;

public abstract class AbstractRowBlock
        implements Block
{
    protected final int numFields;

    protected abstract Block[] getFieldBlocks();

    protected abstract int[] getFieldBlockOffsets();

    protected abstract int getOffsetBase();

    protected abstract boolean[] getRowIsNull();

    // the offset in each field block, it can also be viewed as the "entry-based" offset in the RowBlock
    protected int getFieldBlockOffset(int position)
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
    public RowBlockEncoding getEncoding()
    {
        BlockEncoding[] fieldBlockEncodings = new BlockEncoding[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldBlockEncodings[i] = getFieldBlocks()[i].getEncoding();
        }
        return new RowBlockEncoding(fieldBlockEncodings);
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        int newPositionCount = positions.size();
        int[] newOffsets = new int[newPositionCount + 1];
        boolean[] newRowIsNull = new boolean[newPositionCount];

        List<Integer> fieldBlockPositions = new ArrayList<>(newPositionCount);
        for (int i = 0; i < newPositionCount; i++) {
            int position = positions.get(i);
            if (isNull(position)) {
                newRowIsNull[i] = true;
                newOffsets[i + 1] = newOffsets[i];
            }
            else {
                newOffsets[i + 1] = newOffsets[i] + 1;
                fieldBlockPositions.add(getFieldBlockOffset(position));
            }
        }

        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = getFieldBlocks()[i].copyPositions(fieldBlockPositions);
        }
        return new RowBlock(0, positions.size(), newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public Block getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        if (position < 0 || length < 0 || position + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + position + " in block with " + positionCount + " positions");
        }

        if (position == 0 && length == positionCount) {
            return this;
        }

        return new RowBlock(position + getOffsetBase(), length, getRowIsNull(), getFieldBlockOffsets(), getFieldBlocks());
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        if (position < 0 || length < 0 || position + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + position + " in block with " + positionCount + " positions");
        }

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + length);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;

        long regionSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) length;
        for (int i = 0; i < numFields; i++) {
            regionSizeInBytes += getFieldBlocks()[i].getRegionSizeInBytes(startFieldBlockOffset, fieldBlockLength);
        }
        return regionSizeInBytes;
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        if (position < 0 || length < 0 || position + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + position + " in block with " + positionCount + " positions");
        }

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + length);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;
        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = getFieldBlocks()[i].copyRegion(startFieldBlockOffset, fieldBlockLength);
        }

        int[] newOffsets = compactOffsets(getFieldBlockOffsets(), position + getOffsetBase(), length);
        boolean[] newRowIsNull = compactArray(getRowIsNull(), position + getOffsetBase(), length);

        if (arraySame(newBlocks, getFieldBlocks()) && newOffsets == getFieldBlockOffsets() && newRowIsNull == getRowIsNull()) {
            return this;
        }
        return new RowBlock(0, length, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        if (clazz != Block.class) {
            throw new IllegalArgumentException("clazz must be Block.class");
        }
        checkReadablePosition(position);

        return clazz.cast(new SingleRowBlock(getFieldBlockOffset(position) * numFields, getFieldBlocks()));
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
        int fieldBlockOffset = getFieldBlockOffset(position);
        for (int i = 0; i < numFields; i++) {
            if (getFieldBlocks()[i].isNull(fieldBlockOffset)) {
                entryBuilder.appendNull();
            }
            else {
                getFieldBlocks()[i].writePositionTo(fieldBlockOffset, entryBuilder);
                entryBuilder.closeEntry();
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
            newBlocks[i] = getFieldBlocks()[i].copyRegion(startFieldBlockOffset, fieldBlockLength);
        }
        boolean[] newRowIsNull = new boolean[] {isNull(position)};
        int[] newOffsets = new int[] {0, fieldBlockLength};

        return new RowBlock(0, 1, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return getRowIsNull()[position + getOffsetBase()];
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
