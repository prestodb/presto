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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractArrayBlock
        implements Block
{
    protected abstract Block getValues();

    protected abstract Slice getOffsets();

    protected abstract int getOffsetBase();

    protected abstract Slice getValueIsNull();

    @Override
    public BlockEncoding getEncoding()
    {
        return new ArrayBlockEncoding(getValues().getEncoding());
    }

    private int getOffset(int position)
    {
        return position == 0 ? 0 : getOffsets().getInt((position - 1) * 4) - getOffsetBase();
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        SliceOutput newOffsets = Slices.allocate(positions.size() * Integer.BYTES).getOutput();
        SliceOutput newValueIsNull = Slices.allocate(positions.size()).getOutput();

        List<Integer> valuesPositions = new ArrayList<>();
        int countNewOffset = 0;
        for (int position : positions) {
            if (isNull(position)) {
                newValueIsNull.appendByte(1);
                newOffsets.appendInt(countNewOffset);
            }
            else {
                newValueIsNull.appendByte(0);
                int positionStartOffset = getOffset(position);
                int positionEndOffset = getOffset(position + 1);
                countNewOffset += positionEndOffset - positionStartOffset;
                newOffsets.appendInt(countNewOffset);
                for (int j = positionStartOffset; j < positionEndOffset; j++) {
                    valuesPositions.add(j);
                }
            }
        }
        Block newValues = getValues().copyPositions(valuesPositions);
        return new ArrayBlock(newValues, newOffsets.slice(), 0, newValueIsNull.slice());
    }

    @Override
    public Block getRegion(int position, int length)
    {
        return getRegion(position, length, false);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        return getRegion(position, length, true);
    }

    private Block getRegion(int position, int length, boolean compact)
    {
        int positionCount = getPositionCount();
        if (position < 0 || length < 0 || position + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + position + " in block with " + positionCount + " positions");
        }

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + length);
        Block newValues;
        Slice newOffsets;
        Slice newValueIsNull;
        int newOffsetBase;
        if (compact) {
            newValues = getValues().copyRegion(startValueOffset, endValueOffset - startValueOffset);
            int[] newOffsetsArray = new int[length];
            for (int i = 0; i < length; i++) {
                newOffsetsArray[i] = getOffset(position + i + 1) - getOffset(position);
            }
            newOffsets = Slices.wrappedIntArray(newOffsetsArray);
            newValueIsNull = Slices.copyOf(getValueIsNull(), position, length);
            newOffsetBase = 0;
        }
        else {
            if (position == 0 && length == positionCount) {
                // It is incorrect to pull up this `if` because child blocks may be compact-able when this if condition is satisfied
                return this;
            }
            else {
                newValues = getValues().getRegion(startValueOffset, endValueOffset - startValueOffset);
                newOffsets = getOffsets().slice(position * 4, length * 4);
                newValueIsNull = getValueIsNull().slice(position, length);
                newOffsetBase = startValueOffset + getOffsetBase();
            }
        }
        return new ArrayBlock(newValues, newOffsets, newOffsetBase, newValueIsNull);
    }

    @Override
    public int getLength(int position)
    {
        return getOffset(position + 1) - getOffset(position);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        if (clazz != Block.class) {
            throw new IllegalArgumentException("clazz must be Block.class");
        }
        checkReadablePosition(position);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);
        return clazz.cast(getValues().getRegion(startValueOffset, endValueOffset - startValueOffset));
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);
        for (int i = startValueOffset; i < endValueOffset; i++) {
            if (getValues().isNull(i)) {
                entryBuilder.appendNull();
            }
            else {
                getValues().writePositionTo(i, entryBuilder);
                entryBuilder.closeEntry();
            }
        }
    }

    @Override
    public byte getByte(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int position, int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);

        Block newValues = getValues().copyRegion(startValueOffset, endValueOffset - startValueOffset);

        // rewrite offsets so that baseOffset is always zero
        Slice newOffsets = Slices.wrappedIntArray(endValueOffset - startValueOffset);

        Slice newValueIsNull = Slices.copyOf(getValueIsNull(), position, 1);

        return new ArrayBlock(newValues, newOffsets, 0, newValueIsNull);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return getValueIsNull().getByte(position) != 0;
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
