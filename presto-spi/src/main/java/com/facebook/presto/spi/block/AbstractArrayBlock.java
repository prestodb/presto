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
import java.util.Arrays;
import java.util.List;

public abstract class AbstractArrayBlock
        implements Block
{
    protected abstract Block getValues();

    protected abstract int[] getOffsets();

    protected abstract int getOffsetBase();

    protected abstract boolean[] getValueIsNull();

    private int getOffset(int position)
    {
        return getOffsets()[position + getOffsetBase()];
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new ArrayBlockEncoding(getValues().getEncoding());
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        int[] newOffsets = new int[positions.size() + 1];
        boolean[] newValueIsNull = new boolean[positions.size()];

        List<Integer> valuesPositions = new ArrayList<>();
        int newPosition = 0;
        for (int position : positions) {
            if (isNull(position)) {
                newValueIsNull[newPosition] = true;
                newOffsets[newPosition + 1] = newOffsets[newPosition];
            }
            else {
                int valuesStartOffset = getOffset(position);
                int valuesEndOffset = getOffset(position + 1);
                int valuesLength = valuesEndOffset - valuesStartOffset;

                newOffsets[newPosition + 1] = newOffsets[newPosition] + valuesLength;

                for (int elementIndex = valuesStartOffset; elementIndex < valuesEndOffset; elementIndex++) {
                    valuesPositions.add(elementIndex);
                }
            }
            newPosition++;
        }
        Block newValues = getValues().copyPositions(valuesPositions);
        return new ArrayBlock(positions.size(), newValueIsNull, newOffsets, newValues);
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

        return new ArrayBlock(
                position + getOffsetBase(),
                length,
                getValueIsNull(),
                getOffsets(),
                getValues());
    }

    @Override
    public int getRegionSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        if (position < 0 || length < 0 || position + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + position + " in block with " + positionCount + " positions");
        }

        int valueStart = getOffsets()[getOffsetBase() + position];
        int valueEnd = getOffsets()[getOffsetBase() + position + length];

        return getValues().getRegionSizeInBytes(valueStart, valueEnd - valueStart) + ((Integer.BYTES + Byte.BYTES) * length);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        if (position < 0 || length < 0 || position + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + position + " in block with " + positionCount + " positions");
        }

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + length);
        Block newValues = getValues().copyRegion(startValueOffset, endValueOffset - startValueOffset);

        int[] newOffsets = new int[length + 1];
        for (int i = 1; i < newOffsets.length; i++) {
            newOffsets[i] = getOffset(position + i) - startValueOffset;
        }

        boolean[] newValueIsNull = Arrays.copyOfRange(getValueIsNull(), position + getOffsetBase(), position + getOffsetBase() + length);

        return new ArrayBlock(length, newValueIsNull, newOffsets, newValues);
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
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        int startValueOffset = getOffset(position);
        int valueLength = getOffset(position + 1) - startValueOffset;
        Block newValues = getValues().copyRegion(startValueOffset, valueLength);

        return new ArrayBlock(
                1,
                new boolean[] {isNull(position)},
                new int[] {0, valueLength},
                newValues);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return getValueIsNull()[position + getOffsetBase()];
    }

    public <T> T apply(ArrayBlockFunction<T> function, int position)
    {
        checkReadablePosition(position);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);
        return function.apply(getValues(), startValueOffset, endValueOffset - startValueOffset);
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    public interface ArrayBlockFunction<T>
    {
        T apply(Block block, int startPosition, int length);
    }
}
