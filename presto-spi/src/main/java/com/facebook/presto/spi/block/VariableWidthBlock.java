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

import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class VariableWidthBlock
        extends AbstractVariableWidthBlock
{
    private final int positionCount;
    private final Slice slice;
    private final Slice offsets;
    private final Slice valueIsNull;

    public VariableWidthBlock(int positionCount, Slice slice, Slice offsets, Slice valueIsNull)
    {
        this.positionCount = positionCount;
        this.slice = slice;

        if (offsets.length() < (positionCount + 1) * SIZE_OF_INT) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        this.offsets = offsets;

        if (valueIsNull.length() < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;
    }

    @Override
    protected final int getPositionOffset(int position)
    {
        return offsets.getInt(position * SIZE_OF_INT);
    }

    @Override
    public int getLength(int position)
    {
        return getPositionOffset(position + 1) - getPositionOffset(position);
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull.getByte(position) != 0;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        long size = slice.length() + offsets.length() + valueIsNull.length();
        if (size > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) size;
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        return slice;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        Slice newOffsets = offsets.slice(positionOffset * SIZE_OF_INT, (length + 1) * SIZE_OF_INT);
        Slice newValueIsNull = valueIsNull.slice(positionOffset, length);
        return new VariableWidthBlock(length, slice, newOffsets, newValueIsNull);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append(", slice=").append(slice);
        sb.append('}');
        return sb.toString();
    }
}
