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

import com.facebook.presto.spi.type.FixedWidthType;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.Objects;

public class FixedWidthBlock
        extends AbstractFixedWidthBlock
{
    private final int positionCount;
    private final Slice slice;
    private final boolean[] valueIsNull;

    public FixedWidthBlock(FixedWidthType type, int positionCount, Slice slice, boolean[] valueIsNull)
    {
        super(type);

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        this.slice = Objects.requireNonNull(slice, "slice is null");

        if (valueIsNull.length < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;
    }

    @Override
    protected Slice getRawSlice()
    {
        return slice;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull[position];
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        long size = getRawSlice().length() + SizeOf.sizeOf(valueIsNull);
        if (size > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) size;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        Slice newSlice = slice.slice(positionOffset * fixedSize, length * fixedSize);
        boolean[] newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length);
        return new FixedWidthBlock(type, length, newSlice, newValueIsNull);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("FixedWidthBlock{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", slice=").append(slice);
        sb.append('}');
        return sb.toString();
    }
}
