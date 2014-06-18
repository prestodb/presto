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

import com.facebook.presto.spi.type.VariableWidthType;
import io.airlift.slice.Slice;

import java.util.Arrays;

public class VariableWidthBlock
        extends AbstractVariableWidthBlock
{
    private final int positionCount;
    private final Slice slice;
    private final int[] offsets;

    public VariableWidthBlock(VariableWidthType type, int positionCount, Slice slice, int[] offsets)
    {
        super(type);

        this.positionCount = positionCount;
        this.slice = slice;
        this.offsets = offsets;
    }

    @Override
    protected int[] getOffsets()
    {
        return offsets;
    }

    @Override
    protected final int getPositionOffset(int position)
    {
        return offsets[position];
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    protected Slice getRawSlice()
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

        int[] newOffsets = Arrays.copyOfRange(offsets, positionOffset, positionOffset + length);
        return new VariableWidthBlock(type, length, slice, newOffsets);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthRandomAccessBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append(", slice=").append(getRawSlice());
        sb.append('}');
        return sb.toString();
    }
}
