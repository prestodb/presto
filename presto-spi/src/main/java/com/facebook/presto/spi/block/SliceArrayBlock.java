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

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;

import java.util.Arrays;

public class SliceArrayBlock
        extends AbstractVariableWidthBlock
{
    private final int positionCount;
    private final Slice[] values;

    public SliceArrayBlock(int positionCount, Slice[] values)
    {
        this.positionCount = positionCount;

        if (values.length < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;
    }

    Slice[] getValues()
    {
        return values;
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        return values[position];
    }

    @Override
    protected int getPositionOffset(int position)
    {
        return 0;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return values[position] == null;
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new SliceArrayBlockEncoding();
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getLength(int position)
    {
        return values[position].length();
    }

    @Override
    public int getSizeInBytes()
    {
        // todo how to include the size of the distinct slice instances
        long size = SizeOf.sizeOf(values);
        if (size > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) size;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        Slice[] newValues = Arrays.copyOfRange(values, positionOffset, positionOffset + length);
        return new SliceArrayBlock(length, newValues);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("SliceArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }
}
