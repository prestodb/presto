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

public class VariableWidthRandomAccessBlock
        extends AbstractVariableWidthRandomAccessBlock
{
    private final int positionCount;
    private final Slice slice;
    private final int[] offsets;

    public VariableWidthRandomAccessBlock(VariableWidthType type, int positionCount, Slice slice, int[] offsets)
    {
        super(type);

        this.positionCount = positionCount;
        this.slice = slice;
        this.offsets = offsets;
    }

    public VariableWidthRandomAccessBlock(VariableWidthType type, int positionCount, Slice slice)
    {
        super(type);

        this.positionCount = positionCount;
        this.slice = slice;
        this.offsets = new int[positionCount];

        VariableWidthBlockCursor cursor = new VariableWidthBlockCursor(type, positionCount, slice);
        for (int position = 0; position < positionCount; position++) {
            if (!cursor.advanceNextPosition()) {
                throw new IllegalStateException();
            }
            offsets[position] = cursor.getRawOffset();
        }
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
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthRandomAccessBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append(", slice=").append(getRawSlice());
        sb.append('}');
        return sb.toString();
    }
}
