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
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.tuple.VariableWidthTypeInfo;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkState;

public class VariableWidthRandomAccessBlock
        extends AbstractVariableWidthRandomAccessBlock
{
    private final Slice slice;
    private final int[] offsets;

    public VariableWidthRandomAccessBlock(VariableWidthTypeInfo typeInfo, Slice slice, int[] offsets)
    {
        super(typeInfo);

        this.slice = slice;
        this.offsets = offsets;
    }

    public VariableWidthRandomAccessBlock(VariableWidthTypeInfo typeInfo, int positionCount, Slice slice)
    {
        super(typeInfo);

        this.slice = slice;
        this.offsets = new int[positionCount];

        VariableWidthBlockCursor cursor = new VariableWidthBlockCursor(typeInfo, getPositionCount(), slice);
        for (int position = 0; position < positionCount; position++) {
            checkState(cursor.advanceNextPosition());
            offsets[position] = cursor.getRawOffset();
        }
    }

    protected int getPositionOffset(int position)
    {
        return offsets[position];
    }

    @Override
    public int getPositionCount()
    {
        return offsets.length;
    }

    @Override
    public Slice getRawSlice()
    {
        return slice;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", getPositionCount())
                .add("slice", getRawSlice())
                .toString();
    }
}
