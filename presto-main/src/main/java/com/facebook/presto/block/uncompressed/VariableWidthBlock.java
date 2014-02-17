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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockEncoding;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.type.Type;
import com.facebook.presto.type.VariableWidthType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;

public class VariableWidthBlock
        implements Block
{
    private final int positionCount;
    private final VariableWidthType type;
    private final Slice slice;

    public VariableWidthBlock(VariableWidthType type, int positionCount, Slice slice)
    {
        Preconditions.checkArgument(positionCount >= 0, "positionCount is negative");
        Preconditions.checkNotNull(type, "type is null");
        Preconditions.checkNotNull(slice, "data is null");

        this.type = type;
        this.slice = slice;
        this.positionCount = positionCount;
    }

    public Type getType()
    {
        return type;
    }

    Slice getRawSlice()
    {
        return slice;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        return slice.length();
    }

    @Override
    public BlockCursor cursor()
    {
        return new VariableWidthBlockCursor(type, positionCount, slice);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new VariableWidthBlockEncoding(getType());
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        Preconditions.checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return cursor().getRegionAndAdvance(length);
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        return new VariableWidthRandomAccessBlock(type, positionCount, slice);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("type", type)
                .add("slice", slice)
                .toString();
    }
}
