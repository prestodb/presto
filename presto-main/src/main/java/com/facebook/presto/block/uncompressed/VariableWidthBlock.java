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
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.VariableWidthTypeInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

public class VariableWidthBlock
        implements Block
{
    private final int positionCount;
    private final VariableWidthTypeInfo typeInfo;
    private final Slice slice;

    public VariableWidthBlock(VariableWidthTypeInfo typeInfo, int positionCount, Slice slice)
    {
        Preconditions.checkArgument(positionCount >= 0, "positionCount is negative");
        Preconditions.checkNotNull(typeInfo, "typeInfo is null");
        Preconditions.checkNotNull(slice, "data is null");

        this.typeInfo = typeInfo;
        this.slice = slice;
        this.positionCount = positionCount;
    }

    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(typeInfo.getType());
    }

    public Slice getRawSlice()
    {
        return slice;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public DataSize getDataSize()
    {
        return new DataSize(slice.length(), Unit.BYTE);
    }

    @Override
    public BlockCursor cursor()
    {
        return new VariableWidthBlockCursor(typeInfo, positionCount, slice);
    }

    @Override
    public UncompressedBlockEncoding getEncoding()
    {
        return new UncompressedBlockEncoding(getTupleInfo());
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
        return new VariableWidthRandomAccessBlock(typeInfo, positionCount, slice);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("tupleInfo", typeInfo)
                .add("slice", slice)
                .toString();
    }
}
