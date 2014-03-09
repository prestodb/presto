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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.VariableWidthType;
import io.airlift.slice.Slice;

import static java.util.Objects.requireNonNull;

public class VariableWidthBlock
        implements Block
{
    private final int positionCount;
    private final VariableWidthType type;
    private final Slice slice;

    public VariableWidthBlock(VariableWidthType type, int positionCount, Slice slice)
    {
        this.type = requireNonNull(type, "type is null");

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        this.slice = requireNonNull(slice, "data is null");
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
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }
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
        StringBuilder sb = new StringBuilder("VariableWidthBlock{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", type=").append(type);
        sb.append(", slice=").append(slice);
        sb.append('}');
        return sb.toString();
    }
}
