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

public class LazySliceArrayBlock
        extends AbstractVariableWidthBlock
{
    private final int positionCount;
    private final LazySliceArrayBlockLoader loader;
    private Slice[] values;

    public LazySliceArrayBlock(int positionCount, LazySliceArrayBlockLoader loader)
    {
        this.positionCount = positionCount;
        this.loader = loader;
    }

    Slice[] getValues()
    {
        assureLoaded();
        return values;
    }

    public void setValues(Slice[] values)
    {
        this.values = values;
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new LazySliceArrayBlockEncoding();
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        assureLoaded();
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
        assureLoaded();
        return values[position] == null;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getLength(int position)
    {
        assureLoaded();
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

        assureLoaded();
        Slice[] newValues = Arrays.copyOfRange(values, positionOffset, positionOffset + length);
        return new SliceArrayBlock(length, newValues);
    }

    @Override
    public void assureLoaded()
    {
        if (values == null) {
            loader.load(this);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("LazySliceArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    public interface LazySliceArrayBlockLoader
    {
        void load(LazySliceArrayBlock block);
    }
}
