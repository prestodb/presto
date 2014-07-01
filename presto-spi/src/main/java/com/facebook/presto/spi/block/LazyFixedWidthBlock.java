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

public class LazyFixedWidthBlock
        extends AbstractFixedWidthBlock
{
    private final int positionCount;
    private final LazyFixedWidthBlockLoader loader;
    private Slice slice;
    private boolean[] valueIsNull;

    public LazyFixedWidthBlock(int fixedSize, int positionCount, LazyFixedWidthBlockLoader loader)
    {
        super(fixedSize);

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (loader == null) {
            throw new IllegalArgumentException("loader is null");
        }
        this.loader = loader;
    }

    LazyFixedWidthBlock(int fixedSize, int positionCount, LazyFixedWidthBlockLoader loader, Slice slice, boolean[] valueIsNull)
    {
        super(fixedSize);
        this.positionCount = positionCount;
        this.loader = loader;
        this.slice = slice;
        this.valueIsNull = valueIsNull;
    }

    @Override
    protected Slice getRawSlice()
    {
        assureLoaded();
        return slice;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        assureLoaded();
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
        long size = (positionCount * fixedSize) + SizeOf.sizeOf(valueIsNull);
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

        assureLoaded();
        Slice newSlice = slice.slice(positionOffset * fixedSize, length * fixedSize);
        return new LazyFixedWidthBlock(fixedSize, length, loader, newSlice, Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset + length));
    }

    @Override
    public void assureLoaded()
    {
        if (slice != null) {
            return;
        }
        loader.load(this);
    }

    public void setRawSlice(Slice slice)
    {
        if (slice.length() < positionCount * fixedSize) {
            throw new IllegalArgumentException("slice is not large enough to hold all positions");
        }
        this.slice = slice;
    }

    public void setNullVector(boolean[] valueIsNull)
    {
        if (valueIsNull.length < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("FixedWidthBlock{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", slice=").append(slice == null ? "not loaded" : slice);
        sb.append('}');
        return sb.toString();
    }

    public interface LazyFixedWidthBlockLoader
    {
        void load(LazyFixedWidthBlock block);
    }
}
