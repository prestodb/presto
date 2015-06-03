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

import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.spi.block.SliceArrayBlock.deepCopyAndCompact;
import static com.facebook.presto.spi.block.SliceArrayBlock.getSliceArraySizeInBytes;

public class LazySliceArrayBlock
        extends AbstractVariableWidthBlock
{
    private final int positionCount;
    private LazyBlockLoader<LazySliceArrayBlock> loader;
    private Slice[] values;
    private final AtomicInteger sizeInBytes = new AtomicInteger(-1);

    public LazySliceArrayBlock(int positionCount, LazyBlockLoader<LazySliceArrayBlock> loader)
    {
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;
        this.loader = Objects.requireNonNull(loader);
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
        int sizeInBytes = this.sizeInBytes.get();
        if (sizeInBytes < 0) {
            assureLoaded();
            sizeInBytes = getSliceArraySizeInBytes(values);
            this.sizeInBytes.set(sizeInBytes);
        }
        return sizeInBytes;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        // TODO: This should account for memory used by the loader.
        return getSizeInBytes();
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
    public Block copyRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        assureLoaded();
        return new SliceArrayBlock(length, deepCopyAndCompact(values, positionOffset, length));
    }

    @Override
    public void assureLoaded()
    {
        if (values != null) {
            return;
        }
        loader.load(this);

        if (values == null) {
            throw new IllegalArgumentException("Lazy block loader did not load this block");
        }

        // clear reference to loader to free resources, since load was successful
        loader = null;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("LazySliceArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }
}
