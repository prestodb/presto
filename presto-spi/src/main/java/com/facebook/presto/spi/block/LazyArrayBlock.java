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
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

public class LazyArrayBlock
        extends AbstractArrayBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LazyArrayBlock.class).instanceSize();

    private Block values;
    private Slice offsets;
    private Slice valueIsNull;
    private int offsetBase = -1;
    private LazyBlockLoader<LazyArrayBlock> loader;

    public LazyArrayBlock(LazyBlockLoader<LazyArrayBlock> loader)
    {
        this.loader = Objects.requireNonNull(loader);
    }

    @Override
    public int getPositionCount()
    {
        assureLoaded();
        return valueIsNull.length();
    }

    @Override
    protected Block getValues()
    {
        assureLoaded();
        return values;
    }

    @Override
    protected Slice getOffsets()
    {
        assureLoaded();
        return offsets;
    }

    @Override
    protected int getOffsetBase()
    {
        assureLoaded();
        return offsetBase;
    }

    @Override
    protected Slice getValueIsNull()
    {
        assureLoaded();
        return valueIsNull;
    }

    @Override
    public void assureLoaded()
    {
        if (loader == null) {
            return;
        }
        loader.load(this);

        if (values == null || offsets == null || valueIsNull == null || offsetBase < 0) {
            throw new IllegalArgumentException("Lazy block loader did not load this block");
        }

        // clear reference to loader to free resources, since load was successful
        loader = null;
    }

    public void copyFromBlock(Block block)
    {
        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;
        this.values = arrayBlock.getValues();
        this.offsets = arrayBlock.getOffsets();
        this.offsetBase = arrayBlock.getOffsetBase();
        this.valueIsNull = arrayBlock.getValueIsNull();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("LazyArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append(", loaded=").append(loader == null);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int getSizeInBytes()
    {
        if (loader != null) {
            // This block hasn't been loaded. Return a value close to empty.
            return Integer.BYTES;
        }
        return getValues().getSizeInBytes() + offsets.length() + valueIsNull.length();
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        if (loader != null) {
            // This block hasn't been loaded. Return a value close to empty.
            return INSTANCE_SIZE;
        }
        return INSTANCE_SIZE + values.getRetainedSizeInBytes() + offsets.getRetainedSize() + valueIsNull.getRetainedSize();
    }
}
