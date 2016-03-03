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

import static java.util.Objects.requireNonNull;

public class ArrayBlock
        extends AbstractArrayBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayBlock.class).instanceSize();

    private final Block values;
    private final Slice offsets;
    private final int offsetBase;
    private final Slice valueIsNull;

    public ArrayBlock(Block values, Slice offsets, int offsetBase, Slice valueIsNull)
    {
        this.values = requireNonNull(values);
        this.offsets = requireNonNull(offsets);
        this.offsetBase = offsetBase;
        this.valueIsNull = requireNonNull(valueIsNull);
    }

    @Override
    public int getPositionCount()
    {
        return valueIsNull.length();
    }

    @Override
    public int getSizeInBytes()
    {
        return getValues().getSizeInBytes() + offsets.length() + valueIsNull.length();
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + values.getRetainedSizeInBytes() + offsets.getRetainedSize() + valueIsNull.getRetainedSize();
    }

    @Override
    protected Block getValues()
    {
        return values;
    }

    @Override
    protected Slice getOffsets()
    {
        return offsets;
    }

    @Override
    protected int getOffsetBase()
    {
        return offsetBase;
    }

    @Override
    protected Slice getValueIsNull()
    {
        return valueIsNull;
    }

    @Override
    public void assureLoaded()
    {
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }
}
