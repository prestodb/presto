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
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.array.LongArrayList;
import com.facebook.presto.spi.block.resource.BlockResourceContext;
import io.airlift.slice.SizeOf;

public class LongArrayListNative
        implements LongArrayList
{
    private final it.unimi.dsi.fastutil.longs.LongArrayList internal;

    public LongArrayListNative()
    {
        internal = new it.unimi.dsi.fastutil.longs.LongArrayList();
    }

    public LongArrayListNative(int expectedPositions)
    {
        internal = new it.unimi.dsi.fastutil.longs.LongArrayList(expectedPositions);
    }

    @Override
    public void clear()
    {
        internal.clear();
    }

    @Override
    public boolean add(long value)
    {
        return internal.add(value);
    }

    @Override
    public long getLong(int index)
    {
        return internal.getLong(index);
    }

    @Override
    public void setLong(int index, long value)
    {
        internal.set(index, value);
    }

    @Override
    public long sizeOf()
    {
        return SizeOf.sizeOf(internal.elements());
    }

    @Override
    public int size()
    {
        return internal.size();
    }

    @Override
    public long[] toLongArray()
    {
        return internal.toLongArray(null);
    }

    @Override
    public BlockResourceContext getBlockResourceContext()
    {
        return new HeapBlockResourceContext();
    }
}
