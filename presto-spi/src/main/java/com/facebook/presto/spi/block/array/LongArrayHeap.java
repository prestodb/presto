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
package com.facebook.presto.spi.block.array;

import io.airlift.slice.SizeOf;

public class LongArrayHeap
        implements LongArray
{
    private final int length;
    private final long[] array;
    private final long retainedSize;

    public LongArrayHeap(int length)
    {
        this.length = length;
        this.array = new long[length];
        retainedSize = SizeOf.sizeOf(array);
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public long get(int position)
    {
        return array[position];
    }

    @Override
    public long getRetainedSize()
    {
        return retainedSize;
    }

    @Override
    public void set(int position, long value)
    {
        array[position] = value;
    }
}
