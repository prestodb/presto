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

import io.airlift.slice.Slice;

//@TODO make bit based not byte based.
public class BooleanArraySlice
        implements BooleanArray
{

    private final long retainedSize;
    private final int length;
    private final Slice slice;

    public BooleanArraySlice(Slice slice)
    {
        this.slice = slice;
        this.length = slice.length();
        this.retainedSize = slice.getRetainedSize();
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public boolean get(int position)
    {
        return slice.getByte(position) != 0;
    }

    @Override
    public long getRetainedSize()
    {
        return retainedSize;
    }

    @Override
    public void set(int position, boolean value)
    {
        slice.setByte(position, value ? 1 : 0);
    }

}
