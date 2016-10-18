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
package com.facebook.presto.rcfile;

import io.airlift.slice.Slice;

public final class ColumnData
{
    public static final int MAX_SIZE = 1024;

    private final int[] offset;
    private final Slice slice;

    public ColumnData(int[] offset, Slice slice)
    {
        this.offset = offset;
        this.slice = slice;
    }

    public int rowCount()
    {
        return offset.length - 1;
    }

    public Slice getSlice()
    {
        return slice;
    }

    public Slice getSlice(int position)
    {
        return slice.slice(getOffset(position), getLength(position));
    }

    public int getOffset(int position)
    {
        return offset[position];
    }

    public int getLength(int position)
    {
        return offset[position + 1] - offset[position];
    }
}
