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

import com.facebook.presto.spi.block.array.BooleanArray;
import com.facebook.presto.spi.block.array.BooleanArrayHeap;
import com.facebook.presto.spi.block.array.IntArray;
import com.facebook.presto.spi.block.array.IntArrayHeap;
import com.facebook.presto.spi.block.array.LongArrayList;
import com.facebook.presto.spi.block.resource.BlockResourceContext;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class HeapBlockResourceContext
        implements BlockResourceContext
{

    @Override
    public BooleanArray newBooleanArray(int size)
    {
        return new BooleanArrayHeap(size);
    }

    @Override
    public BooleanArray copyOfRangeBooleanArray(BooleanArray booleanArray, int positionOffset, int length)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IntArray newIntArray(int size)
    {
        return new IntArrayHeap(size);
    }

    @Override
    public IntArray copyOfRangeIntArray(IntArray intArray, int positionOffset, int length)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Slice copyOfSlice(Slice slice, int offset, int length)
    {
        return Slices.copyOf(slice, offset, length);
    }

    @Override
    public Slice copyOfSlice(Slice slice)
    {
        return copyOfSlice(slice, 0, slice.length());
    }

    @Override
    public LongArrayList newLongArrayList(int expectedPositions)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Slice newSlice(int length)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
