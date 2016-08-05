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
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

public class LongArrayListSlice
        implements LongArrayList
{
    private static final int LONG_BYTES = 8;
    private final int expectedPositionsPerSlice;
    private final BlockResourceContext blockResourceContext;
    private final List<Slice> slices = new ArrayList<>();
    private int size;

    public LongArrayListSlice(int expectedPositions, BlockResourceContext blockResourceContext)
    {
        this.expectedPositionsPerSlice = expectedPositions;
        this.blockResourceContext = blockResourceContext;
    }

    @Override
    public void clear()
    {
        size = 0;
    }

    @Override
    public boolean add(long value)
    {
        int index = size;
        int sliceIndex = getSliceIndex(index);
        createSliceIfNeeded(sliceIndex);
        Slice slice = slices.get(sliceIndex);
        int internalSlicePosition = getInternalSlicePosition(index);
        slice.setLong(internalSlicePosition * LONG_BYTES, value);
        size++;
        return true;
    }

    @Override
    public long getLong(int index)
    {
        checkSize(index);
        int sliceIndex = getSliceIndex(index);
        Slice slice = slices.get(sliceIndex);
        int internalSlicePosition = getInternalSlicePosition(index);
        return slice.getLong(internalSlicePosition * LONG_BYTES);
    }

    @Override
    public void setLong(int index, long value)
    {
        checkSize(index);
        int sliceIndex = getSliceIndex(index);
        Slice slice = slices.get(sliceIndex);
        int internalSlicePosition = getInternalSlicePosition(index);
        slice.setLong(internalSlicePosition * LONG_BYTES, value);
    }

    @Override
    public long sizeOf()
    {
        long total = 0L;
        for (Slice slice : slices) {
            total += slice.getRetainedSize();
        }
        return total;
    }

    @Override
    public int size()
    {
        return size;
    }

    private int getInternalSlicePosition(int position)
    {
        return position % expectedPositionsPerSlice;
    }

    private int getSliceIndex(int position)
    {
        return position / expectedPositionsPerSlice;
    }

    private void createSliceIfNeeded(int desiredSliceIndex)
    {
        while (true) {
            if (desiredSliceIndex < slices.size()) {
                return;
            }
            slices.add(blockResourceContext.newSlice(expectedPositionsPerSlice * LONG_BYTES));
        }
    }

    private void checkSize(int index)
    {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index (" + index + ") is greater than or equal to list size (" + size + ")");
        }
    }

    @Override
    public long[] toLongArray()
    {
        long[] data = new long[size];
        for (int i = 0; i < size; i++) {
            data[i] = getLong(i);
        }
        return data;
    }

    @Override
    public BlockResourceContext getBlockResourceContext()
    {
        return blockResourceContext;
    }
}
