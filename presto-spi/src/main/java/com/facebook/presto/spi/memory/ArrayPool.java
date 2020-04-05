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
package com.facebook.presto.spi.memory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class ArrayPool<ArrayType>
{
    public static abstract class Allocator<ArrayType>
    {
        abstract ArrayType alloc(int size);
        abstract void init(ArrayType array);
        abstract int getLength(ArrayType array);
        void clear(ArrayType array)
        {
        }
    }

    private int[] sizes;
    private ArrayList<ArrayType>[] contents;
    private LongArrayList[] times;
    private long[] hits;
    private long[] misses;
    private AtomicLong totalSize = new AtomicLong();
    private long capacity;
    Allocator<ArrayType> allocator;

    public ArrayPool(int smallestSize, int largestSize, long capacity, Allocator<ArrayType> allocator)
    {
        int size = smallestSize;
        ArrayList<Integer> sizeList = new ArrayList();
        while (size < largestSize) {
            sizeList.add(size);
            sizeList.add(size + (size / 2));
            size *= 2;
        }
        sizeList.add(largestSize);

        this.capacity = capacity;

        sizes = sizeList.stream().mapToInt(Integer::intValue).toArray();
        misses = new long[sizes.length];
        hits = new long[sizes.length];
        contents = new ArrayList[sizes.length];
        times = new LongArrayList[sizes.length];
        for (int i = 0; i < sizes.length; i++) {
            contents[i] = new ArrayList();
            times[i] = new LongArrayList();
        }
        this.allocator = allocator;
    }

    public ArrayType alloc(int size)
    {
        return alloc(size, false);
    }

    public ArrayType allocInitialized(int size)
    {
        return alloc(size, true);
    }

    public ArrayType alloc(int size, boolean init)
    {
        int idx = getSizeIndex(size);
        if (idx >= sizes.length) {
            return allocator.alloc(size);
        }
        ArrayList<ArrayType> list = contents[idx];
        ArrayType data = null;
        synchronized(list) {
            int count = list.size();
            if (count > 0) {
                data = list.get(count - 1);
                list.remove(count - 1);
                times[idx].popBack();
            }
        }
        if (data != null) {
            totalSize.getAndAdd(-sizes[idx]);
            hits[idx]++;
            if (init) {
                allocator.init(data);
            }
            return data;
        }
        misses[idx]++;
        return allocator.alloc(sizes[idx]);
    }

    public void release(ArrayType data)
    {
        int idx = getSizeIndex(allocator.getLength(data));
        if (idx < sizes.length) {
            int addedSize = sizes[idx];
            long now = System.nanoTime();
            ArrayList<ArrayType> list = contents[idx];
            long newSize = totalSize.getAndAdd(addedSize);
            if (sizes[idx] != allocator.getLength(data)) {
                throw new IllegalArgumentException("Wrongf size for ArrayPool release");
            }
            synchronized (list) {
                /*
                for (byte[] existing : list) {
                    if (existing == data) {
                        throw new IllegalArgumentException("Duplicate release in ArrayPool");
                    }
                }
                */
                list.add(data);
                times[idx].add(now);
            }
            if (newSize + addedSize > capacity) {
                trim();
            }
        }
    }

    private int getSizeIndex(int size)
    {
        int idx = Arrays.binarySearch(sizes, size);
        return idx < 0 ? -1 - idx : idx;
    }

    private void trim()
    {
        long totalTime = 0;
        long now = System.nanoTime();
        while (totalSize.get() > capacity) {

            for (int idx = 0; idx < sizes.length; idx++) {
                LongArrayList ages = times[idx];
                for (int i = 0; i < ages.size(); i++) {
                    totalTime += ages.get(i) - now;
                }
            }
            break;
        }
    }
}