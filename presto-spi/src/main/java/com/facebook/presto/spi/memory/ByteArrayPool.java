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

public class ByteArrayPool
{
    private int[] sizes;
    private ArrayList<byte[]>[] contents;
    private LongArrayList[] times;
    private long[] hits;
    private long[] misses;
    private AtomicLong totalSize = new AtomicLong();
    private long capacity;

    public ByteArrayPool(int smallestSize, int largestSize, long capacity)
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
    }

    public byte[] getBytes(int size)
    {
        int idx = getSizeIndex(size);
        if (idx >= sizes.length) {
            return new byte[size];
        }
        ArrayList<byte[]> list = contents[idx];
        byte[] data = null;
        synchronized (list) {
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
            return data;
        }
        misses[idx]++;
        return new byte[sizes[idx]];
    }

    public void release(byte[] data)
    {
        int idx = getSizeIndex(data.length);
        if (idx < sizes.length) {
            int addedSize = sizes[idx];
            long now = System.nanoTime();
            ArrayList<byte[]> list = contents[idx];
            long newSize = totalSize.getAndAdd(addedSize);
            if (sizes[idx] != data.length) {
                throw new IllegalArgumentException("Wrongf size for ByteArrayPool release");
            }
            synchronized (list) {
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

    class LongArrayList
    {
        private long[] longs = new long[10];
        private int size;

        void add(long value)
        {
            if (longs.length <= size) {
                longs = Arrays.copyOf(longs, 2 * size);
            }
            longs[size++] = value;
        }

        int size()
        {
            return size;
        }

        long get(int i)
        {
            return longs[i];
        }

        void popBack()
        {
            size--;
        }
    }
}
