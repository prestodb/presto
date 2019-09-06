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

import com.facebook.presto.spi.PrestoException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ArrayPool<T>
{
    public abstract static class Allocator<T>
    {
        abstract T allocate(int size);

        abstract void initialize(T array);

        abstract int getSize(T array);
    }

    private static class LongArrayList
    {
        private static final int INITIAL_SIZE = 10;
        private long[] longs = new long[INITIAL_SIZE];

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
            if (i >= size) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("LongArrayList index out of bound %d", i));
            }
            return longs[i];
        }

        void popBack()
        {
            size--;
        }

        // Remove elements from position 0 to index - 1
        void removeFront(int index)
        {
            verify(index >= 0 && index <= size, format("LongArrayList index %d is out of bound", index));
            size -= index;
            if (size > index) {
                System.arraycopy(longs, index, longs, 0, size - index);
            }
        }
    }

    private static class PoolStats
    {
        private long totalSize;
        private long objectCount;

        PoolStats(long totalSize, long objectCount)
        {
            this.totalSize = totalSize;
            this.objectCount = objectCount;
        }
    }

    // The fraction of objects that will be sampled to calculate average age during trimming. 2 means 1/2 objects will be sampled.
    private static final int SAMPLING_FRACTION = 2;

    private final int[] arrayObjectSizes;
    private ArrayList<T>[] arrayObjects;
    private LongArrayList[] timeAdded;
    private final Allocator<T> allocator;
    private long[] hits;
    private long[] misses;
    private AtomicReference<PoolStats> poolStats;
    private long capacity;
    private AtomicBoolean trimming;

    public ArrayPool(int smallestSize, int largestSize, long capacity, Allocator<T> allocator)
    {
        ArrayList<Integer> sizeList = getSizeList(smallestSize, largestSize);
        arrayObjectSizes = sizeList.stream().mapToInt(Integer::intValue).toArray();
        arrayObjects = new ArrayList[arrayObjectSizes.length];
        timeAdded = new LongArrayList[arrayObjectSizes.length];
        for (int i = 0; i < arrayObjectSizes.length; i++) {
            arrayObjects[i] = new ArrayList();
            timeAdded[i] = new LongArrayList();
        }
        this.allocator = requireNonNull(allocator, "allocator is null");
        misses = new long[arrayObjectSizes.length];
        hits = new long[arrayObjectSizes.length];
        this.capacity = capacity;
        poolStats = new AtomicReference(new PoolStats(0, 0));
        trimming = new AtomicBoolean();
    }

    public T allocate(int size)
    {
        return allocate(size, false);
    }

    public T allocateAndInitialize(int size)
    {
        return allocate(size, true);
    }

    public int getStandardSize(int size)
    {
        int idx = getSizeIndex(size);
        return idx >= arrayObjectSizes.length ? size : arrayObjectSizes[idx];
    }

    public int[] getStandardSizes()
    {
        return arrayObjectSizes;
    }

    private T allocate(int size, boolean init)
    {
        int idx = getSizeIndex(size);
        if (idx >= arrayObjectSizes.length) {
            return allocator.allocate(size);
        }

        ArrayList<T> list = arrayObjects[idx];
        T data = null;
        synchronized (list) {
            int count = list.size();
            if (count > 0) {
                data = list.get(count - 1);
                list.remove(count - 1);
                timeAdded[idx].popBack();
            }
        }
        if (data != null) {
            // Atomically update poolStats
            PoolStats currentPoolStats = poolStats.get();
            PoolStats newPoolStats = new PoolStats(currentPoolStats.totalSize - arrayObjectSizes[idx], currentPoolStats.objectCount - 1);
            while (!poolStats.compareAndSet(currentPoolStats, newPoolStats)) {
                currentPoolStats = poolStats.get();
                newPoolStats = new PoolStats(currentPoolStats.totalSize - arrayObjectSizes[idx], currentPoolStats.objectCount - 1);
            }

            hits[idx]++;
            if (init) {
                allocator.initialize(data);
            }
            return data;
        }
        misses[idx]++;
        return allocator.allocate(arrayObjectSizes[idx]);
    }

    public void release(T data)
    {
        int index = getSizeIndex(allocator.getSize(data));

        if (index < arrayObjectSizes.length) {
            int sizeToAdd = arrayObjectSizes[index];
            if (sizeToAdd != allocator.getSize(data)) {
                throw new IllegalArgumentException("Wrong size for ArrayPool release");
            }

            long now = System.nanoTime();
            ArrayList<T> list = arrayObjects[index];

            // Note that by adding the object before trimming, it's possible totalSize.get() > capacity at some time.
            synchronized (list) {
                list.add(data);
                timeAdded[index].add(now);
            }

            // Atomically update poolStats
            PoolStats currentPoolStats = poolStats.get();

            PoolStats newPoolStats = new PoolStats(currentPoolStats.totalSize + sizeToAdd, currentPoolStats.objectCount + 1);
            while (!poolStats.compareAndSet(currentPoolStats, newPoolStats)) {
                currentPoolStats = poolStats.get();
                newPoolStats = new PoolStats(currentPoolStats.totalSize + sizeToAdd, currentPoolStats.objectCount + 1);
            }

            if (newPoolStats.totalSize > capacity) {
                trim();
            }
        }
    }

    public long getTotalSize()
    {
        return poolStats.get().totalSize;
    }

    public int getAverageObjectSize()
    {
        PoolStats currentPoolStats = poolStats.get();
        return currentPoolStats.objectCount == 0 ? 0 : (int) ((double) currentPoolStats.totalSize / currentPoolStats.objectCount);
    }

    public void setCapacity(long capacity)
    {
        this.capacity = capacity;
        trim();
    }

    // For testing only
    int getAllocationSizeInBytes(int requestedSizeInBytes)
    {
        int index = getSizeIndex(requestedSizeInBytes);
        int targetAllocationSizeInBytes = requestedSizeInBytes;
        if (index < arrayObjectSizes.length) {
            targetAllocationSizeInBytes = arrayObjectSizes[index];
        }
        return targetAllocationSizeInBytes;
    }

    private ArrayList<Integer> getSizeList(int smallestSize, int largestSize)
    {
        int size = smallestSize;
        ArrayList<Integer> sizeList = new ArrayList();
        while (size < largestSize) {
            sizeList.add(size - 128);
            sizeList.add(size + (size / 2) - 128);
            size *= 2;
        }
        sizeList.add(largestSize);
        return sizeList;
    }

    public int getSizeIndex(int size)
    {
        int idx = Arrays.binarySearch(arrayObjectSizes, size);
        return idx < 0 ? -1 - idx : idx;
    }

    private void trim()
    {
        // We only want one thread to do trimming
        if (!trimming.compareAndSet(false, true)) {
            return;
        }

        try {
            long now = System.nanoTime();

            while (poolStats.get().totalSize > capacity * 0.75) {
                // Get the min, max and avg age for all objects in the pool. We use age which is a delta instead of absolute time to avoid overflow when calculating average.
                long oldestAgeForAllObjects = Long.MIN_VALUE;
                long totalAgeForAllObjects = 0;
                int objectCountSnapshot = 0;
                for (int i = 0; i < arrayObjectSizes.length; i++) {
                    LongArrayList timeAddedForCurrentSize = timeAdded[i];
                    ArrayList<T> objectList = arrayObjects[i];

                    // synchronize on objectList to make sure no other threads would insert or remove from the objectList and timeAdded at the same time
                    synchronized (objectList) {
                        for (int j = 0; j < timeAddedForCurrentSize.size(); j += SAMPLING_FRACTION) {
                            long ageForCurrentObject = now - timeAddedForCurrentSize.get(j);
                            totalAgeForAllObjects += ageForCurrentObject;
                            oldestAgeForAllObjects = max(oldestAgeForAllObjects, ageForCurrentObject);
                            objectCountSnapshot++;
                        }
                    }
                }
                double averageAgeForAllObjects = (double) totalAgeForAllObjects / objectCountSnapshot;
                long timeAddedThreshold = now - (long) ((oldestAgeForAllObjects + averageAgeForAllObjects) / 2);

                // Evict objects who were added before or exactly at timeAddedThreshold, ie. timeAdded <= timeAddedThreshold
                for (int i = 0; i < arrayObjectSizes.length; i++) {
                    LongArrayList timeAddedForCurrentSize = timeAdded[i];
                    ArrayList<T> objectList = arrayObjects[i];
                    int currentSize = arrayObjectSizes[i];

                    int sizeToRemove = 0;
                    int cutoffIndex = 0;
                    synchronized (objectList) {
                        if (poolStats.get().totalSize <= capacity * 0.75) {
                            trimming.set(false);
                            return;
                        }

                        if (objectList.size() == 0) {
                            continue;
                        }

                        int length = objectList.size();
                        while (cutoffIndex < length && timeAddedForCurrentSize.get(cutoffIndex) <= timeAddedThreshold && poolStats.get().totalSize > capacity * 0.75) {
                            objectList.remove(length - cutoffIndex - 1);
                            cutoffIndex++;
                        }

                        if (cutoffIndex > 0) {
                            timeAddedForCurrentSize.removeFront(cutoffIndex);
                            sizeToRemove = cutoffIndex * currentSize;
                        }
                    }

                    // Atomically update poolStats
                    PoolStats currentPoolStats = poolStats.get();
                    PoolStats newPoolStats = new PoolStats(currentPoolStats.totalSize - sizeToRemove, currentPoolStats.objectCount - cutoffIndex);
                    while (!poolStats.compareAndSet(currentPoolStats, newPoolStats)) {
                        currentPoolStats = poolStats.get();
                        newPoolStats = new PoolStats(currentPoolStats.totalSize - sizeToRemove, currentPoolStats.objectCount - cutoffIndex);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("ArrayPool failed to trim with exception %s", e.getCause().getStackTrace()));
        }
        finally {
            trimming.set(false);
        }
    }

    private static void verify(boolean assertion, String message)
    {
        if (!assertion) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, message);
        }
    }

    // For testing
    int[] getArrayObjectSizes()
    {
        return arrayObjectSizes;
    }
}
