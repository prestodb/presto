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

import java.util.Arrays;

public final class Caches
{
    static final int BOOLEAN_SMALLEST_ARRAY_SIZE = 16;
    static final int BOOLEAN_LARGEST_ARRAY_SIZE = 64 * 1024;
    static final long BOOLEAN_POOL_CAPACITY = 1024 * 1024;
    static final int BYTE_SMALLEST_ARRAY_SIZE = 1024;
    static final int BYTE_LARGEST_ARRAY_SIZE = 8 * 1024 * 1024;
    static final long BYTE_POOL_CAPACITY = 2028 * 1024 * 1024;

    private static ArrayPool<byte[]> byteArrayPool;
    private static ByteArrayPoolCacheAdapter byteArrayPoolCacheAdapter;

    private Caches() {}

    private static class BooleanArrayAllocator
            extends ArrayPool.Allocator<boolean[]>
    {
        @Override
        boolean[] allocate(int size)
        {
            return new boolean[size];
        }

        @Override
        void initialize(boolean[] array)
        {
            Arrays.fill(array, false);
        }

        @Override
        int getSize(boolean[] array)
        {
            return array.length;
        }
    }

    private static class ByteArrayAllocator
            extends ArrayPool.Allocator<byte[]>
    {
        @Override
        byte[] allocate(int size)
        {
            return new byte[size];
        }

        @Override
        void initialize(byte[] array)
        {
            Arrays.fill(array, (byte) 0);
        }

        @Override
        int getSize(byte[] array)
        {
            return array.length;
        }
    }

    private static ArrayPool<boolean[]> booleanArrayPool;

    public static ArrayPool<boolean[]> getBooleanArrayPool()
    {
        synchronized (Caches.class) {
            if (booleanArrayPool == null) {
                booleanArrayPool = new ArrayPool(BOOLEAN_SMALLEST_ARRAY_SIZE, BOOLEAN_LARGEST_ARRAY_SIZE, BOOLEAN_POOL_CAPACITY, new BooleanArrayAllocator());
            }
        }
        return booleanArrayPool;
    }

    public static ArrayPool<byte[]> getByteArrayPool()
    {
        synchronized (Caches.class) {
            if (byteArrayPool == null) {
                byteArrayPool = new ArrayPool(BYTE_SMALLEST_ARRAY_SIZE, BYTE_LARGEST_ARRAY_SIZE, BYTE_POOL_CAPACITY, new ByteArrayAllocator());
            }
        }
        return byteArrayPool;
    }

    public static CacheAdapter getByteArrayPoolCacheAdapter()
    {
        ArrayPool<byte[]> pool = getByteArrayPool();
        synchronized (Caches.class) {
            if (byteArrayPoolCacheAdapter == null) {
                byteArrayPoolCacheAdapter = new ByteArrayPoolCacheAdapter(byteArrayPool);
            }
            return byteArrayPoolCacheAdapter;
        }
    }
}
