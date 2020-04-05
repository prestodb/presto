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

public class Caches
{
    private static ArrayPool<byte[]> byteArrayPool;
    private static ByteArrayPoolCacheAdapter byteArrayPoolCacheAdapter;
    private static class BooleanArrayAllocator
            extends ArrayPool.Allocator<boolean[]>
    {
        @Override
        boolean[] alloc(int size)
        {
            return new boolean[size];
        }

        @Override
        void init(boolean[] array)
        {
            Arrays.fill(array, false);
        }

        @Override
        int getLength(boolean[] array)
        {
            return array.length;
        }
    }

    private static class ByteArrayAllocator
            extends ArrayPool.Allocator<byte[]>
    {
        @Override
        byte[] alloc(int size)
        {
            return new byte[size];
        }

        @Override
        void init(byte[] array)
        {
            Arrays.fill(array, (byte) 0);
        }

        @Override
        int getLength(byte[] array)
        {
            return array.length;
        }
    }


    private static ArrayPool<boolean[]> booleanArrayPool;

    public static ArrayPool<boolean[]> getBooleanArrayPool()
    {
        synchronized (Caches.class) {
            if (booleanArrayPool == null) {
                booleanArrayPool = new ArrayPool(16, 64 * 1024, 1024 * 1024, new BooleanArrayAllocator());
            }
        }
        return booleanArrayPool;
    }

    public static ArrayPool<byte[]> getByteArrayPool()
    {
        synchronized (Caches.class) {
            if (byteArrayPool == null) {
                byteArrayPool = new ArrayPool(8 * 1024 * 1024, 16 * 1024 * 1024, 4056 * 1024 * 1024, new ByteArrayAllocator());
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