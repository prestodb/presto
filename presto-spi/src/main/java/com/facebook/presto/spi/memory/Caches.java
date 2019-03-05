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

public class Caches
{
    private static ByteArrayPool byteArrayPool;
    private static ByteArrayPoolCacheAdapter byteArrayPoolCacheAdapter;

    private Caches() {}

    public static ByteArrayPool getByteArrayPool()
    {
        synchronized (Caches.class) {
            if (byteArrayPool == null) {
                byteArrayPool = new ByteArrayPool(1024, 8 * 1024 * 1024, 2028 * 1024 * 1024);
            }
        }
        return byteArrayPool;
    }

    public static CacheAdapter getByteArrayPoolCacheAdapter()
    {
        ByteArrayPool pool = getByteArrayPool();
        synchronized (Caches.class) {
            if (byteArrayPoolCacheAdapter == null) {
                byteArrayPoolCacheAdapter = new ByteArrayPoolCacheAdapter(byteArrayPool);
            }
            return byteArrayPoolCacheAdapter;
        }
    }
}
