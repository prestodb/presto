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

public class ByteArrayPoolCacheAdapter
        implements CacheAdapter
{

    private ArrayPool<byte[]> pool;

    ByteArrayPoolCacheAdapter(ArrayPool<byte[]> pool)
    {
        this.pool = pool;
    }

    public static class ByteArrayCacheEntry
            implements CacheEntry
    {
        private final byte[] data;
        private final ArrayPool<byte[]> pool;

        public ByteArrayCacheEntry(ArrayPool<byte[]> pool, byte[] data)
        {
            this.pool = pool;
            this.data = data;
        }

        @Override
        public byte[] getData()
        {
            return data;
        }

        @Override
        public void release()
        {
            pool.release(data);
        }
    }

    public ByteArrayCacheEntry get(int size)
    {
        return new ByteArrayCacheEntry(pool, pool.allocInitialized(size));
    }
}