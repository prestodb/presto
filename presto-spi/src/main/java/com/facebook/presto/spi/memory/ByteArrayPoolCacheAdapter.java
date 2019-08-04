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

import static java.util.Objects.requireNonNull;

public class ByteArrayPoolCacheAdapter
        implements CacheAdapter
{
    private final ArrayPool<byte[]> pool;

    ByteArrayPoolCacheAdapter(ArrayPool<byte[]> pool)
    {
        this.pool = requireNonNull(pool, "pool is null");
    }

    public static class ByteArrayCacheEntry
            implements CacheEntry
    {
        private final byte[] data;
        private final ArrayPool<byte[]> pool;

        public ByteArrayCacheEntry(ArrayPool<byte[]> pool, byte[] data)
        {
            this.pool = requireNonNull(pool, "pool is null");
            this.data = requireNonNull(data, "data is null");
        }

        @Override
        public byte[] getData()
        {
            return data;
        }

        @Override
        public boolean isFetched()
        {
            return false;
        }

        @Override
        public boolean needFetch()
        {
            return true;
        }

        @Override
        public void setFetched()
        {
        }

        @Override
        public void waitFetched()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void release()
        {
            pool.release(data);
        }
    }

    public ByteArrayCacheEntry get(long position, int size)
    {
        return new ByteArrayCacheEntry(pool, pool.allocate(size));
    }
}
