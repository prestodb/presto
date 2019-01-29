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

import com.facebook.presto.spi.block.ConcatenatedByteArrayInputStream;
import com.facebook.presto.spi.memory.ByteArrayPool;

public class ExchangeClientByteArrayAllocator
        implements io.airlift.http.client.ByteArrayAllocator
{
    private ByteArrayPool pool;

    public ExchangeClientByteArrayAllocator(ByteArrayPool pool)
    {
        this.pool = pool;
    }

    public ConcatenatedByteArrayInputStream.Allocator toPrestoAllocator()
    {
        return new PrestoByteArrayAllocator();
    }

    @Override
    public byte[] allocate(int size)
    {
        return pool.getBytes(size);
    }

    @Override
    public void free(byte[] bytes)
    {
        pool.release(bytes);
    }

    public class PrestoByteArrayAllocator
            implements ConcatenatedByteArrayInputStream.Allocator
    {
        @Override
        public void free(byte[] bytes)
        {
            pool.release(bytes);
        }
    }
}
