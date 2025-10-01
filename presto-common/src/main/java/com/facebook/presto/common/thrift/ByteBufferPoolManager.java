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
package com.facebook.presto.common.thrift;

import com.facebook.drift.buffer.ByteBufferPool;

import java.util.concurrent.ConcurrentHashMap;

public class ByteBufferPoolManager
{
    private final ConcurrentHashMap<Thread, ByteBufferPool> threadPools = new ConcurrentHashMap<>();
    private static final int DEFAULT_BUFFER_SIZE = 1024; // 1KB
    private static final int DEFAULT_BUFFER_COUNT = 16;
    private final int bufferSize;
    private final int maxPoolSize;

    public ByteBufferPoolManager()
    {
        this.bufferSize = DEFAULT_BUFFER_SIZE;
        this.maxPoolSize = DEFAULT_BUFFER_COUNT;
    }

    public ByteBufferPoolManager(int bufferSize, int maxPoolSize)
    {
        this.bufferSize = bufferSize;
        this.maxPoolSize = maxPoolSize;
    }

    public ByteBufferPool getPool()
    {
        return threadPools.computeIfAbsent(Thread.currentThread(), t -> new ByteBufferPool(bufferSize, maxPoolSize));
    }
}
