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
package com.facebook.presto.cache;

import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class CacheStats
{
    private final AtomicLong inMemoryRetainedBytes = new AtomicLong();
    private final AtomicLong hit = new AtomicLong();
    private final AtomicLong miss = new AtomicLong();
    private final AtomicLong quotaExceed = new AtomicLong();

    public void incrementCacheHit()
    {
        hit.getAndIncrement();
    }

    public void incrementCacheMiss()
    {
        miss.getAndIncrement();
    }

    public void incrementQuotaExceed()
    {
        quotaExceed.getAndIncrement();
    }

    public void addInMemoryRetainedBytes(long bytes)
    {
        inMemoryRetainedBytes.addAndGet(bytes);
    }

    @Managed
    public long getInMemoryRetainedBytes()
    {
        return inMemoryRetainedBytes.get();
    }

    @Managed
    public long getCacheHit()
    {
        return hit.get();
    }

    @Managed
    public long getCacheMiss()
    {
        return miss.get();
    }

    @Managed
    public long getQuotaExceed()
    {
        return quotaExceed.get();
    }
}
