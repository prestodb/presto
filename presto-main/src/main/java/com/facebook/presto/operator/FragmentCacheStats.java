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

import org.weakref.jmx.Managed;

import java.util.concurrent.atomic.AtomicLong;

public class FragmentCacheStats
{
    private final AtomicLong hit = new AtomicLong();
    private final AtomicLong miss = new AtomicLong();
    private final AtomicLong inFlightBytes = new AtomicLong();
    private final AtomicLong cacheRemoval = new AtomicLong();
    private final AtomicLong cacheEntries = new AtomicLong();

    // Total on-disk size in bytes.
    private final AtomicLong cacheSizeInBytes = new AtomicLong();

    public void incrementCacheHit()
    {
        hit.getAndIncrement();
    }

    public void incrementCacheMiss()
    {
        miss.getAndIncrement();
    }

    public void addInFlightBytes(long bytes)
    {
        inFlightBytes.addAndGet(bytes);
    }

    public void addCacheSizeInBytes(long bytes)
    {
        cacheSizeInBytes.addAndGet(bytes);
    }

    public void incrementCacheRemoval()
    {
        cacheRemoval.getAndIncrement();
    }

    public void incrementCacheEntries()
    {
        cacheEntries.getAndIncrement();
    }

    public void decrementCacheEntries()
    {
        cacheEntries.getAndDecrement();
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
    public long getInFlightBytes()
    {
        return inFlightBytes.get();
    }

    @Managed
    public long getCacheRemoval()
    {
        return cacheRemoval.get();
    }

    @Managed
    public long getCacheEntries()
    {
        return cacheEntries.get();
    }

    @Managed
    public long getCacheSizeInBytes()
    {
        return cacheSizeInBytes.get();
    }
}
