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
package com.facebook.presto.cache.filemerge;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;

public class FileMergeCacheConfig
{
    private int maxCachedEntries = 1_000;
    private Duration cacheTtl = new Duration(2, DAYS);
    private DataSize maxInMemoryCacheSize = new DataSize(2, GIGABYTE);

    @Min(1)
    public int getMaxCachedEntries()
    {
        return maxCachedEntries;
    }

    @Config("cache.max-cached-entries")
    @ConfigDescription("Number of entries allowed in the cache")
    public FileMergeCacheConfig setMaxCachedEntries(int maxCachedEntries)
    {
        this.maxCachedEntries = maxCachedEntries;
        return this;
    }

    public DataSize getMaxInMemoryCacheSize()
    {
        return maxInMemoryCacheSize;
    }

    @Config("cache.max-in-memory-cache-size")
    @ConfigDescription("The maximum cache size allowed in memory")
    public FileMergeCacheConfig setMaxInMemoryCacheSize(DataSize maxInMemoryCacheSize)
    {
        this.maxInMemoryCacheSize = maxInMemoryCacheSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("cache.ttl")
    @ConfigDescription("Time-to-live for a cache entry")
    public FileMergeCacheConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }
}
