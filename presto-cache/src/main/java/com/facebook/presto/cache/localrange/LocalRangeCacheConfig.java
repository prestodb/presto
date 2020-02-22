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
package com.facebook.presto.cache.localrange;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;

import static java.util.concurrent.TimeUnit.DAYS;

public class LocalRangeCacheConfig
{
    private int maxCachedEntries = 1_000;
    private Duration cacheTtl = new Duration(2, DAYS);

    @Min(1)
    public int getMaxCachedEntries()
    {
        return maxCachedEntries;
    }

    @Config("cache.max-cached-entries")
    @ConfigDescription("Number of entries allowed in the cache")
    public LocalRangeCacheConfig setMaxCachedEntries(int maxCachedEntries)
    {
        this.maxCachedEntries = maxCachedEntries;
        return this;
    }

    @MinDuration("0s")
    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("cache.ttl")
    @ConfigDescription("Time-to-live for a cache entry")
    public LocalRangeCacheConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }

    @Override
    public String toString()
    {
        return "LocalRangeCacheConfig{" +
                "maxCachedEntries=" + maxCachedEntries +
                ", cacheTtl=" + cacheTtl +
                '}';
    }
}
