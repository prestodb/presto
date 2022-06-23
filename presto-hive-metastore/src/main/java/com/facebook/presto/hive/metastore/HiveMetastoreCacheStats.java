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
package com.facebook.presto.hive.metastore;

import com.facebook.airlift.stats.CounterStat;
import com.google.common.cache.LoadingCache;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class HiveMetastoreCacheStats
        implements MetastoreCacheStats
{
    private final CounterStat partitionsWithColumnCountGreaterThanThreshold = new CounterStat();
    private LoadingCache<?, ?> partitionCache;

    @Override
    public void setPartitionCache(LoadingCache<?, ?> partitionCache)
    {
        this.partitionCache = partitionCache;
    }

    @Override
    public void incrementPartitionsWithColumnCountGreaterThanThreshold()
    {
        partitionsWithColumnCountGreaterThanThreshold.update(1);
    }

    @Managed
    @Override
    public long getPartitionCacheHit()
    {
        return partitionCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getPartitionCacheMiss()
    {
        return partitionCache.stats().missCount();
    }

    @Managed
    @Override
    public long getPartitionCacheEviction()
    {
        return partitionCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getPartitionCacheSize()
    {
        return partitionCache.size();
    }

    @Managed
    @Nested
    @Override
    public CounterStat getPartitionsWithColumnCountGreaterThanThreshold()
    {
        return partitionsWithColumnCountGreaterThanThreshold;
    }
}
