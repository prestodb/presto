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
package com.facebook.presto.iceberg;

import com.facebook.airlift.stats.DistributionStat;
import com.facebook.presto.hive.CacheStatsMBean;
import com.google.common.cache.Cache;
import com.google.common.cache.ForwardingCache;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class ManifestFileCache
        extends ForwardingCache.SimpleForwardingCache<ManifestFileCacheKey, ManifestFileCachedContent>
{
    private final DistributionStat fileSizes = new DistributionStat();
    private final long maxFileLength;
    private final boolean enabled;
    private final long bufferChunkSize;
    private final CacheStatsMBean statsMBean;

    public ManifestFileCache(Cache<ManifestFileCacheKey, ManifestFileCachedContent> delegate, boolean enabled, long maxFileLength, long bufferChunkSize)
    {
        super(delegate);
        this.maxFileLength = maxFileLength;
        this.enabled = enabled;
        this.bufferChunkSize = bufferChunkSize;
        this.statsMBean = new CacheStatsMBean(delegate);
    }

    @Managed
    @Nested
    public CacheStatsMBean getCacheStats()
    {
        return statsMBean;
    }

    @Managed
    @Nested
    public DistributionStat getFileSizeDistribution()
    {
        return fileSizes;
    }

    public void recordFileSize(long size)
    {
        fileSizes.add(size);
    }

    public long getMaxFileLength()
    {
        return maxFileLength;
    }

    public long getBufferChunkSize()
    {
        return bufferChunkSize;
    }

    public boolean isEnabled()
    {
        return enabled;
    }
}
