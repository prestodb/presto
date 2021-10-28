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
package com.facebook.presto.hive;

import com.facebook.presto.common.RuntimeStats;

import java.util.Optional;

import static com.facebook.presto.hive.CacheQuota.NO_CACHE_CONSTRAINTS;
import static java.util.Objects.requireNonNull;

public class HiveFileContext
{
    public static final HiveFileContext DEFAULT_HIVE_FILE_CONTEXT = new HiveFileContext(true, NO_CACHE_CONSTRAINTS, Optional.empty(), Optional.empty(), 0, false);

    private final boolean cacheable;
    private final CacheQuota cacheQuota;
    private final Optional<ExtraHiveFileInfo<?>> extraFileInfo;
    private final Optional<Long> fileSize;
    private final long modificationTime;
    private final boolean verboseRuntimeStatsEnabled;

    private final RuntimeStats stats = new RuntimeStats();

    public HiveFileContext(boolean cacheable, CacheQuota cacheQuota, Optional<ExtraHiveFileInfo<?>> extraFileInfo, Optional<Long> fileSize, long modificationTime, boolean verboseRuntimeStatsEnabled)
    {
        this.cacheable = cacheable;
        this.cacheQuota = requireNonNull(cacheQuota, "cacheQuota is null");
        this.extraFileInfo = requireNonNull(extraFileInfo, "extraFileInfo is null");
        this.fileSize = requireNonNull(fileSize, "fileSize is null");
        this.modificationTime = modificationTime;
        this.verboseRuntimeStatsEnabled = verboseRuntimeStatsEnabled;
    }

    /**
     * Decide whether the current file is eligible for local cache
     */
    public boolean isCacheable()
    {
        return cacheable;
    }

    public CacheQuota getCacheQuota()
    {
        return cacheQuota;
    }

    public long getModificationTime()
    {
        return modificationTime;
    }

    /**
     * For file opener
     */
    public Optional<ExtraHiveFileInfo<?>> getExtraFileInfo()
    {
        return extraFileInfo;
    }

    public Optional<Long> getFileSize()
    {
        return fileSize;
    }

    public interface ExtraHiveFileInfo<T>
    {
        T getExtraFileInfo();
    }

    public void incrementCounter(String name, long value)
    {
        if (verboseRuntimeStatsEnabled) {
            stats.addMetricValue(name, value);
        }
    }

    public RuntimeStats getStats()
    {
        return stats;
    }
}
