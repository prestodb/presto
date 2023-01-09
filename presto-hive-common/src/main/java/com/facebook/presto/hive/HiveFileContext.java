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
import com.facebook.presto.common.RuntimeUnit;

import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.hive.CacheQuota.NO_CACHE_CONSTRAINTS;
import static java.util.Objects.requireNonNull;

public class HiveFileContext
{
    public static final HiveFileContext DEFAULT_HIVE_FILE_CONTEXT = new HiveFileContext(
            true,
            NO_CACHE_CONSTRAINTS,
            Optional.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            0,
            false);

    private final boolean cacheable;
    private final CacheQuota cacheQuota;
    private final Optional<ExtraHiveFileInfo<?>> extraFileInfo;
    private final OptionalLong fileSize;
    // HiveFileContext optionally contains startOffset and length that readers are going to read
    // For large files, these hints will help in fetching only the required blocks from the storage
    // Note: even when they are present, readers can read past this range like footer and stripe's
    // start may be in this range, but end may be beyond this range. So this is a hint and readers
    // should handle the read beyond the range gracefully.
    private final OptionalLong startOffset;
    private final OptionalLong length;
    private final long modificationTime;
    private final boolean verboseRuntimeStatsEnabled;

    private final RuntimeStats stats = new RuntimeStats();

    public HiveFileContext(
            boolean cacheable,
            CacheQuota cacheQuota,
            Optional<ExtraHiveFileInfo<?>> extraFileInfo,
            OptionalLong fileSize,
            OptionalLong startOffset,
            OptionalLong length,
            long modificationTime,
            boolean verboseRuntimeStatsEnabled)
    {
        this.cacheable = cacheable;
        this.cacheQuota = requireNonNull(cacheQuota, "cacheQuota is null");
        this.extraFileInfo = requireNonNull(extraFileInfo, "extraFileInfo is null");
        this.fileSize = requireNonNull(fileSize, "fileSize is null");
        this.startOffset = requireNonNull(startOffset, "startOffset is null");
        this.length = requireNonNull(length, "length is null");
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

    public OptionalLong getFileSize()
    {
        return fileSize;
    }

    public OptionalLong getStartOffset()
    {
        return startOffset;
    }

    public OptionalLong getLength()
    {
        return length;
    }

    public interface ExtraHiveFileInfo<T>
    {
        T getExtraFileInfo();
    }

    public void incrementCounter(String name, RuntimeUnit unit, long value)
    {
        if (verboseRuntimeStatsEnabled) {
            stats.addMetricValue(name, unit, value);
        }
    }

    public RuntimeStats getStats()
    {
        return stats;
    }
}
