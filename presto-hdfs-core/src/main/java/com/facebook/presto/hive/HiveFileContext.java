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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.ConnectorIdentity;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

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
    private final Optional<String> source;
    private final Optional<ConnectorIdentity> identity;
    private final Optional<String> queryId;
    private final Optional<String> schema;
    private final Optional<String> clientInfo;
    private final Optional<Set<String>> clientTags;

    private final RuntimeStats stats;

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
        this(cacheable, cacheQuota, extraFileInfo, fileSize, startOffset, length, modificationTime, verboseRuntimeStatsEnabled, new RuntimeStats());
    }

    public HiveFileContext(
            boolean cacheable,
            CacheQuota cacheQuota,
            Optional<ExtraHiveFileInfo<?>> extraFileInfo,
            OptionalLong fileSize,
            OptionalLong startOffset,
            OptionalLong length,
            long modificationTime,
            boolean verboseRuntimeStatsEnabled,
            RuntimeStats runtimeStats)
    {
        this(
                cacheable,
                cacheQuota,
                extraFileInfo,
                fileSize,
                startOffset,
                length,
                modificationTime,
                verboseRuntimeStatsEnabled,
                runtimeStats,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public HiveFileContext(
            boolean cacheable,
            CacheQuota cacheQuota,
            Optional<ExtraHiveFileInfo<?>> extraFileInfo,
            OptionalLong fileSize,
            OptionalLong startOffset,
            OptionalLong length,
            long modificationTime,
            boolean verboseRuntimeStatsEnabled,
            RuntimeStats runtimeStats,
            ConnectorSession session)
    {
        this(
                cacheable,
                cacheQuota,
                extraFileInfo,
                fileSize,
                startOffset,
                length,
                modificationTime,
                verboseRuntimeStatsEnabled,
                runtimeStats,
                session.getSource(),
                Optional.of(session.getIdentity()),
                Optional.of(session.getQueryId()),
                session.getSchema(),
                session.getClientInfo(),
                Optional.of(session.getClientTags()));
    }

    public HiveFileContext(
            boolean cacheable,
            CacheQuota cacheQuota,
            Optional<ExtraHiveFileInfo<?>> extraFileInfo,
            OptionalLong fileSize,
            OptionalLong startOffset,
            OptionalLong length,
            long modificationTime,
            boolean verboseRuntimeStatsEnabled,
            RuntimeStats runtimeStats,
            Optional<String> source,
            Optional<ConnectorIdentity> identity,
            Optional<String> queryId,
            Optional<String> schema,
            Optional<String> clientInfo,
            Optional<Set<String>> clientTags)
    {
        this.cacheable = cacheable;
        this.cacheQuota = requireNonNull(cacheQuota, "cacheQuota is null");
        this.extraFileInfo = requireNonNull(extraFileInfo, "extraFileInfo is null");
        this.fileSize = requireNonNull(fileSize, "fileSize is null");
        this.startOffset = requireNonNull(startOffset, "startOffset is null");
        this.length = requireNonNull(length, "length is null");
        this.modificationTime = modificationTime;
        this.verboseRuntimeStatsEnabled = verboseRuntimeStatsEnabled;
        this.stats = requireNonNull(runtimeStats, "runtimeStats is null");
        this.source = requireNonNull(source, "source is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.clientTags = requireNonNull(clientTags, "clientTags is null");
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

    public Optional<String> getSource()
    {
        return source;
    }

    public Optional<ConnectorIdentity> getIdentity()
    {
        return identity;
    }

    public Optional<String> getQueryId()
    {
        return queryId;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    public Optional<Set<String>> getClientTags()
    {
        return clientTags;
    }
}
