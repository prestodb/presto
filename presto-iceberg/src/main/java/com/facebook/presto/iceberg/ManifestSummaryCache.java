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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.hive.CacheStatsMBean;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * Caches the manifest summary folded for one (snapshot, schema, spec, predicate, columns) tuple.
 *
 * <p>Folding that summary is the dominant cost of planning against a large table: it walks every
 * data file in the snapshot and decodes the per-file column bounds. A single query asks for it
 * repeatedly, because a new {@code CachingStatsProvider} is created per optimizer pass and its
 * cache is keyed on plan node identity, and repeat queries ask for it again.
 *
 * <p>Every input is part of the key (see {@link ManifestSummaryCacheKey}), so entries are shared
 * across queries, sessions and users without any staleness window. A new commit produces a new
 * snapshot id and therefore a new key rather than a stale hit.
 */
public class ManifestSummaryCache
{
    private final Cache<ManifestSummaryCacheKey, Optional<Partition>> cache;
    private final CacheStatsMBean cacheStats;
    private final CounterStat foldedFiles = new CounterStat();

    public ManifestSummaryCache(Cache<ManifestSummaryCacheKey, Optional<Partition>> cache)
    {
        this.cache = requireNonNull(cache, "cache is null");
        this.cacheStats = new CacheStatsMBean(cache);
    }

    /**
     * Returns the cached summary, or folds one through {@code loader} and caches it.
     *
     * <p>An empty summary, which is how a scan matching no files is reported, is cached like any
     * other outcome: it is a fact about the snapshot and the predicate named in the key, and both
     * of those are immutable, so it cannot go stale.
     *
     * <p>The loader runs at most once per key even under concurrent callers. That matters here
     * specifically: the cold case is several queries planning against the same table at once, which
     * without the deduplication would each fold the same manifests in parallel.
     *
     * <p>A failure of {@code loader} is rethrown as it was thrown, not wrapped by the cache, so that
     * a caller sees the same exception, and the same error code, whether or not the cache is used.
     */
    public Optional<Partition> get(ManifestSummaryCacheKey key, Supplier<Optional<Partition>> loader)
    {
        try {
            return cache.get(key, () -> {
                Optional<Partition> folded = loader.get();
                folded.ifPresent(summary -> foldedFiles.update(summary.getFileCount()));
                // Handed to every later query that hits this key, so it must not stay mutable.
                return folded.map(Partition::immutableCopy);
            });
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new UncheckedExecutionException(e.getCause());
        }
    }

    /**
     * Drops every entry. Never needed for correctness while snapshot ids are unique per table, which
     * is why this is reachable only through an explicit procedure: the cases that break that premise
     * — metadata rolled back by hand, or a table re-registered against different metadata — are
     * operator actions, so the remedy is one too.
     */
    public void invalidateAll()
    {
        cache.invalidateAll();
    }

    @Managed
    @Nested
    public CacheStatsMBean getCacheStats()
    {
        return cacheStats;
    }

    /**
     * Number of data files folded, which is the work the cache exists to avoid repeating. Only a
     * miss adds to it, so it stops growing once a workload's keys are resident.
     */
    @Managed
    @Nested
    public CounterStat getFoldedFiles()
    {
        return foldedFiles;
    }
}
