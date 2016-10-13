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
package com.facebook.presto.accumulo.index;

import com.facebook.presto.accumulo.index.metrics.MetricCacheKey;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TimestampType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * This class is an indexing utility to cache the cardinality of a column value for every table.
 * Each table has its own cache that is independent of every other, and every column also has its
 * own Guava cache. Use of this utility can have a significant impact for retrieving the cardinality
 * of many columns, preventing unnecessary accesses to the metrics table in Accumulo for a
 * cardinality that won't change much.
 */
public class ColumnCardinalityCache
{
    private static final Logger LOG = Logger.get(ColumnCardinalityCache.class);
    private final ExecutorService executorService;
    private final LoadingCache<MetricCacheKey, Long> cache;

    @SuppressWarnings("unchecked")
    public ColumnCardinalityCache(int size, Duration expireDuration)
    {
        requireNonNull(expireDuration, "expireDuration is null");

        // Create executor service with one hot thread, pool size capped at 4x processors,
        // one minute keep alive, and a labeled ThreadFactory
        AtomicLong threadCount = new AtomicLong(0);
        this.executorService = MoreExecutors.getExitingExecutorService(
                new ThreadPoolExecutor(
                        1,
                        4 * Runtime.getRuntime().availableProcessors(),
                        60L,
                        TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        runnable -> new Thread(runnable, "cardinality-lookup-thread-" + threadCount.getAndIncrement())
                ));

        LOG.debug("Created new cache size %d expiry %s", size, expireDuration);
        CacheBuilder cacheBuilder = CacheBuilder.newBuilder()
                .maximumSize(size)
                .expireAfterWrite(expireDuration.toMillis(), TimeUnit.MILLISECONDS);

        if (LOG.isDebugEnabled()) {
            cacheBuilder.recordStats();
        }

        this.cache = (LoadingCache<MetricCacheKey, Long>) cacheBuilder.build(new CardinalityCacheLoader());
    }

    /**
     * Gets the cardinality for each {@link AccumuloColumnConstraint}.
     * Given constraints are expected to be indexed! Who knows what would happen if they weren't!
     *
     * @param schema Schema name
     * @param table Table name
     * @param idxConstraintRangePairs Mapping of all ranges for a given constraint
     * @param auths Scan-time authorizations for loading any cardinalities from Accumulo
     * @param earlyReturnThreshold Smallest acceptable cardinality to return early while other tasks complete. Use a negative value to disable early return.
     * @param pollingDuration Duration for polling the cardinality completion service. Use a Duration of zero to disable polling.
     * @param metricsStorage Metrics storage for looking up the cardinality
     * @param truncateTimestamps True if timestamp type metrics are truncated
     * @return An immutable multimap of cardinality to column constraint, sorted by cardinality from smallest to largest
     * @throws TableNotFoundException If the metrics table does not exist
     * @throws ExecutionException If another error occurs; I really don't even know anymore.
     */
    public Multimap<Long, AccumuloColumnConstraint> getCardinalities(String schema, String table, Multimap<AccumuloColumnConstraint, Range> idxConstraintRangePairs, Authorizations auths, long earlyReturnThreshold, Duration pollingDuration, MetricsStorage metricsStorage, boolean truncateTimestamps)
            throws ExecutionException, TableNotFoundException
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(idxConstraintRangePairs, "idxConstraintRangePairs is null");
        requireNonNull(auths, "auths is null");
        requireNonNull(pollingDuration, "pollingDuration is null");
        requireNonNull(metricsStorage, "metricsStorage is null");

        if (idxConstraintRangePairs.isEmpty()) {
            return ImmutableMultimap.of();
        }

        // Submit tasks to the executor to fetch column cardinality, adding it to the Guava cache if necessary
        CompletionService<Pair<Long, AccumuloColumnConstraint>> executor = new ExecutorCompletionService<>(executorService);
        idxConstraintRangePairs.asMap().entrySet().forEach(e ->
                executor.submit(() -> {
                            long cardinality = getColumnCardinality(schema, table, e.getKey().getFamily(), e.getKey().getQualifier(), metricsStorage, truncateTimestamps && e.getKey().getType() == TimestampType.TIMESTAMP, auths, e.getValue());
                            long start = System.currentTimeMillis();
                            LOG.debug("Cardinality for column %s is %s, took %s ms", e.getKey().getName(), cardinality, System.currentTimeMillis() - start);
                            return Pair.of(cardinality, e.getKey());
                        }
                ));

        long pollingMillis = pollingDuration.toMillis();

        // Create a multi map sorted by cardinality
        ListMultimap<Long, AccumuloColumnConstraint> cardinalityToConstraints = MultimapBuilder.treeKeys().arrayListValues().build();
        try {
            boolean earlyReturn = false;
            int numTasks = idxConstraintRangePairs.asMap().entrySet().size();
            do {
                // Sleep for the polling duration to allow concurrent tasks to run for this time
                if (pollingMillis > 0) {
                    Thread.sleep(pollingMillis);
                }

                // Poll each task, retrieving the result if it is done
                for (int i = 0; i < numTasks; ++i) {
                    Future<Pair<Long, AccumuloColumnConstraint>> futureCardinality = executor.poll();
                    if (futureCardinality != null) {
                        Pair<Long, AccumuloColumnConstraint> columnCardinality = futureCardinality.get();
                        cardinalityToConstraints.put(columnCardinality.getLeft(), columnCardinality.getRight());
                    }
                }

                // If the smallest cardinality is present and below the threshold, set the earlyReturn flag
                Optional<Entry<Long, AccumuloColumnConstraint>> smallestCardinality = cardinalityToConstraints.entries().stream().findFirst();
                if (smallestCardinality.isPresent()) {
                    if (smallestCardinality.get().getKey() <= earlyReturnThreshold) {
                        LOG.debug("Cardinality for column %s is below threshold of %s. Returning early while other tasks finish",
                                smallestCardinality.get().getValue().getName(), earlyReturnThreshold);
                        earlyReturn = true;
                    }
                }
            }
            while (!earlyReturn && cardinalityToConstraints.entries().size() < numTasks);
        }
        catch (ExecutionException | InterruptedException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Exception when getting cardinality", e);
        }

        // Create a copy of the cardinalities
        return ImmutableMultimap.copyOf(cardinalityToConstraints);
    }

    /**
     * Gets the column cardinality for all of the given range values. May reach out to the
     * metrics table in Accumulo to retrieve new cache elements.
     *
     * @param schema Table schema
     * @param table Table name
     * @param family Accumulo column family
     * @param qualifier Accumulo column qualifier
     * @param metricsStorage Metrics storage for looking up the cardinality
     * @param timestampsTruncated True if timestamps are truncated AND this is a Timestamp type, false otherwise
     * @param auths Scan-time authorizations for loading any cardinalities from Accumulo
     * @param columnRanges All range values to summarize for the cardinality
     * @return The cardinality of the column
     */
    private long getColumnCardinality(
            String schema,
            String table,
            String family,
            String qualifier,
            MetricsStorage metricsStorage,
            boolean timestampsTruncated,
            Authorizations auths,
            Collection<Range> columnRanges)
            throws ExecutionException
    {
        LOG.debug("Getting cardinality for %s:%s", family, qualifier);

        // Collect all exact Accumulo Ranges, i.e. single value entries vs. a full scan
        Collection<MetricCacheKey> exactRanges = columnRanges
                .stream()
                .filter(this::isExact)
                .map(range -> new MetricCacheKey(metricsStorage, schema, table, family, qualifier, timestampsTruncated, auths, range))
                .collect(Collectors.toList());

        LOG.debug("Column values contain %s exact ranges of %s", exactRanges.size(),
                columnRanges.size());

        // Sum the cardinalities for the exact-value Ranges
        // This is where the reach-out to Accumulo occurs for all Ranges that have not
        // previously been fetched
        long sum = 0;
        if (exactRanges.size() == 1) {
            sum = cache.get(exactRanges.stream().findAny().get());
        }
        else {
            for (Long value : cache.getAll(exactRanges).values()) {
                sum += value;
            }
        }

        // If these collection sizes are not equal,
        // then there is at least one non-exact range
        if (exactRanges.size() != columnRanges.size()) {
            // for each range in the column value
            for (Range range : columnRanges) {
                // if this range is not exact
                if (!isExact(range)) {
                    // Then get the value for this range using the single-value cache lookup
                    MetricCacheKey key = new MetricCacheKey(metricsStorage, schema, table, family, qualifier, timestampsTruncated, auths, range);
                    long value = cache.get(key);

                    // add our value to the cache and our sum
                    cache.put(key, value);
                    sum += value;
                }
            }
        }

        LOG.debug("Cache stats : size=%s, %s", cache.size(), cache.stats());
        return sum;
    }

    private boolean isExact(Range range)
    {
        return !range.isInfiniteStartKey()
                && !range.isInfiniteStopKey()
                && range.getStartKey().followingKey(PartialKey.ROW).equals(range.getEndKey());
    }

    /**
     * Internal class for loading the cardinality from Accumulo
     */
    private class CardinalityCacheLoader
            extends CacheLoader<MetricCacheKey, Long>
    {
        /**
         * Loads the cardinality for the given Range. Uses a BatchScanner and sums the cardinality for all values that encapsulate the Range.
         *
         * @param key Range to get the cardinality for
         * @return The cardinality of the column, which would be zero if the value does not exist
         */
        @Override
        public Long load(@Nonnull MetricCacheKey key)
                throws Exception
        {
            return key.storage.newReader().getCardinality(key);
        }

        @Override
        public Map<MetricCacheKey, Long> loadAll(Iterable<? extends MetricCacheKey> keys)
                throws Exception
        {
            @SuppressWarnings("unchecked")
            Collection<MetricCacheKey> cacheKeys = (Collection<MetricCacheKey>) keys;

            if (cacheKeys.size() == 0) {
                return ImmutableMap.of();
            }

            return cacheKeys.stream().findAny().get().storage.newReader().getCardinalities(cacheKeys);
        }
    }
}
