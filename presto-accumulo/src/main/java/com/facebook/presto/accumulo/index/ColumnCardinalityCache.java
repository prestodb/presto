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

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.iterators.ValueSummingIterator;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.spi.PrestoException;
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
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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

import static com.facebook.presto.accumulo.AccumuloErrorCode.INTERNAL_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.nio.charset.StandardCharsets.UTF_8;
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
    private final Connector connector;
    private final ExecutorService executorService;
    private final LoadingCache<CacheKey, Long> cache;

    public ColumnCardinalityCache(
            Connector connector,
            AccumuloConfig config)
    {
        this.connector = requireNonNull(connector, "connector is null");
        int size = requireNonNull(config, "config is null").getCardinalityCacheSize();
        Duration expireDuration = config.getCardinalityCacheExpiration();

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

        cache = (LoadingCache<CacheKey, Long>) cacheBuilder.build(new CardinalityCacheLoader());
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
     * @return An immutable multimap of cardinality to column constraint, sorted by cardinality from smallest to largest
     * @throws TableNotFoundException If the metrics table does not exist
     * @throws ExecutionException If another error occurs; I really don't even know anymore.
     */
    public Multimap<Long, AccumuloColumnConstraint> getCardinalities(String schema, String table, Multimap<AccumuloColumnConstraint, Range> idxConstraintRangePairs, Authorizations auths, long earlyReturnThreshold, Duration pollingDuration)
            throws ExecutionException, TableNotFoundException
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(idxConstraintRangePairs, "idxConstraintRangePairs is null");
        requireNonNull(auths, "auths is null");
        requireNonNull(pollingDuration, "pollingDuration is null");

        if (idxConstraintRangePairs.isEmpty()) {
            return ImmutableMultimap.of();
        }

        // Submit tasks to the executor to fetch column cardinality, adding it to the Guava cache if necessary
        CompletionService<Pair<Long, AccumuloColumnConstraint>> executor = new ExecutorCompletionService<>(executorService);
        idxConstraintRangePairs.asMap().entrySet().forEach(e ->
                executor.submit(() -> {
                            long cardinality = getColumnCardinality(schema, table, e.getKey().getFamily(), e.getKey().getQualifier(), auths, e.getValue());
                            LOG.info("Cardinality for column %s is %s", e.getKey().getName(), cardinality);
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
                        LOG.info("Cardinality for column %s is below threshold of %s. Returning early while other tasks finish",
                                smallestCardinality.get().getValue().getName(), earlyReturnThreshold);
                        earlyReturn = true;
                    }
                }
            }
            while (!earlyReturn && cardinalityToConstraints.entries().size() < numTasks);
        }
        catch (ExecutionException | InterruptedException e) {
            throw new PrestoException(INTERNAL_ERROR, "Exception when getting cardinality", e);
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
     * @param auths Scan-time authorizations for loading any cardinalities from Accumulo
     * @param colValues All range values to summarize for the cardinality
     * @return The cardinality of the column
     */
    private long getColumnCardinality(String schema, String table, String family, String qualifier, Authorizations auths, Collection<Range> colValues)
            throws ExecutionException
    {
        LOG.debug("Getting cardinality for %s:%s", family, qualifier);

        // Collect all exact Accumulo Ranges, i.e. single value entries vs. a full scan
        Collection<CacheKey> exactRanges =
                colValues.stream().filter(this::isExact).map(range ->
                        new CacheKey(schema, table, family, qualifier, auths, range)).collect(Collectors.toList());

        LOG.debug("Column values contain %s exact ranges of %s", exactRanges.size(),
                colValues.size());

        // Sum the cardinalities for the exact-value Ranges
        // This is where the reach-out to Accumulo occurs for all Ranges that have not
        // previously been fetched
        long sum = 0;
        for (Long e : cache.getAll(exactRanges).values()) {
            sum += e;
        }

        // If these collection sizes are not equal,
        // then there is at least one non-exact range
        if (exactRanges.size() != colValues.size()) {
            // for each range in the column value
            for (Range range : colValues) {
                // if this range is not exact
                if (!isExact(range)) {
                    // Then get the value for this range using the single-value cache lookup
                    CacheKey key = new CacheKey(schema, table, family, qualifier, auths, range);
                    long val = cache.get(key);

                    // add our value to the cache and our sum
                    cache.put(key, val);
                    sum += val;
                }
            }
        }

        LOG.debug("Cache stats : size=%s, %s", cache.size(), cache.stats());
        return sum;
    }

    private boolean isExact(Range r)
    {
        return !r.isInfiniteStartKey()
                && !r.isInfiniteStopKey()
                && r.getStartKey().followingKey(PartialKey.ROW).equals(r.getEndKey());
    }

    /**
     * Complex key for the CacheLoader
     */
    private class CacheKey
    {
        public String schema;
        public String table;
        public String family;
        public String qualifier;
        public Authorizations auths;
        public Range range;

        public CacheKey(String schema,
                String table,
                String family,
                String qualifier,
                Authorizations auths,
                Range range)
        {
            this.schema = schema;
            this.table = table;
            this.family = family;
            this.qualifier = qualifier;
            this.auths = auths;
            this.range = range;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(schema, table, family, qualifier, auths, range);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            CacheKey other = (CacheKey) obj;
            return Objects.equals(this.range, other.range)
                    && Objects.equals(this.schema, other.schema)
                    && Objects.equals(this.table, other.table)
                    && Objects.equals(this.family, other.family)
                    && Objects.equals(this.auths, other.auths)
                    && Objects.equals(this.qualifier, other.qualifier);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("schema", schema)
                    .add("table", table)
                    .add("family", family)
                    .add("qualifier", qualifier)
                    .add("auths", auths)
                    .add("range", range).toString();
        }
    }

    /**
     * Internal class for loading the cardinality from Accumulo
     */
    private class CardinalityCacheLoader
            extends CacheLoader<CacheKey, Long>
    {
        /**
         * Loads the cardinality for the given Range. Uses a BatchScanner and sums the cardinality for all values that encapsulate the Range.
         *
         * @param key Range to get the cardinality for
         * @return The cardinality of the column, which would be zero if the value does not exist
         */
        @Override
        public Long load(CacheKey key)
                throws Exception
        {
            LOG.debug("Loading a non-exact range from Accumulo: %s", key);
            // Get metrics table name and the column family for the scanner
            String metricsTable = Indexer.getMetricsTableName(key.schema, key.table);
            Text columnFamily = new Text(Indexer.getIndexColumnFamily(key.family.getBytes(UTF_8), key.qualifier.getBytes(UTF_8)).array());

            // Create scanner for querying the range
            BatchScanner scanner = connector.createBatchScanner(metricsTable, key.auths, 10);
            try {
                scanner.setRanges(connector.tableOperations().splitRangeByTablets(metricsTable, key.range, Integer.MAX_VALUE));
                scanner.fetchColumn(columnFamily, Indexer.CARDINALITY_CQ_AS_TEXT);

                IteratorSetting setting = new IteratorSetting(Integer.MAX_VALUE, "valuesummingcombiner", ValueSummingIterator.class);
                ValueSummingIterator.setEncodingType(setting, Indexer.ENCODER_TYPE);
                scanner.addScanIterator(setting);

                // Sum the entries to get the cardinality
                long sum = 0;
                for (Entry<Key, Value> entry : scanner) {
                    sum += Long.parseLong(entry.getValue().toString());
                }
                return sum;
            }
            finally {
                if (scanner != null) {
                    // Don't forget to close your scanner before returning the cardinalities
                    scanner.close();
                }
            }
        }

        @Override
        public Map<CacheKey, Long> loadAll(Iterable<? extends CacheKey> keys)
                throws Exception
        {
            @SuppressWarnings("unchecked")
            Collection<CacheKey> cacheKeys = (Collection<CacheKey>) keys;
            if (cacheKeys.isEmpty()) {
                return ImmutableMap.of();
            }

            LOG.debug("Loading %s exact ranges from Accumulo", cacheKeys.size());

            // In order to simplify the implementation, we are making a (safe) assumption
            // that the CacheKeys will all contain the same combination of schema/table/family/qualifier
            // This is asserted with the below internal error
            CacheKey key = cacheKeys.stream().findAny().get();
            cacheKeys.forEach(k -> {
                if (!k.schema.equals(key.schema) || !k.table.equals(key.table) || !k.family.equals(key.family) || !k.qualifier.equals(key.qualifier)) {
                    throw new PrestoException(INTERNAL_ERROR, "loadAll called with a non-homogeneous collection of cache keys");
                }
            });

            // Transform the collection into a map of each CacheKey's Range to the key itself
            // This allows us to look up the corresponding CacheKey based on the Row
            // we receive from the scanner, and we can then back-fill our returned map
            // With any values that were not returned by the scan (cardinality zero)
            Map<Range, CacheKey> rangeToKey = new HashMap<>();
            cacheKeys.forEach(k -> rangeToKey.put(k.range, k));
            LOG.debug("rangeToKey size is " + rangeToKey.size());

            // Get metrics table name and the column family for the scanner
            String metricsTable = Indexer.getMetricsTableName(key.schema, key.table);
            Text columnFamily = new Text(
                    Indexer.getIndexColumnFamily(key.family.getBytes(UTF_8), key.qualifier.getBytes(UTF_8)).array());

            BatchScanner scanner = connector.createBatchScanner(metricsTable, key.auths, 10);
            try {
                scanner.setRanges(cacheKeys.stream().map(k -> k.range).collect(Collectors.toList()));
                scanner.fetchColumn(columnFamily, Indexer.CARDINALITY_CQ_AS_TEXT);

                // Create a new map to hold our cardinalities for each range
                // retrieved from the scanner
                ImmutableMap.Builder<CacheKey, Long> rangeValues = ImmutableMap.builder();
                for (Entry<Key, Value> entry : scanner) {
                    // Remove the cache key that corresponds to this row ID
                    CacheKey cKey = rangeToKey.remove(Range.exact(entry.getKey().getRow()));
                    rangeValues.put(cKey, Long.parseLong(entry.getValue().toString()));
                }

                // Add the remaining cache keys to our return list with a cardinality of zero
                for (CacheKey remainingKey : rangeToKey.values()) {
                    rangeValues.put(remainingKey, 0L);
                }

                return rangeValues.build();
            }
            finally {
                if (scanner != null) {
                    // Don't forget to close your scanner before returning the cardinalities
                    scanner.close();
                }
            }
        }
    }
}
