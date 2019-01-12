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
package io.prestosql.plugin.accumulo.index;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.accumulo.conf.AccumuloConfig;
import io.prestosql.plugin.accumulo.model.AccumuloColumnConstraint;
import io.prestosql.spi.PrestoException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

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
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Streams.stream;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.plugin.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static io.prestosql.plugin.accumulo.index.Indexer.CARDINALITY_CQ_AS_TEXT;
import static io.prestosql.plugin.accumulo.index.Indexer.getIndexColumnFamily;
import static io.prestosql.plugin.accumulo.index.Indexer.getMetricsTableName;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
    private final ExecutorService coreExecutor;
    private final BoundedExecutor executorService;
    private final LoadingCache<CacheKey, Long> cache;

    @Inject
    public ColumnCardinalityCache(Connector connector, AccumuloConfig config)
    {
        this.connector = requireNonNull(connector, "connector is null");
        int size = requireNonNull(config, "config is null").getCardinalityCacheSize();
        Duration expireDuration = config.getCardinalityCacheExpiration();

        // Create a bounded executor with a pool size at 4x number of processors
        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("cardinality-lookup-%s"));
        this.executorService = new BoundedExecutor(coreExecutor, 4 * Runtime.getRuntime().availableProcessors());

        LOG.debug("Created new cache size %d expiry %s", size, expireDuration);
        cache = CacheBuilder.newBuilder()
                .maximumSize(size)
                .expireAfterWrite(expireDuration.toMillis(), MILLISECONDS)
                .build(new CardinalityCacheLoader());
    }

    @PreDestroy
    public void shutdown()
    {
        coreExecutor.shutdownNow();
    }

    /**
     * Gets the cardinality for each {@link AccumuloColumnConstraint}.
     * Given constraints are expected to be indexed! Who knows what would happen if they weren't!
     *
     * @param schema Schema name
     * @param table Table name
     * @param auths Scan authorizations
     * @param idxConstraintRangePairs Mapping of all ranges for a given constraint
     * @param earlyReturnThreshold Smallest acceptable cardinality to return early while other tasks complete
     * @param pollingDuration Duration for polling the cardinality completion service
     * @return An immutable multimap of cardinality to column constraint, sorted by cardinality from smallest to largest
     * @throws TableNotFoundException If the metrics table does not exist
     * @throws ExecutionException If another error occurs; I really don't even know anymore.
     */
    public Multimap<Long, AccumuloColumnConstraint> getCardinalities(String schema, String table, Authorizations auths, Multimap<AccumuloColumnConstraint, Range> idxConstraintRangePairs, long earlyReturnThreshold, Duration pollingDuration)
    {
        // Submit tasks to the executor to fetch column cardinality, adding it to the Guava cache if necessary
        CompletionService<Pair<Long, AccumuloColumnConstraint>> executor = new ExecutorCompletionService<>(executorService);
        idxConstraintRangePairs.asMap().forEach((key, value) -> executor.submit(() -> {
            long cardinality = getColumnCardinality(schema, table, auths, key.getFamily(), key.getQualifier(), value);
            LOG.debug("Cardinality for column %s is %s", key.getName(), cardinality);
            return Pair.of(cardinality, key);
        }));

        // Create a multi map sorted by cardinality
        ListMultimap<Long, AccumuloColumnConstraint> cardinalityToConstraints = MultimapBuilder.treeKeys().arrayListValues().build();
        try {
            boolean earlyReturn = false;
            int numTasks = idxConstraintRangePairs.asMap().entrySet().size();
            do {
                // Sleep for the polling duration to allow concurrent tasks to run for this time
                Thread.sleep(pollingDuration.toMillis());

                // Poll each task, retrieving the result if it is done
                for (int i = 0; i < numTasks; ++i) {
                    Future<Pair<Long, AccumuloColumnConstraint>> futureCardinality = executor.poll();
                    if (futureCardinality != null && futureCardinality.isDone()) {
                        Pair<Long, AccumuloColumnConstraint> columnCardinality = futureCardinality.get();
                        cardinalityToConstraints.put(columnCardinality.getLeft(), columnCardinality.getRight());
                    }
                }

                // If the smallest cardinality is present and below the threshold, set the earlyReturn flag
                Optional<Entry<Long, AccumuloColumnConstraint>> smallestCardinality = cardinalityToConstraints.entries().stream().findFirst();
                if (smallestCardinality.isPresent()) {
                    if (smallestCardinality.get().getKey() <= earlyReturnThreshold) {
                        LOG.info("Cardinality %s, is below threshold. Returning early while other tasks finish", smallestCardinality);
                        earlyReturn = true;
                    }
                }
            }
            while (!earlyReturn && cardinalityToConstraints.entries().size() < numTasks);
        }
        catch (ExecutionException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
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
     * @param auths Scan authorizations
     * @param family Accumulo column family
     * @param qualifier Accumulo column qualifier
     * @param colValues All range values to summarize for the cardinality
     * @return The cardinality of the column
     */
    public long getColumnCardinality(String schema, String table, Authorizations auths, String family, String qualifier, Collection<Range> colValues)
            throws ExecutionException
    {
        LOG.debug("Getting cardinality for %s:%s", family, qualifier);

        // Collect all exact Accumulo Ranges, i.e. single value entries vs. a full scan
        Collection<CacheKey> exactRanges = colValues.stream()
                .filter(ColumnCardinalityCache::isExact)
                .map(range -> new CacheKey(schema, table, family, qualifier, range, auths))
                .collect(Collectors.toList());

        LOG.debug("Column values contain %s exact ranges of %s", exactRanges.size(), colValues.size());

        // Sum the cardinalities for the exact-value Ranges
        // This is where the reach-out to Accumulo occurs for all Ranges that have not
        // previously been fetched
        long sum = cache.getAll(exactRanges).values().stream().mapToLong(Long::longValue).sum();

        // If these collection sizes are not equal,
        // then there is at least one non-exact range
        if (exactRanges.size() != colValues.size()) {
            // for each range in the column value
            for (Range range : colValues) {
                // if this range is not exact
                if (!isExact(range)) {
                    // Then get the value for this range using the single-value cache lookup
                    sum += cache.get(new CacheKey(schema, table, family, qualifier, range, auths));
                }
            }
        }

        return sum;
    }

    private static boolean isExact(Range range)
    {
        return !range.isInfiniteStartKey() && !range.isInfiniteStopKey() &&
                range.getStartKey().followingKey(PartialKey.ROW).equals(range.getEndKey());
    }

    /**
     * Complex key for the CacheLoader
     */
    private static class CacheKey
    {
        private final String schema;
        private final String table;
        private final String family;
        private final String qualifier;
        private final Range range;
        private final Authorizations auths;

        public CacheKey(
                String schema,
                String table,
                String family,
                String qualifier,
                Range range,
                Authorizations auths)
        {
            this.schema = requireNonNull(schema, "schema is null");
            this.table = requireNonNull(table, "table is null");
            this.family = requireNonNull(family, "family is null");
            this.qualifier = requireNonNull(qualifier, "qualifier is null");
            this.range = requireNonNull(range, "range is null");
            this.auths = requireNonNull(auths, "auths is null");
        }

        public String getSchema()
        {
            return schema;
        }

        public String getTable()
        {
            return table;
        }

        public String getFamily()
        {
            return family;
        }

        public String getQualifier()
        {
            return qualifier;
        }

        public Range getRange()
        {
            return range;
        }

        public Authorizations getAuths()
        {
            return auths;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(schema, table, family, qualifier, range);
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
            return Objects.equals(this.schema, other.schema)
                    && Objects.equals(this.table, other.table)
                    && Objects.equals(this.family, other.family)
                    && Objects.equals(this.qualifier, other.qualifier)
                    && Objects.equals(this.range, other.range);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("schema", schema)
                    .add("table", table)
                    .add("family", family)
                    .add("qualifier", qualifier)
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
            String metricsTable = getMetricsTableName(key.getSchema(), key.getTable());
            Text columnFamily = new Text(getIndexColumnFamily(key.getFamily().getBytes(UTF_8), key.getQualifier().getBytes(UTF_8)).array());

            // Create scanner for querying the range
            BatchScanner scanner = connector.createBatchScanner(metricsTable, key.auths, 10);
            scanner.setRanges(connector.tableOperations().splitRangeByTablets(metricsTable, key.range, Integer.MAX_VALUE));
            scanner.fetchColumn(columnFamily, CARDINALITY_CQ_AS_TEXT);

            try {
                return stream(scanner)
                        .map(Entry::getValue)
                        .map(Value::toString)
                        .mapToLong(Long::parseLong)
                        .sum();
            }
            finally {
                scanner.close();
            }
        }

        @Override
        public Map<CacheKey, Long> loadAll(Iterable<? extends CacheKey> keys)
                throws Exception
        {
            int size = Iterables.size(keys);
            if (size == 0) {
                return ImmutableMap.of();
            }

            LOG.debug("Loading %s exact ranges from Accumulo", size);

            // In order to simplify the implementation, we are making a (safe) assumption
            // that the CacheKeys will all contain the same combination of schema/table/family/qualifier
            // This is asserted with the below implementation error just to make sure
            CacheKey anyKey = stream(keys).findAny().get();
            if (stream(keys).anyMatch(k -> !k.getSchema().equals(anyKey.getSchema()) || !k.getTable().equals(anyKey.getTable()) || !k.getFamily().equals(anyKey.getFamily()) || !k.getQualifier().equals(anyKey.getQualifier()))) {
                throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "loadAll called with a non-homogeneous collection of cache keys");
            }

            Map<Range, CacheKey> rangeToKey = stream(keys).collect(Collectors.toMap(CacheKey::getRange, Function.identity()));
            LOG.debug("rangeToKey size is %s", rangeToKey.size());

            // Get metrics table name and the column family for the scanner
            String metricsTable = getMetricsTableName(anyKey.getSchema(), anyKey.getTable());
            Text columnFamily = new Text(getIndexColumnFamily(anyKey.getFamily().getBytes(UTF_8), anyKey.getQualifier().getBytes(UTF_8)).array());

            BatchScanner scanner = connector.createBatchScanner(metricsTable, anyKey.getAuths(), 10);
            try {
                scanner.setRanges(stream(keys).map(CacheKey::getRange).collect(Collectors.toList()));
                scanner.fetchColumn(columnFamily, CARDINALITY_CQ_AS_TEXT);

                // Create a new map to hold our cardinalities for each range, returning a default of
                // Zero for each non-existent Key
                Map<CacheKey, Long> rangeValues = new HashMap<>();
                stream(keys).forEach(key -> rangeValues.put(key, 0L));

                for (Entry<Key, Value> entry : scanner) {
                    rangeValues.put(rangeToKey.get(Range.exact(entry.getKey().getRow())), parseLong(entry.getValue().toString()));
                }

                return rangeValues;
            }
            finally {
                if (scanner != null) {
                    scanner.close();
                }
            }
        }
    }
}
