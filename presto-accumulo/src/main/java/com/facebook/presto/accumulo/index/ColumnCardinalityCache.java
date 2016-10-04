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
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    private final Authorizations auths;
    private static final Logger LOG = Logger.get(ColumnCardinalityCache.class);
    private final Connector connector;
    private final int size;
    private final Duration expireDuration;

    private final Map<String, TableColumnCache> tableToCache = new HashMap<>();

    public ColumnCardinalityCache(
            Connector connector,
            AccumuloConfig config,
            Authorizations auths)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.size = requireNonNull(config, "config is null").getCardinalityCacheSize();
        this.expireDuration = config.getCardinalityCacheExpiration();
        this.auths = requireNonNull(auths, "auths is null");
    }

    /**
     * Deletes any cache for the given table, no-op of table does not exist in the cache
     *
     * @param schema Schema name
     * @param table Table name
     */
    public void deleteCache(String schema, String table)
    {
        LOG.debug("Deleting cache for %s.%s", schema, table);
        if (tableToCache.containsKey(table)) {
            // clear the cache and remove it
            getTableCache(schema, table).clear();
            tableToCache.remove(table);
        }
    }

    /**
     * Gets the cardinality for each {@link AccumuloColumnConstraint}.
     * Given constraints are expected to be indexed! Who knows what would happen if they weren't!
     *
     * @param schema Schema name
     * @param table Table name
     * @param idxConstraintRangePairs Mapping of all ranges for a given constraint
     * @return An immutable multimap of cardinality to column constraint, sorted by cardinality from smallest to largest
     * @throws TableNotFoundException If the metrics table does not exist
     * @throws ExecutionException If another error occurs; I really don't even know anymore.
     */
    public Multimap<Long, AccumuloColumnConstraint> getCardinalities(String schema, String table, Multimap<AccumuloColumnConstraint, Range> idxConstraintRangePairs)
            throws ExecutionException, TableNotFoundException
    {
        // Create a multi map sorted by cardinality, sort columns by name
        TreeMultimap<Long, AccumuloColumnConstraint> cardinalityToConstraints = TreeMultimap.create(
                Long::compare,
                (AccumuloColumnConstraint o1, AccumuloColumnConstraint o2) -> o1.getName().compareTo(o2.getName()));

        for (Entry<AccumuloColumnConstraint, Collection<Range>> entry : idxConstraintRangePairs.asMap().entrySet()) {
            long card = getColumnCardinality(schema, table, entry.getKey(), entry.getValue());
            LOG.debug("Cardinality for column %s is %s", entry.getKey().getName(), card);
            cardinalityToConstraints.put(card, entry.getKey());
        }

        return ImmutableMultimap.copyOf(cardinalityToConstraints);
    }

    /**
     * Gets the cardinality for the given column constraint with the given Ranges.
     * Ranges can be exact values or a range of values.
     *
     * @param schema Schema name
     * @param table Table name
     * @param columnConstraint Mapping of all ranges for a given constraint
     * @param indexRanges Ranges for each exact or ranged value of the column constraint
     * @return The cardinality for the column
     * @throws TableNotFoundException If the metrics table does not exist
     * @throws ExecutionException If another error occurs; I really don't even know anymore.
     */
    private long getColumnCardinality(String schema, String table, AccumuloColumnConstraint columnConstraint, Collection<Range> indexRanges)
            throws ExecutionException, TableNotFoundException
    {
        return getTableCache(schema, table)
                .getColumnCardinality(
                        columnConstraint.getName(),
                        columnConstraint.getFamily(),
                        columnConstraint.getQualifier(),
                        indexRanges);
    }

    /**
     * Gets the {@link TableColumnCache} for the given table, creating a new one if necessary.
     *
     * @param schema Schema name
     * @param table Table name
     * @return An existing or new TableColumnCache
     */
    private TableColumnCache getTableCache(String schema, String table)
    {
        String fullName = AccumuloTable.getFullTableName(schema, table);
        TableColumnCache cache = tableToCache.get(fullName);
        if (cache == null) {
            LOG.debug("Creating new TableColumnCache for %s.%s %s", schema, table, this);
            cache = new TableColumnCache(schema, table);
            tableToCache.put(fullName, cache);
        }
        return cache;
    }

    /**
     * Internal class for holding the mapping of column names to the LoadingCache
     */
    private class TableColumnCache
    {
        private final Map<String, LoadingCache<Range, Long>> columnToCache = new HashMap<>();
        private final String schema;
        private final String table;

        public TableColumnCache(String schema,
                String table)
        {
            this.schema = schema;
            this.table = table;
        }

        /**
         * Clears and removes all caches as if the object had been first created
         */
        public void clear()
        {
            columnToCache.values().forEach(LoadingCache::invalidateAll);
            columnToCache.clear();
        }

        /**
         * Gets the column cardinality for all of the given range values.
         * May reach out to the metrics table in Accumulo to retrieve new cache elements.
         *
         * @param column Presto column name
         * @param family Accumulo column family
         * @param qualifier Accumulo column qualifier
         * @param colValues All range values to summarize for the cardinality
         * @return The cardinality of the column
         */
        public long getColumnCardinality(String column, String family, String qualifier, Collection<Range> colValues)
                throws ExecutionException, TableNotFoundException
        {
            // Get the column cache for this column, creating a new one if necessary
            LoadingCache<Range, Long> cache = columnToCache.get(column);
            if (cache == null) {
                cache = newCache(schema, table, family, qualifier);
                columnToCache.put(column, cache);
            }

            // Collect all exact Accumulo Ranges, i.e. single value entries vs. a full scan
            Collection<Range> exactRanges = colValues.stream().filter(this::isExact).collect(Collectors.toList());
            LOG.debug("Column values contain %s exact ranges of %s", exactRanges.size(), colValues.size());

            // Sum the cardinalities for the exact-value Ranges
            // This is where the reach-out to Accumulo occurs for all Ranges that have not previously been fetched
            long sum = 0;
            for (Long value : cache.getAll(exactRanges).values()) {
                sum += value;
            }

            // If these collection sizes are not equal, then there is at least one non-exact range
            if (exactRanges.size() != colValues.size()) {
                // for each range in the column value
                for (Range range : colValues) {
                    // if this range is not exact
                    if (!isExact(range)) {
                        // Then get the value for this range using the single-value cache lookup
                        long value = cache.get(range);

                        // add our value to the cache and our sum
                        cache.put(range, value);
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

        private LoadingCache<Range, Long> newCache(String schema, String table, String family, String qualifier)
        {
            LOG.debug("Created new cache for %s.%s, column %s:%s, size %s expiry %s", schema, table, family, qualifier, size, expireDuration);
            return CacheBuilder
                    .newBuilder()
                    .maximumSize(size)
                    .expireAfterWrite(expireDuration.toMillis(), TimeUnit.MILLISECONDS)
                    .build(new CardinalityCacheLoader(schema, table, family, qualifier));
        }
    }

    /**
     * Internal class for loading the cardinality from Accumulo
     */
    private class CardinalityCacheLoader
            extends CacheLoader<Range, Long>
    {
        private final String metricsTable;
        private final Text columnFamily;

        public CardinalityCacheLoader(
                String schema,
                String table,
                String family,
                String qualifier)
        {
            this.metricsTable = Indexer.getMetricsTableName(schema, table);

            // Create the column family for our scanners
            this.columnFamily = new Text(Indexer.getIndexColumnFamily(family.getBytes(UTF_8), qualifier.getBytes(UTF_8)).array());
        }

        /**
         * Loads the cardinality for the given Range. Uses a BatchScanner and sums the cardinality for all values that encapsulate the Range.
         *
         * @param key Range to get the cardinality for
         * @return The cardinality of the column, which would be zero if the value does not exist
         */
        @Override
        public Long load(Range key)
                throws Exception
        {
            // Create a BatchScanner against our metrics table, setting the value range and fetching the appropriate column
            BatchScanner scanner = connector.createBatchScanner(metricsTable, auths, 10);
            scanner.setRanges(ImmutableList.of(key));
            scanner.fetchColumn(columnFamily, Indexer.CARDINALITY_CQ_AS_TEXT);

            // Sum all those entries!
            long sum = 0;
            for (Entry<Key, Value> entry : scanner) {
                sum += Long.parseLong(entry.getValue().toString());
            }

            scanner.close();
            return sum;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<Range, Long> loadAll(Iterable<? extends Range> keys)
                throws Exception
        {
            LOG.debug("Loading %s exact ranges from Accumulo", ((Collection<Range>) keys).size());

            // Create batch scanner for querying all ranges
            BatchScanner scanner = connector.createBatchScanner(metricsTable, auths, 10);
            scanner.setRanges((Collection<Range>) keys);
            scanner.fetchColumn(columnFamily, Indexer.CARDINALITY_CQ_AS_TEXT);

            // Create a new map to hold our cardinalities for each range, returning a default of zero for each non-existent Key
            Map<Range, Long> rangeValues = new MapDefaultZero();
            for (Entry<Key, Value> entry : scanner) {
                rangeValues.put(
                        Range.exact(entry.getKey().getRow()),
                        Long.parseLong(entry.getValue().toString()));
            }

            scanner.close();
            return rangeValues;
        }

        /**
         * We extend HashMap here and override get to return a value of zero if the key is not in the map.
         * This mitigates the CacheLoader InvalidCacheLoadException if loadAll fails to return a value for a given key,
         * which occurs when there is no key in Accumulo.
         */
        public class MapDefaultZero
                extends HashMap<Range, Long>
        {
            @Override
            public Long get(Object key)
            {
                // Get the key from our map overlord
                Long value = super.get(key);

                // Return zero if null
                return value == null ? 0 : value;
            }
        }
    }
}
