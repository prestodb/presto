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
package com.facebook.presto.accumulo.index.metrics;

import com.facebook.presto.accumulo.AccumuloTableManager;
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.iterators.ValueSummingIterator;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_DNE;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.DAY;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.HOUR;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MINUTE;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.SECOND;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AccumuloMetricsStorage
        extends MetricsStorage
{
    private static final byte[] CARDINALITY_CQ = "___card___".getBytes(UTF_8);
    private static final LongCombiner.Type ENCODER_TYPE = LongCombiner.Type.STRING;
    private static final TypedValueCombiner.Encoder<Long> ENCODER = new LongCombiner.StringEncoder();
    private static final Map<TimestampPrecision, byte[]> TIMESTAMP_CARDINALITY_FAMILIES = ImmutableMap.of(
            SECOND, "_tss".getBytes(UTF_8),
            MINUTE, "_tsm".getBytes(UTF_8),
            HOUR, "_tsh".getBytes(UTF_8),
            DAY, "_tsd".getBytes(UTF_8));

    private final AccumuloTableManager tableManager;

    public AccumuloMetricsStorage(Connector connector)
    {
        super(connector);
        tableManager = new AccumuloTableManager(connector);
    }

    @Override
    public void create(AccumuloTable table)
    {
        String metricsTableName = getMetricsTableName(table.getSchema(), table.getTable());
        if (!tableManager.exists(metricsTableName)) {
            tableManager.createAccumuloTable(metricsTableName);
        }

        tableManager.setLocalityGroups(metricsTableName, getLocalityGroups(table));

        // Attach iterators to metrics table
        for (IteratorSetting setting : getMetricIterators(table)) {
            tableManager.setIterator(metricsTableName, setting);
        }
    }

    private Map<String, Set<Text>> getLocalityGroups(AccumuloTable table)
    {
        ImmutableMap.Builder<String, Set<Text>> groups = ImmutableMap.builder();

        // For each indexed column
        for (AccumuloColumnHandle columnHandle : table.getColumns().stream().filter(AccumuloColumnHandle::isIndexed).collect(Collectors.toList())) {
            // Create a Text version of the index column family
            Text indexColumnFamily = new Text(Indexer.getIndexColumnFamily(columnHandle.getFamily().get().getBytes(UTF_8), columnHandle.getQualifier().get().getBytes(UTF_8)).array());
            groups.put(indexColumnFamily.toString(), ImmutableSet.of(indexColumnFamily));

            if (table.isTruncateTimestamps() && columnHandle.getType() == TIMESTAMP) {
                for (byte[] family : TIMESTAMP_CARDINALITY_FAMILIES.values()) {
                    Text timestampFamily = new Text(Bytes.concat(indexColumnFamily.copyBytes(), family));
                    groups.put(timestampFamily.toString(), ImmutableSet.of(timestampFamily));
                }
            }
        }

        return groups.build();
    }

    @Override
    public void rename(AccumuloTable oldTable, AccumuloTable newTable)
    {
        String oldTableName = getMetricsTableName(oldTable.getSchema(), oldTable.getTable());
        String newTableName = getMetricsTableName(newTable.getSchema(), newTable.getTable());
        tableManager.renameAccumuloTable(oldTableName, newTableName);
    }

    @Override
    public boolean exists(SchemaTableName table)
    {
        String metricsTableName = getMetricsTableName(table.getSchemaName(), table.getTableName());
        return tableManager.exists(metricsTableName);
    }

    @Override
    public void drop(AccumuloTable table)
    {
        if (table.isExternal()) {
            return;
        }

        String metricsTableName = getMetricsTableName(table.getSchema(), table.getTable());
        if (tableManager.exists(metricsTableName)) {
            tableManager.deleteAccumuloTable(metricsTableName);
        }
    }

    @Override
    public MetricsWriter newWriter(AccumuloTable table)
    {
        return new AccumuloMetricsWriter(connector, table);
    }

    @Override
    public MetricsReader newReader()
    {
        return new AccumuloMetricsReader(this, connector);
    }

    /**
     * Gets the fully-qualified index metrics table name for the given table
     *
     * @param schema Schema name
     * @param table Table name
     * @return Qualified index metrics table name
     */
    private static String getMetricsTableName(String schema, String table)
    {
        return schema.equals("default")
                ? table + "_idx_metrics"
                : schema + '.' + table + "_idx_metrics";
    }

    /**
     * Gets a collection of iterator settings that should be added to the metric table for the given
     * Accumulo table. Don't forget! Please!
     *
     * @param table Table for retrieving metrics iterators, see AccumuloClient#getTable
     * @return Collection of iterator settings
     */
    private Collection<IteratorSetting> getMetricIterators(AccumuloTable table)
    {
        String cardQualifier = new String(CARDINALITY_CQ, UTF_8);
        String rowsFamily = new String(METRICS_TABLE_ROWS_COLUMN.array(), UTF_8);

        // Build a string for all columns where the summing combiner should be applied, i.e. all indexed columns
        ImmutableList.Builder<IteratorSetting.Column> columnBuilder = ImmutableList.builder();
        columnBuilder.add(new IteratorSetting.Column(rowsFamily, cardQualifier));
        for (String s : getLocalityGroups(table).keySet()) {
            columnBuilder.add(new IteratorSetting.Column(s, cardQualifier));
        }

        // Summing combiner for cardinality columns
        IteratorSetting s1 = new IteratorSetting(1, SummingCombiner.class);
        SummingCombiner.setEncodingType(s1, LongCombiner.Type.STRING);
        SummingCombiner.setColumns(s1, columnBuilder.build());

        return ImmutableList.of(s1);
    }

    private static class AccumuloMetricsWriter
            extends MetricsWriter
    {
        private static final byte[] CARDINALITY_CQ = "___card___".getBytes(UTF_8);

        private final BatchWriterConfig writerConfig;
        private final Connector connector;

        public AccumuloMetricsWriter(Connector connector, AccumuloTable table)
        {
            super(table);
            this.connector = requireNonNull(connector, "connector is null");
            this.writerConfig = new BatchWriterConfig();
        }

        @Override
        public void flush()
        {
            // Write out metrics mutations
            try {
                Collection<Mutation> mutations = this.getMetricsMutations();
                if (mutations.size() > 0) {
                    BatchWriter metricsWriter = connector.createBatchWriter(getMetricsTableName(table.getSchema(), table.getTable()), writerConfig);
                    metricsWriter.addMutations(mutations);
                    metricsWriter.close();
                }
                metrics.clear();
                timestampMetrics.clear();
            }
            catch (MutationsRejectedException e) {
                throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Mutation was rejected by server on close", e);
            }
            catch (TableNotFoundException e) {
                throw new PrestoException(ACCUMULO_TABLE_DNE, "Accumulo table does not exist", e);
            }
        }

        /**
         * Gets a collection of mutations based on the current metric map
         *
         * @return A collection of Mutations
         */
        private Collection<Mutation> getMetricsMutations()
        {
            ImmutableList.Builder<Mutation> mutationBuilder = ImmutableList.builder();
            // Mapping of column value to column to number of row IDs that contain that value
            for (Map.Entry<CardinalityKey, AtomicLong> entry : metrics.entrySet()) {
                if (entry.getValue().get() != 0) {
                    // Row ID: Column value
                    // Family: columnfamily_columnqualifier
                    // Qualifier: CARDINALITY_CQ
                    // Visibility: Inherited from indexed Mutation
                    // Value: Cardinality

                    Mutation mut = new Mutation(entry.getKey().value.array());
                    mut.put(
                            entry.getKey().column.array(),
                            CARDINALITY_CQ,
                            entry.getKey().visibility,
                            ENCODER.encode(entry.getValue().get()));

                    // Add to our list of mutations
                    mutationBuilder.add(mut);
                }
            }

            for (Map.Entry<TimestampTruncateKey, AtomicLong> entry : timestampMetrics.entrySet()) {
                if (entry.getValue().get() != 0) {
                    Mutation mut = new Mutation(entry.getKey().value.array());
                    mut.put(
                            Bytes.concat(entry.getKey().column.array(), TIMESTAMP_CARDINALITY_FAMILIES.get(entry.getKey().level)),
                            CARDINALITY_CQ,
                            entry.getKey().visibility,
                            ENCODER.encode(entry.getValue().get()));
                    mutationBuilder.add(mut);
                }
            }

            return mutationBuilder.build();
        }
    }

    private static class AccumuloMetricsReader
            extends MetricsReader
    {
        private static final Logger LOG = Logger.get(AccumuloMetricsReader.class);
        private static final Text CARDINALITY_CQ_TEXT = new Text(CARDINALITY_CQ);

        private final Connector connector;
        private final ExecutorService executorService;

        public AccumuloMetricsReader(AccumuloMetricsStorage storage, Connector connector)
        {
            super(storage);
            this.connector = requireNonNull(connector, "connector is null");
            this.executorService = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(0, 5, 60L, SECONDS, new SynchronousQueue<>()));
        }

        @Override
        public long getCardinality(MetricCacheKey key)
                throws Exception
        {
            LOG.debug("Loading a non-exact range from Accumulo: %s", key);
            // Get metrics table name and the column family for the scanner
            String metricsTable = getMetricsTableName(key.schema, key.table);
            Text columnFamily = new Text(super.getColumnFamily(key));

            IteratorSetting setting = new IteratorSetting(Integer.MAX_VALUE, "valuesummingcombiner", ValueSummingIterator.class);
            ValueSummingIterator.setEncodingType(setting, ENCODER_TYPE);

            // Create scanner for querying the range
            if (!key.truncateTimestamps) {
                BatchScanner scanner = null;
                try {
                    scanner = connector.createBatchScanner(metricsTable, key.auths, 10);
                    scanner.setRanges(connector.tableOperations().splitRangeByTablets(metricsTable, key.range, Integer.MAX_VALUE));
                    scanner.fetchColumn(columnFamily, CARDINALITY_CQ_TEXT);
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
                        scanner.close();
                    }
                }
            }
            else {
                // For timestamp columns
                List<Future<Long>> tasks = MetricsStorage.splitTimestampRange(key.range).asMap().entrySet().stream().map(timestampEntry ->
                        executorService.submit(() -> {
                                    long sum = 0;
                                    BatchScanner scanner = null;
                                    try {
                                        scanner = connector.createBatchScanner(metricsTable, key.auths, 10);
                                        scanner.setRanges(timestampEntry.getValue());
                                        scanner.addScanIterator(setting);

                                        if (timestampEntry.getKey() == TimestampPrecision.MILLISECOND) {
                                            scanner.fetchColumn(columnFamily, CARDINALITY_CQ_TEXT);
                                        }
                                        else {
                                            scanner.fetchColumn(new Text(Bytes.concat(columnFamily.copyBytes(), TIMESTAMP_CARDINALITY_FAMILIES.get(timestampEntry.getKey()))), CARDINALITY_CQ_TEXT);
                                        }

                                        // Sum the entries to get the cardinality
                                        for (Entry<Key, Value> entry : scanner) {
                                            sum += Long.parseLong(entry.getValue().toString());
                                        }
                                        LOG.debug("Sum for %s is %s, %s ranges", timestampEntry.getKey(), sum, timestampEntry.getValue().size());
                                        return sum;
                                    }
                                    finally {
                                        if (scanner != null) {
                                            scanner.close();
                                        }
                                    }
                                }
                        )).collect(Collectors.toList());

                long sum = 0;
                for (Future<Long> task : tasks) {
                    sum += task.get();
                }
                LOG.debug("Final sum is %s", sum);
                return sum;
            }
        }

        @Override
        public Map<MetricCacheKey, Long> getCardinalities(Collection<MetricCacheKey> keys)
        {
            if (keys.isEmpty()) {
                return ImmutableMap.of();
            }

            LOG.debug("Loading %s exact ranges from Accumulo", keys.size());

            // Transform the collection into a map of each CacheKey's Range to the key itself
            // This allows us to look up the corresponding CacheKey based on the Row
            // we receive from the scanner, and we can then back-fill our returned map
            // With any values that were not returned by the scan (cardinality zero)
            Map<Range, MetricCacheKey> rangeToKey = new HashMap<>(keys.size());
            keys.forEach(k -> rangeToKey.put(k.range, k));

            // Create a copy of the map which we will use to fill out the zeroes
            Map<Range, MetricCacheKey> remainingKeys = new HashMap<>(rangeToKey);

            MetricCacheKey anyKey = super.getAnyKey(keys);

            // Get metrics table name and the column family for the scanner
            String metricsTable = getMetricsTableName(anyKey.schema, anyKey.table);
            Text columnFamily = new Text(super.getColumnFamily(anyKey));

            // Create batch scanner for querying all ranges
            BatchScanner scanner = null;
            try {
                scanner = connector.createBatchScanner(metricsTable, anyKey.auths, 10);
                scanner.setRanges(keys.stream().map(k -> k.range).collect(Collectors.toList()));
                scanner.fetchColumn(columnFamily, CARDINALITY_CQ_TEXT);

                // Create a new map to hold our cardinalities for each range
                // retrieved from the scanner
                Map<MetricCacheKey, Long> rangeValues = new HashMap<>();
                for (Map.Entry<Key, Value> entry : scanner) {
                    // Convert the row ID into an exact range and get the CacheKey
                    Range range = Range.exact(entry.getKey().getRow());
                    MetricCacheKey cacheKey = rangeToKey.get(range);
                    if (cacheKey == null) {
                        throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "rangeToKey had no entry for " + range);
                    }

                    // Remove this range from remaining keys since we have a value
                    remainingKeys.remove(range);

                    // Sum the values (if a value exists already)
                    Long value = rangeValues.get(cacheKey);
                    rangeValues.put(cacheKey, Long.parseLong(entry.getValue().toString()) + (value == null ? 0 : value));
                }

                // Add the remaining cache keys to our return list with a cardinality of zero
                for (MetricCacheKey remainingKey : remainingKeys.values()) {
                    rangeValues.put(remainingKey, 0L);
                }

                return ImmutableMap.copyOf(rangeValues);
            }
            catch (TableNotFoundException e) {
                throw new PrestoException(ACCUMULO_TABLE_DNE, "Accumulo table does not exist", e);
            }
            finally {
                if (scanner != null) {
                    scanner.close();
                }
            }
        }

        @Override
        public void close()
                throws Exception
        {
            // noop
        }
    }
}
