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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.accumulo.model.TabletSplitMetadata;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.airlift.units.Duration;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.accumulo.AccumuloClient.getRangesFromDomain;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexCardinalityCachePollingDuration;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexSmallCardThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getIndexThreshold;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.getNumIndexRowsPerSplit;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isIndexMetricsEnabled;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isIndexShortCircuitEnabled;
import static com.facebook.presto.accumulo.conf.AccumuloSessionProperties.isOptimizeIndexEnabled;
import static com.facebook.presto.accumulo.index.Indexer.CARDINALITY_CQ_AS_TEXT;
import static com.facebook.presto.accumulo.index.Indexer.METRICS_TABLE_ROWID_AS_TEXT;
import static com.facebook.presto.accumulo.index.Indexer.METRICS_TABLE_ROWS_CF_AS_TEXT;
import static com.facebook.presto.accumulo.index.Indexer.getIndexTableName;
import static com.facebook.presto.accumulo.index.Indexer.getMetricsTableName;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

/**
 * Class to assist the Presto connector, and maybe external applications,
 * leverage the secondary * index built by the {@link Indexer}.
 * Leverages {@link ColumnCardinalityCache} to assist in * retrieving row IDs.
 * Currently pretty bound to the Presto connector APIs.
 */
public class IndexLookup
{
    private static final Logger LOG = Logger.get(IndexLookup.class);
    private static final Range METRICS_TABLE_ROWID_RANGE = new Range(METRICS_TABLE_ROWID_AS_TEXT);
    private final ColumnCardinalityCache cardinalityCache;
    private final Connector connector;
    private final ExecutorService coreExecutor;
    private final BoundedExecutor executorService;

    @Inject
    public IndexLookup(Connector connector, ColumnCardinalityCache cardinalityCache)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.cardinalityCache = requireNonNull(cardinalityCache, "cardinalityCache is null");

        // Create a bounded executor with a pool size at 4x number of processors
        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("cardinality-lookup-%s"));
        this.executorService = new BoundedExecutor(coreExecutor, 4 * Runtime.getRuntime().availableProcessors());
    }

    @PreDestroy
    public void shutdown()
    {
        coreExecutor.shutdownNow();
    }

    /**
     * Scans the index table, applying the index based on the given column constraints to return a set of tablet splits.
     * <p>
     * If this function returns true, the output parameter tabletSplits contains a list of TabletSplitMetadata objects.
     * These in turn contain a collection of Ranges containing the exact row IDs determined using the index.
     * <p>
     * If this function returns false, the secondary index should not be used. In this case,
     * either the accumulo session has disabled secondary indexing,
     * or the number of row IDs that would be used by the secondary index is greater than the configured threshold
     * (again retrieved from the session).
     *
     * @param schema Schema name
     * @param table Table name
     * @param session Current client session
     * @param constraints All column constraints (this method will filter for if the column is indexed)
     * @param rowIdRanges Collection of Accumulo ranges based on any predicate against a record key
     * @param tabletSplits Output parameter containing the bundles of row IDs determined by the use of the index.
     * @param serializer Instance of a row serializer
     * @param auths Scan-time authorizations
     * @return True if the tablet splits are valid and should be used, false otherwise
     * @throws Exception If something bad happens. What are the odds?
     */
    public boolean applyIndex(
            String schema,
            String table,
            ConnectorSession session,
            Collection<AccumuloColumnConstraint> constraints,
            Collection<Range> rowIdRanges,
            List<TabletSplitMetadata> tabletSplits,
            AccumuloRowSerializer serializer,
            Authorizations auths)
            throws Exception
    {
        // Early out if index is disabled
        if (!isOptimizeIndexEnabled(session)) {
            LOG.debug("Secondary index is disabled");
            return false;
        }

        LOG.debug("Secondary index is enabled");

        // Collect Accumulo ranges for each indexed column constraint
        Multimap<AccumuloColumnConstraint, Range> constraintRanges = getIndexedConstraintRanges(constraints, serializer);

        // If there is no constraints on an index column, we again will bail out
        if (constraintRanges.isEmpty()) {
            LOG.debug("Query contains no constraints on indexed columns, skipping secondary index");
            return false;
        }

        // If metrics are not enabled
        if (!isIndexMetricsEnabled(session)) {
            LOG.debug("Use of index metrics is disabled");
            // Get the ranges via the index table
            List<Range> indexRanges = getIndexRanges(getIndexTableName(schema, table), constraintRanges, rowIdRanges, auths);

            if (!indexRanges.isEmpty()) {
                // Bin the ranges into TabletMetadataSplits and return true to use the tablet splits
                binRanges(getNumIndexRowsPerSplit(session), indexRanges, tabletSplits);
                LOG.debug("Number of splits for %s.%s is %d with %d ranges", schema, table, tabletSplits.size(), indexRanges.size());
            }
            else {
                LOG.debug("Query would return no results, returning empty list of splits");
            }

            return true;
        }
        else {
            LOG.debug("Use of index metrics is enabled");
            // Get ranges using the metrics
            return getRangesWithMetrics(session, schema, table, constraintRanges, rowIdRanges, tabletSplits, auths);
        }
    }

    private static Multimap<AccumuloColumnConstraint, Range> getIndexedConstraintRanges(Collection<AccumuloColumnConstraint> constraints, AccumuloRowSerializer serializer)
    {
        ImmutableListMultimap.Builder<AccumuloColumnConstraint, Range> builder = ImmutableListMultimap.builder();
        for (AccumuloColumnConstraint columnConstraint : constraints) {
            if (columnConstraint.isIndexed()) {
                for (Range range : getRangesFromDomain(columnConstraint.getDomain(), serializer)) {
                    builder.put(columnConstraint, range);
                }
            }
            else {
                LOG.warn("Query contains constraint on non-indexed column %s. Is it worth indexing?", columnConstraint.getName());
            }
        }
        return builder.build();
    }

    private boolean getRangesWithMetrics(
            ConnectorSession session,
            String schema,
            String table,
            Multimap<AccumuloColumnConstraint, Range> constraintRanges,
            Collection<Range> rowIdRanges,
            List<TabletSplitMetadata> tabletSplits,
            Authorizations auths)
            throws Exception
    {
        String metricsTable = getMetricsTableName(schema, table);
        long numRows = getNumRowsInTable(metricsTable, auths);

        // Get the cardinalities from the metrics table
        Multimap<Long, AccumuloColumnConstraint> cardinalities;
        if (isIndexShortCircuitEnabled(session)) {
            cardinalities = cardinalityCache.getCardinalities(
                    schema,
                    table,
                    auths,
                    constraintRanges,
                    (long) (numRows * getIndexSmallCardThreshold(session)),
                    getIndexCardinalityCachePollingDuration(session));
        }
        else {
            // disable short circuit using 0
            cardinalities = cardinalityCache.getCardinalities(schema, table, auths, constraintRanges, 0, new Duration(0, TimeUnit.MILLISECONDS));
        }

        Optional<Entry<Long, AccumuloColumnConstraint>> entry = cardinalities.entries().stream().findFirst();
        if (!entry.isPresent()) {
            return false;
        }

        Entry<Long, AccumuloColumnConstraint> lowestCardinality = entry.get();
        String indexTable = getIndexTableName(schema, table);
        double threshold = getIndexThreshold(session);
        List<Range> indexRanges;

        // If the smallest cardinality in our list is above the lowest cardinality threshold,
        // we should look at intersecting the row ID ranges to try and get under the threshold.
        if (smallestCardAboveThreshold(session, numRows, lowestCardinality.getKey())) {
            // If we only have one column, we can skip the intersection process and just check the index threshold
            if (cardinalities.size() == 1) {
                long numEntries = lowestCardinality.getKey();
                double ratio = ((double) numEntries / (double) numRows);
                LOG.debug("Use of index would scan %s of %s rows, ratio %s. Threshold %2f, Using for index table? %s", numEntries, numRows, ratio, threshold, ratio < threshold);
                if (ratio >= threshold) {
                    return false;
                }
            }

            // Else, get the intersection of all row IDs for all column constraints
            LOG.debug("%d indexed columns, intersecting ranges", constraintRanges.size());
            indexRanges = getIndexRanges(indexTable, constraintRanges, rowIdRanges, auths);
            LOG.debug("Intersection results in %d ranges from secondary index", indexRanges.size());
        }
        else {
            // Else, we don't need to intersect the columns and we can just use the column with the lowest cardinality,
            // so get all those row IDs in a set of ranges.
            LOG.debug("Not intersecting columns, using column with lowest cardinality ");
            ImmutableMultimap.Builder<AccumuloColumnConstraint, Range> lcBldr = ImmutableMultimap.builder();
            lcBldr.putAll(lowestCardinality.getValue(), constraintRanges.get(lowestCardinality.getValue()));
            indexRanges = getIndexRanges(indexTable, lcBldr.build(), rowIdRanges, auths);
        }

        if (indexRanges.isEmpty()) {
            LOG.debug("Query would return no results, returning empty list of splits");
            return true;
        }

        // Okay, we now check how many rows we would scan by using the index vs. the overall number
        // of rows
        long numEntries = indexRanges.size();
        double ratio = (double) numEntries / (double) numRows;
        LOG.debug("Use of index would scan %d of %d rows, ratio %s. Threshold %2f, Using for table? %b", numEntries, numRows, ratio, threshold, ratio < threshold, table);

        // If the percentage of scanned rows, the ratio, less than the configured threshold
        if (ratio < threshold) {
            // Bin the ranges into TabletMetadataSplits and return true to use the tablet splits
            binRanges(getNumIndexRowsPerSplit(session), indexRanges, tabletSplits);
            LOG.debug("Number of splits for %s.%s is %d with %d ranges", schema, table, tabletSplits.size(), indexRanges.size());
            return true;
        }
        else {
            // We are going to do too much work to use the secondary index, so return false
            return false;
        }
    }

    private static boolean smallestCardAboveThreshold(ConnectorSession session, long numRows, long smallestCardinality)
    {
        double ratio = ((double) smallestCardinality / (double) numRows);
        double threshold = getIndexSmallCardThreshold(session);
        LOG.debug("Smallest cardinality is %d, num rows is %d, ratio is %2f with threshold of %f", smallestCardinality, numRows, ratio, threshold);
        return ratio > threshold;
    }

    private long getNumRowsInTable(String metricsTable, Authorizations auths)
            throws TableNotFoundException
    {
        // Create scanner against the metrics table, pulling the special column and the rows column
        Scanner scanner = connector.createScanner(metricsTable, auths);
        scanner.setRange(METRICS_TABLE_ROWID_RANGE);
        scanner.fetchColumn(METRICS_TABLE_ROWS_CF_AS_TEXT, CARDINALITY_CQ_AS_TEXT);

        // Scan the entry and get the number of rows
        long numRows = -1;
        for (Entry<Key, Value> entry : scanner) {
            if (numRows > 0) {
                throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Should have received only one entry when scanning for number of rows in metrics table");
            }
            numRows = Long.parseLong(entry.getValue().toString());
        }
        scanner.close();

        LOG.debug("Number of rows in table is %d", numRows);
        return numRows;
    }

    private List<Range> getIndexRanges(String indexTable, Multimap<AccumuloColumnConstraint, Range> constraintRanges, Collection<Range> rowIDRanges, Authorizations auths)
    {
        Set<Range> finalRanges = new HashSet<>();
        // For each column/constraint pair we submit a task to scan the index ranges
        List<Future<Set<Range>>> tasks = new ArrayList<>();
        CompletionService<Set<Range>> executor = new ExecutorCompletionService<>(executorService);
        for (Entry<AccumuloColumnConstraint, Collection<Range>> constraintEntry : constraintRanges.asMap().entrySet()) {
            tasks.add(executor.submit(() -> {
                // Create a batch scanner against the index table, setting the ranges
                BatchScanner scan = connector.createBatchScanner(indexTable, auths, 10);
                scan.setRanges(constraintEntry.getValue());

                // Fetch the column family for this specific column
                scan.fetchColumnFamily(new Text(Indexer.getIndexColumnFamily(constraintEntry.getKey().getFamily().getBytes(), constraintEntry.getKey().getQualifier().getBytes()).array()));

                // For each entry in the scanner
                Text tmpQualifier = new Text();
                Set<Range> columnRanges = new HashSet<>();
                for (Entry<Key, Value> entry : scan) {
                    entry.getKey().getColumnQualifier(tmpQualifier);

                    // Add to our column ranges if it is in one of the row ID ranges
                    if (inRange(tmpQualifier, rowIDRanges)) {
                        columnRanges.add(new Range(tmpQualifier));
                    }
                }

                LOG.debug("Retrieved %d ranges for index column %s", columnRanges.size(), constraintEntry.getKey().getName());
                scan.close();
                return columnRanges;
            }));
        }
        tasks.forEach(future ->
        {
            try {
                // If finalRanges is null, we have not yet added any column ranges
                if (finalRanges.isEmpty()) {
                    finalRanges.addAll(future.get());
                }
                else {
                    // Retain only the row IDs for this column that have already been added
                    // This is your set intersection operation!
                    finalRanges.retainAll(future.get());
                }
            }
            catch (ExecutionException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Exception when getting index ranges", e.getCause());
            }
        });
        return ImmutableList.copyOf(finalRanges);
    }

    private static void binRanges(int numRangesPerBin, List<Range> splitRanges, List<TabletSplitMetadata> prestoSplits)
    {
        checkArgument(numRangesPerBin > 0, "number of ranges per bin must be greater than zero");
        int toAdd = splitRanges.size();
        int fromIndex = 0;
        int toIndex = Math.min(toAdd, numRangesPerBin);
        do {
            // Add the sublist of range handles
            // Use an empty location because we are binning multiple Ranges spread across many tablet servers
            prestoSplits.add(new TabletSplitMetadata(Optional.empty(), splitRanges.subList(fromIndex, toIndex)));
            toAdd -= toIndex - fromIndex;
            fromIndex = toIndex;
            toIndex += Math.min(toAdd, numRangesPerBin);
        }
        while (toAdd > 0);
    }

    /**
     * Gets a Boolean value indicating if the given value is in one of the Ranges in the given collection
     *
     * @param text Text object to check against the Range collection
     * @param ranges Ranges to look into
     * @return True if the text object is in one of the ranges, false otherwise
     */
    private static boolean inRange(Text text, Collection<Range> ranges)
    {
        Key kCq = new Key(text);
        return ranges.stream().anyMatch(r -> !r.beforeStartKey(kCq) && !r.afterEndKey(kCq));
    }
}
