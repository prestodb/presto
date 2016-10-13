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

import com.facebook.presto.accumulo.AccumuloClient;
import com.facebook.presto.accumulo.conf.AccumuloSessionProperties;
import com.facebook.presto.accumulo.index.metrics.MetricsReader;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.accumulo.model.TabletSplitMetadata;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Class to assist the Presto connector, and maybe external applications,
 * leverage the secondary * index built by the {@link Indexer}.
 * Leverages {@link ColumnCardinalityCache} to assist in * retrieving row IDs.
 * Currently pretty bound to the Presto connector APIs.
 */
public class IndexLookup
{
    private static final Authorizations EMPTY_AUTHS = new Authorizations();
    private static final Logger LOG = Logger.get(IndexLookup.class);

    private final Connector connector;
    private final ColumnCardinalityCache cardinalityCache;
    private final ExecutorService executor;

    public IndexLookup(Connector connector, int cacheSize, Duration cacheExpireDuration)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.cardinalityCache = new ColumnCardinalityCache(cacheSize, requireNonNull(cacheExpireDuration, "cacheExpireDuration is null"));
        AtomicLong threadCount = new AtomicLong(0);
        this.executor = MoreExecutors.getExitingExecutorService(
                new ThreadPoolExecutor(1, 4 * Runtime.getRuntime().availableProcessors(), 60L,
                        TimeUnit.SECONDS, new SynchronousQueue<>(), runnable ->
                        new Thread(runnable, "index-range-scan-thread-" + threadCount.getAndIncrement())
                ));
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
     * @param auths Scan authorizations
     * @param metricsStorage Metrics storage for looking up the cardinality
     * @param truncateTimestamps True if timestamp type metrics are truncated
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
            Authorizations auths,
            MetricsStorage metricsStorage,
            boolean truncateTimestamps)
            throws Exception
    {
        // Early out if index is disabled
        if (!AccumuloSessionProperties.isOptimizeIndexEnabled(session)) {
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
        if (!AccumuloSessionProperties.isIndexMetricsEnabled(session)) {
            LOG.debug("Use of index metrics is disabled");
            // Get the ranges via the index table
            List<Range> indexRanges = getIndexRanges(Indexer.getIndexTableName(schema, table), constraintRanges, rowIdRanges, auths);

            if (!indexRanges.isEmpty()) {
                // Bin the ranges into TabletMetadataSplits and return true to use the tablet splits
                binRanges(AccumuloSessionProperties.getNumIndexRowsPerSplit(session), indexRanges, tabletSplits);
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
            return getRangesWithMetrics(session, schema, table, constraintRanges, rowIdRanges, tabletSplits, auths, metricsStorage, truncateTimestamps);
        }
    }

    private static Multimap<AccumuloColumnConstraint, Range> getIndexedConstraintRanges(Collection<AccumuloColumnConstraint> constraints, AccumuloRowSerializer serializer)
            throws AccumuloSecurityException, AccumuloException
    {
        ImmutableSetMultimap.Builder<AccumuloColumnConstraint, Range> builder = ImmutableSetMultimap.builder();
        constraints.stream()
                .filter(AccumuloColumnConstraint::isIndexed)
                .forEach(constraint -> builder.putAll(constraint, AccumuloClient.getRangesFromDomain(constraint.getDomain(), serializer)));
        return builder.build();
    }

    private boolean getRangesWithMetrics(
            ConnectorSession session,
            String schema,
            String table,
            Multimap<AccumuloColumnConstraint, Range> constraintRanges,
            Collection<Range> rowIdRanges,
            List<TabletSplitMetadata> tabletSplits,
            Authorizations auths,
            MetricsStorage metricsStorage,
            boolean truncateTimestamps)
            throws Exception
    {
        MetricsReader reader = metricsStorage.newReader();
        long numRows = reader.getNumRowsInTable(schema, table);

        // Get the cardinalities from the metrics table
        Multimap<Long, AccumuloColumnConstraint> cardinalities;
        if (AccumuloSessionProperties.isIndexShortCircuitEnabled(session)) {
            cardinalities = cardinalityCache.getCardinalities(
                    schema,
                    table,
                    constraintRanges,
                    auths,
                    getSmallestCardinalityThreshold(session, numRows),
                    AccumuloSessionProperties.getIndexCardinalityCachePollingDuration(session),
                    metricsStorage,
                    truncateTimestamps);
        }
        else {
            // disable short circuit using 0
            cardinalities = cardinalityCache.getCardinalities(
                    schema,
                    table,
                    constraintRanges,
                    auths,
                    0,
                    new Duration(0, TimeUnit.MILLISECONDS),
                    metricsStorage,
                    truncateTimestamps);
        }

        Optional<Entry<Long, AccumuloColumnConstraint>> entry = cardinalities.entries().stream().findFirst();
        if (!entry.isPresent()) {
            return false;
        }

        Entry<Long, AccumuloColumnConstraint> lowestCardinality = entry.get();
        String indexTable = Indexer.getIndexTableName(schema, table);
        double threshold = AccumuloSessionProperties.getIndexThreshold(session);
        List<Range> indexRanges;

        // If the smallest cardinality in our list is above the lowest cardinality threshold,
        // we should look at intersecting the row ID ranges to try and get under the threshold.
        if (smallestCardAboveThreshold(session, numRows, lowestCardinality.getKey())) {
            // If we only have one column, we can skip the intersection process and just check the index threshold
            if (cardinalities.size() == 1) {
                long numEntries = lowestCardinality.getKey();
                double ratio = ((double) numEntries / (double) numRows);
                LOG.debug("Use of index would scan %d of %d rows, ratio %s. Threshold %2f, Using for index table? %b", numEntries, numRows, ratio, threshold, ratio < threshold);
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
            LOG.debug("Not intersecting columns, using column with lowest cardinality");
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
            binRanges(AccumuloSessionProperties.getNumIndexRowsPerSplit(session), indexRanges, tabletSplits);
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
        long threshold = getSmallestCardinalityThreshold(session, numRows);
        LOG.info("Smallest cardinality is %d, num rows is %d, threshold is %d", smallestCardinality, numRows, threshold);
        return smallestCardinality > threshold;
    }

    /**
     * Gets the smallest cardinality threshold, which is the number of rows to skip index intersection
     * if the cardinality is less than or equal to this value.
     * <p>
     * The threshold is the minimum of the percentage-based threshold and the row threshold
     *
     * @param session Current client session
     * @param numRows Number of rows in the table
     * @return Threshold
     */
    private static long getSmallestCardinalityThreshold(ConnectorSession session, long numRows)
    {
        return Math.min((long) (numRows * AccumuloSessionProperties.getIndexSmallCardThreshold(session)),
                AccumuloSessionProperties.getIndexSmallCardRowThreshold(session));
    }

    private List<Range> getIndexRanges(String indexTable, Multimap<AccumuloColumnConstraint, Range> constraintRanges, Collection<Range> rowIDRanges, Authorizations auths)
            throws TableNotFoundException, InterruptedException
    {
        Set<Range> finalRanges = new HashSet<>();
        // For each column/constraint pair we submit a task to scan the index ranges
        List<Callable<Set<Range>>> tasks = new ArrayList<>();
        for (Entry<AccumuloColumnConstraint, Collection<Range>> e : constraintRanges.asMap().entrySet()) {
            Callable<Set<Range>> task = () -> {
                // Create a batch scanner against the index table, setting the ranges
                BatchScanner scanner = connector.createBatchScanner(indexTable, auths, 10);
                scanner.setRanges(e.getValue());

                // Fetch the column family for this specific column
                Text cf = new Text(Indexer.getIndexColumnFamily(e.getKey().getFamily().getBytes(),
                        e.getKey().getQualifier().getBytes()).array());
                scanner.fetchColumnFamily(cf);

                // For each entry in the scanner
                Text tmpQualifier = new Text();
                Set<Range> columnRanges = new HashSet<>();
                for (Entry<Key, Value> entry : scanner) {
                    entry.getKey().getColumnQualifier(tmpQualifier);

                    // Add to our column ranges if it is in one of the row ID ranges
                    if (inRange(tmpQualifier, rowIDRanges)) {
                        columnRanges.add(new Range(tmpQualifier));
                    }
                }

                LOG.info("Retrieved %d ranges for index column %s", columnRanges.size(), e.getKey().getName());
                // Close the scanner
                scanner.close();
                return columnRanges;
            };
            tasks.add(task);
        }

        executor.invokeAll(tasks).forEach(future ->
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
                throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Exception when getting index ranges", e);
            }
        });
        return ImmutableList.copyOf(finalRanges);
    }

    private static void binRanges(int numRangesPerBin, List<Range> splitRanges, List<TabletSplitMetadata> prestoSplits)
    {
        checkArgument(numRangesPerBin > 0, "number of ranges per bin must positivebe greater than zero");
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
        Key qualifier = new Key(text);
        return ranges.stream().anyMatch(r -> !r.beforeStartKey(qualifier) && !r.afterEndKey(qualifier));
    }
}
