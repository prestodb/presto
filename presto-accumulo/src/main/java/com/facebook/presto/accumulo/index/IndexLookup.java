/*
 * Copyright 2016 Bloomberg L.P.
 *
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
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.conf.AccumuloSessionProperties;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.accumulo.model.TabletSplitMetadata;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.accumulo.AccumuloErrorCode.INTERNAL_ERROR;

/**
 * Class to assist the Presto connector, and maybe external applications, leverage the secondary
 * index built by the {@link Indexer}. Leverages {@link ColumnCardinalityCache} to assist in
 * retrieving row IDs. Currently pretty bound to the Presto connector APIs.
 * TODO Decouple from Presto API?
 */
public class IndexLookup
{
    private static final Logger LOG = Logger.get(IndexLookup.class);
    private static final Range METRICS_TABLE_ROWID_RANGE =
            new Range(Indexer.METRICS_TABLE_ROWID_AS_TEXT);

    private final ColumnCardinalityCache ccCache;
    private final Connector conn;
    private final Authorizations auths;
    private final Text tmpCQ = new Text();
    private final ConstraintCardinalityComparator constraintComparator =
            new ConstraintCardinalityComparator();

    /**
     * Creates a new instance of {@link IndexLookup}
     *
     * @param conn Accumulo connector
     * @param config Presto config for Accumulo connector
     * @param auths Authorizations for the scanners
     */
    public IndexLookup(Connector conn, AccumuloConfig config, Authorizations auths)
    {
        this.conn = conn;
        this.auths = auths;
        this.ccCache = new ColumnCardinalityCache(conn, config, auths);
    }

    /**
     * Scans the index table, applying the index based on the given column constraints to return a
     * set of tablet splits.<br>
     * <br>
     * If this function returns true, the output parameter tabletSplits contains a list of
     * TabletSplitMetadata objects. These in turn contain a collection of Ranges containing the
     * exact row IDs determined using the index.
     * <br>
     * If this function returns false, the secondary index should not be used. In this case, either
     * the accumulo session has disabled secondary indexing, or the number of row IDs that would be
     * used by the secondary index is greater than the configured threshold (again retrieved from
     * the session).
     *
     * @param schema Schema name
     * @param table Table name
     * @param session Current client session
     * @param constraints All column constraints (this method will filter for if the column is indexed)
     * @param rowIdRanges Collection of Accumulo ranges based on any predicate against a record key
     * @param tabletSplits Output parameter containing the bundles of row IDs determined by the use of the
     * index.
     * @param serializer Instance of a row serializer
     * @return True if the tablet splits are valid and should be used, false otherwise
     * @throws Exception If something bad happens. What are the odds?
     */
    public boolean applyIndex(String schema, String table, ConnectorSession session,
            Collection<AccumuloColumnConstraint> constraints, Collection<Range> rowIdRanges,
            List<TabletSplitMetadata> tabletSplits, AccumuloRowSerializer serializer)
            throws Exception
    {
        // Early out if index is disabled
        if (!AccumuloSessionProperties.isOptimizeIndexEnabled(session)) {
            LOG.info("Secondary index is disabled");
            return false;
        }

        LOG.info("Secondary index is enabled");

        // Collect Accumulo ranges for each indexed column constraint
        Map<AccumuloColumnConstraint, Collection<Range>> constraintRangePairs = new HashMap<>();
        for (AccumuloColumnConstraint acc : constraints) {
            if (acc.isIndexed()) {
                constraintRangePairs.put(acc,
                        AccumuloClient.getRangesFromDomain(acc.getDomain(), serializer));
            }
            else {
                LOG.warn(
                        "Query containts constraint on non-indexed column %s. Is it worth indexing?",
                        acc.getName());
            }
        }

        // If there is no constraints on an index column, we again will bail out
        if (constraintRangePairs.size() == 0) {
            LOG.info("Query contains no constraints on indexed columns, skipping secondary index");
            return false;
        }

        // If metrics are not enabled
        if (!AccumuloSessionProperties.isIndexMetricsEnabled(session)) {
            LOG.info("Use of index metrics is disabled");
            // Get the ranges via the index table
            final List<Range> idxRanges = getIndexRanges(Indexer.getIndexTableName(schema, table),
                    constraintRangePairs, rowIdRanges);

            if (idxRanges.size() > 0) {
                // Bin the ranges into TabletMetadataSplits and return true to use the tablet splits
                binRanges(AccumuloSessionProperties.getNumIndexRowsPerSplit(session), idxRanges,
                        tabletSplits);
                LOG.info("Number of splits for %s.%s is %d with %d ranges", schema, table,
                        tabletSplits.size(), idxRanges.size());
            }
            else {
                LOG.info("Query would return no results, returning empty list of splits");
            }

            return true;
        }
        else {
            LOG.info("Use of index metrics is enabled");
            // Get ranges using the metrics
            return getRangesWithMetrics(session, schema, table, constraintRangePairs, rowIdRanges,
                    tabletSplits);
        }
    }

    private boolean getRangesWithMetrics(ConnectorSession session, String schema, String table,
            Map<AccumuloColumnConstraint, Collection<Range>> constraintRangePairs,
            Collection<Range> rowIdRanges, List<TabletSplitMetadata> tabletSplits)
            throws Exception
    {
        // Get the cardinalities from the metrics table
        List<Pair<AccumuloColumnConstraint, Long>> cardinalities =
                ccCache.getCardinalities(schema, table, constraintRangePairs);

        // Order by cardinality, ascending
        Collections.sort(cardinalities, constraintComparator);

        // If first entry has cardinality zero, the query would have no results
        if (cardinalities.get(0).getRight() == 0) {
            LOG.info("Query would return no results, returning empty list of splits");
            return true;
        }

        String indexTable = Indexer.getIndexTableName(schema, table);
        String metricsTable = Indexer.getMetricsTableName(schema, table);
        long numRows = getNumRowsInTable(metricsTable);
        double threshold = AccumuloSessionProperties.getIndexThreshold(session);
        final List<Range> idxRanges;

        // If the smallest cardinality in our list is above the lowest cardinality threshold, we
        // should look at intersecting the row ID ranges to try and get under the threshold
        if (smallestCardAboveThreshold(session, numRows, cardinalities)) {
            // If we only have one column, we can skip the intersection process and just check the
            // index threshold
            if (cardinalities.size() == 1) {
                long numEntries = cardinalities.get(0).getRight();
                double ratio = ((double) numEntries / (double) numRows);
                LOG.info(
                        "Use of index would scan %d of %d rows, ratio %s. Threshold %2f, Using for table? %b",
                        numEntries, numRows, ratio, threshold, ratio < threshold);
                if (ratio >= threshold) {
                    return false;
                }
            }

            // Else, get the intersection of all row IDs for all column constraints
            LOG.info("%d indexed columns, intersecting ranges", constraintRangePairs.size());
            idxRanges = getIndexRanges(indexTable, constraintRangePairs, rowIdRanges);
            LOG.info("Intersection results in %d ranges from secondary index", idxRanges.size());
        }
        else {
            // Else, we don't need to intersect the columns and we can just use the column with the
            // lowest cardinality, so get all those row IDs in a set of ranges.
            LOG.info("Not intersecting columns, using column with lowest cardinality ");
            idxRanges =
                    getIndexRanges(indexTable,
                            ImmutableMap.of(cardinalities.get(0).getKey(),
                                    constraintRangePairs.get(cardinalities.get(0).getKey())),
                            rowIdRanges);
        }

        if (idxRanges.size() == 0) {
            LOG.info("Query would return no results, returning empty list of splits");
            return true;
        }

        // Okay, we now check how many rows we would scan by using the index vs. the overall number
        // of rows
        long numEntries = idxRanges.size();
        double ratio = (double) numEntries / (double) numRows;
        LOG.info(
                "Use of index would scan %d of %d rows, ratio %s. Threshold %2f, Using for table? %b",
                numEntries, numRows, ratio, threshold, ratio < threshold, table);

        // If the percentage of scanned rows, the ratio, less than the configured threshold
        if (ratio < threshold) {
            // Bin the ranges into TabletMetadataSplits and return true to use the tablet splits
            binRanges(AccumuloSessionProperties.getNumIndexRowsPerSplit(session), idxRanges,
                    tabletSplits);
            LOG.info("Number of splits for %s.%s is %d with %d ranges", schema, table,
                    tabletSplits.size(), idxRanges.size());
            return true;
        }
        else {
            // We are going to do too much work to use the secondary index, so return false
            return false;
        }
    }

    /**
     * Gets a Boolean value indicating if the smallest cardinality is above the small card threshold
     *
     * @param session Current client session
     * @param numRows Number of rows in the table
     * @param cardinalities Sorted list of cardinalities
     * @return True if the ratio is greater than the configured threshold, false otherwise
     */
    private boolean smallestCardAboveThreshold(ConnectorSession session, long numRows,
            List<Pair<AccumuloColumnConstraint, Long>> cardinalities)
    {
        long lowCard = cardinalities.get(0).getRight();
        double ratio = ((double) lowCard / (double) numRows);
        double threshold = AccumuloSessionProperties.getIndexSmallCardThreshold(session);
        LOG.info("Lowest cardinality is %d, num rows is %d, ratio is %2f with threshold of %f",
                lowCard, numRows, ratio, threshold);
        return ratio > threshold;
    }

    /**
     * Gets the number of rows from the given metrics table
     *
     * @param metricsTable Metrics table to scan for number of rows
     * @return Estimated number of rows in the table
     * @throws TableNotFoundException If the metrics table does not exist
     */
    private long getNumRowsInTable(String metricsTable)
            throws TableNotFoundException
    {
        // Create scanner against the metrics table, pulling the special column and the rows column
        Scanner scan = conn.createScanner(metricsTable, auths);
        scan.setRange(METRICS_TABLE_ROWID_RANGE);
        scan.fetchColumn(Indexer.METRICS_TABLE_ROWS_CF_AS_TEXT, Indexer.CARDINALITY_CQ_AS_TEXT);

        // Scan the entry and get the number of rows
        long numRows = -1;
        for (Entry<Key, Value> entry : scan) {
            if (numRows > 0) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Should have received only one entry when scanning for number of rows in metrics table");
            }
            numRows = Long.parseLong(entry.getValue().toString());
        }
        scan.close();

        LOG.info("Number of rows in table is %d", numRows);
        return numRows;
    }

    /**
     * Gets all index ranges based on the given column constraint/range pairs.
     *
     * @param indexTable Fully-qualified index table name
     * @param constraintRangePairs Mapping of column constraint to Accumulo Ranges for that constraint
     * @param rowIDRanges Collection of Ranges based on any predicate from the row ID. Used to drop and row
     * IDs that are not in one of these Ranges
     * @return A collection of Ranges containing row IDs in the main table to scan
     * @throws TableNotFoundException
     */
    private List<Range> getIndexRanges(String indexTable,
            Map<AccumuloColumnConstraint, Collection<Range>> constraintRangePairs,
            Collection<Range> rowIDRanges)
            throws TableNotFoundException
    {
        Set<Range> finalRanges = null;
        // For each column/constraint pair
        for (Entry<AccumuloColumnConstraint, Collection<Range>> e : constraintRangePairs
                .entrySet()) {
            // Create a batch scanner against the index table, setting the ranges
            BatchScanner scan = conn.createBatchScanner(indexTable, auths, 10);
            scan.setRanges(e.getValue());

            // Fetch the column family for this specific column
            Text cf = new Text(Indexer.getIndexColumnFamily(e.getKey().getFamily().getBytes(),
                    e.getKey().getQualifier().getBytes()).array());
            scan.fetchColumnFamily(cf);

            // For each entry in the scanner
            Set<Range> columnRanges = new HashSet<>();
            for (Entry<Key, Value> entry : scan) {
                entry.getKey().getColumnQualifier(tmpCQ);

                // Add to our column ranges if it is in one of the row ID ranges
                if (inRange(tmpCQ, rowIDRanges)) {
                    columnRanges.add(new Range(tmpCQ));
                }
            }

            LOG.info("Retrieved %d ranges for column %s", columnRanges.size(),
                    e.getKey().getName());

            // If finalRanges is null, we have not yet added any column ranges
            if (finalRanges == null) {
                finalRanges = new HashSet<>();
                finalRanges.addAll(columnRanges);
            }
            else {
                // Retain only the row IDs for this column that have already been added
                // This is your set intersection operation!
                finalRanges.retainAll(columnRanges);
            }

            // Close the scanner
            scan.close();
        }

        // Return the final ranges for all constraint pairs
        return new ArrayList<>(finalRanges);
    }

    /**
     * Bins the given collection of ranges into metadata for the Presto splits
     *
     * @param numRangesPerBin Number of ranges to put in a single metadata object
     * @param splitRanges The ranges to add
     * @param prestoSplits The output collection for the tablet metadata
     */
    private void binRanges(int numRangesPerBin, List<Range> splitRanges,
            List<TabletSplitMetadata> prestoSplits)
    {
        // Shuffle the handles to give an even distribution across all tablet splits
        Collections.shuffle(splitRanges);

        // Location here doesn't matter, I think this 9997 is left over from the connector example
        // Let's leave it to pay homage to where this thing came from
        String loc = "localhost:9997";
        int toAdd = splitRanges.size();
        int fromIndex = 0;
        int toIndex = Math.min(toAdd, numRangesPerBin);
        do {
            // Add the sublist of range handles
            prestoSplits.add(new TabletSplitMetadata(loc, splitRanges.subList(fromIndex, toIndex)));
            toAdd -= toIndex - fromIndex;
            fromIndex = toIndex;
            toIndex += Math.min(toAdd, numRangesPerBin);
        }
        while (toAdd > 0);
    }

    /**
     * Gets a Boolean value indicating if the given value is in one of the Ranges in the given
     * collection
     *
     * @param t Text object to check against the Range collection
     * @param ranges Ranges to look into
     * @return True if the text object is in one of the ranges, false otherwise
     */
    private boolean inRange(Text t, Collection<Range> ranges)
    {
        Key kCq = new Key(t);
        for (Range r : ranges) {
            if (!r.beforeStartKey(kCq) && !r.afterEndKey(kCq)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Internal class for sorting the pairs of column constraints and cardinality
     */
    private class ConstraintCardinalityComparator
            implements Comparator<Pair<AccumuloColumnConstraint, Long>>
    {
        @Override
        public int compare(Pair<AccumuloColumnConstraint, Long> o1,
                Pair<AccumuloColumnConstraint, Long> o2)
        {
            return o1.getRight().compareTo(o2.getRight());
        }
    }
}
