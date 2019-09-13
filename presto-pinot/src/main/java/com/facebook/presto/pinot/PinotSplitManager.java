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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_FAILURE_GETTING_TABLE;
import static com.facebook.presto.pinot.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class PinotSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(PinotSplitManager.class);

    private final String connectorId;
    private final PinotConfig pinotConfig;
    private final PinotConnection pinotPrestoConnection;

    @Inject
    public PinotSplitManager(PinotConnectorId connectorId, PinotConfig pinotConfig, PinotConnection pinotPrestoConnection)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotConfig = pinotConfig;
        this.pinotPrestoConnection = requireNonNull(pinotPrestoConnection, "pinotPrestoConnection is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        PinotTableLayoutHandle layoutHandle = checkType(layout, PinotTableLayoutHandle.class, "layout");
        PinotTableHandle tableHandle = layoutHandle.getTable();
        PinotTable table = null;
        PinotColumn timeColumn = null;
        Map<String, Map<String, List<String>>> routingTable = null;
        Map<String, String> timeBoundary = null;
        try {
            table = pinotPrestoConnection.getTable(tableHandle.getTableName());
            timeColumn = pinotPrestoConnection.getPinotTimeColumnForTable(tableHandle.getTableName());
            routingTable = pinotPrestoConnection.getRoutingTable(tableHandle.getTableName());
            timeBoundary = pinotPrestoConnection.getTimeBoundary(tableHandle.getTableName());
            // this can happen if table is removed during a query
            checkState(table != null, "Table %s no longer exists", tableHandle.getTableName());
        }
        catch (Exception e) {
            throw new PrestoException(
                    PINOT_FAILURE_GETTING_TABLE,
                    "Failed to fetch table status for Pinot table: " + tableHandle.getTableName(),
                    e);
        }

        List<ConnectorSplit> splits = new ArrayList<>();
        if (!routingTable.isEmpty()) {
            setSplits(splits, timeColumn, routingTable, timeBoundary, getOfflineTableName(tableHandle.getTableName()), tableHandle.getConstraintSummary());
            setSplits(splits, timeColumn, routingTable, timeBoundary, getRealtimeTableName(tableHandle.getTableName()), tableHandle.getConstraintSummary());
        }

        Collections.shuffle(splits);
        log.debug("PinotSplits is %s", Arrays.toString(splits.toArray()));

        return new FixedSplitSource(splits);
    }

    private String getTimePredicate(String type, String timeColumn, String maxTimeStamp)
    {
        if ("OFFLINE".equalsIgnoreCase(type)) {
            return String.format("%s < %s", timeColumn, maxTimeStamp);
        }
        if ("REALTIME".equalsIgnoreCase(type)) {
            return String.format("%s >= %s", timeColumn, maxTimeStamp);
        }
        return null;
    }

    private void setSplits(List<ConnectorSplit> splits, PinotColumn timeColumn, Map<String, Map<String, List<String>>> routingTable, Map<String, String> timeBoundary, String tableName, TupleDomain<ColumnHandle> constraintSummary)
    {
        String pinotFilter = getPinotPredicate(constraintSummary);
        String timeFilter = "";
        long limit = -1;
        if (timeBoundary.containsKey("timeColumnName") && timeBoundary.containsKey("timeColumnValue")) {
            timeFilter = getTimePredicate(getTableType(tableName), timeBoundary.get("timeColumnName"), timeBoundary.get("timeColumnValue"));
        }
        for (String routingTableName : routingTable.keySet()) {
            if (routingTableName.equalsIgnoreCase(tableName)) {
                Map<String, List<String>> hostToSegmentsMap = routingTable.get(routingTableName);
                for (String host : hostToSegmentsMap.keySet()) {
                    for (String segment : hostToSegmentsMap.get(host)) {
                        splits.add(new PinotSplit(connectorId, routingTableName, host, segment, timeColumn, timeFilter, pinotFilter, limit));
                    }
                }
            }
        }
    }

    /**
     * Get the predicates for Pinot columns in string format, for constructing Pinot queries directly
     * Note that for predicates like UDF (WHERE ROUND(fare) > 10), column comparison (WHERE colA - colB > 10, WHERE col/100 > 5),
     * constraintSummary passed to Pinot will be empty, since those predicates would be in remainingExpression and not passed here.
     *
     * @param constraintSummary TupleDomain representing the allowed ranges for Pinot columns
     * @return Predicate in Pinot Query Language for Pinot columns
     */
    @VisibleForTesting
    String getPinotPredicate(TupleDomain<ColumnHandle> constraintSummary)
    {
        ImmutableList.Builder<String> pinotFilterBuilder = ImmutableList.builder();

        Map<ColumnHandle, Domain> columnHandleDomainMap = constraintSummary.getDomains().get();
        for (ColumnHandle k : columnHandleDomainMap.keySet()) {
            Domain domain = columnHandleDomainMap.get(k);
            String columnPredicate = getColumnPredicate(domain, ((PinotColumnHandle) k).getColumnName());
            if (!columnPredicate.isEmpty()) {
                pinotFilterBuilder.add("(" + columnPredicate + ")");
            }
        }
        return Joiner.on(" AND ").join(pinotFilterBuilder.build());
    }

    /**
     * Get the predicates for a column in string format, for constructing Pinot queries directly
     *
     * @param domain TupleDomain representing the allowed ranges for a column
     * @param columnName Pinot column name
     * @return Predicate in Pinot Query Language for the column. Empty string would be returned if no constraints
     */
    @VisibleForTesting
    String getColumnPredicate(Domain domain, String columnName)
    {
        List<String> discreteConstraintList = new ArrayList<>();
        List<String> singleValueRangeConstraintList = new ArrayList<>();
        List<String> rangeConstraintList = new ArrayList<>();

        return domain.getValues().getValuesProcessor().transform(
                ranges ->
                {
                    for (Range range : ranges.getOrderedRanges()) {
                        if (range.isSingleValue()) {
                            singleValueRangeConstraintList.add(getMarkerValue(range.getLow()));
                        }
                        else {
                            StringBuilder builder = new StringBuilder();
                            ImmutableList.Builder<String> bounds = ImmutableList.builder();
                            // Get low bound
                            String equationMark = (range.getLow().getBound() == Marker.Bound.EXACTLY) ? "= " : " ";
                            if (!range.getLow().isLowerUnbounded()) {
                                bounds.add(getMarkerValue(range.getLow()) + " <" + equationMark + columnName);
                            }
                            // Get high bound
                            equationMark = (range.getHigh().getBound() == Marker.Bound.EXACTLY) ? "= " : " ";
                            if (!range.getHigh().isUpperUnbounded()) {
                                bounds.add(columnName + " <" + equationMark + getMarkerValue(range.getHigh()));
                            }
                            // Use AND to combine bounds within the same range
                            builder.append("(" + Joiner.on(" AND ").join(bounds.build()) + ")");
                            rangeConstraintList.add(builder.toString());
                        }
                    }
                    // Multiple ranges on the same column are OR'ed together.
                    String rangeConstraint = Joiner.on(" OR ").join(rangeConstraintList);
                    String discreteConstraint = getDiscretePredicate(true, columnName, singleValueRangeConstraintList);

                    return Stream.of(rangeConstraint, discreteConstraint)
                            .filter(s -> s != null && !s.isEmpty())
                            .collect(joining(" OR "));
                },
                discreteValues ->
                {
                    /**
                     * For some types like {@link com.facebook.presto.type.ColorType} that are not orderable, discreteValues would appear here.
                     * For most regular types like boolean, char, number, the discrete values would be converted to singleValues in ranges above,
                     * and would not appear here. So far the column types supported by Pinot all fall in that category.
                     */
                    discreteConstraintList.addAll(discreteValues.getValues().stream().map(Object::toString).collect(Collectors.toList()));
                    return getDiscretePredicate(discreteValues.isWhiteList(), columnName, discreteConstraintList);
                },
                allOrNone ->
                {
                    // no-op
                    return "";
                });
    }

    /**
     * Construct the IN predicate for discrete values
     *
     * @param isWhitelist true for IN predicate, false for NOT IN predicate
     * @param columnName name of the column
     * @param discreteConstraintList list of allowed or not allowed values
     * @return Stringified clause with IN or NOT IN
     */
    String getDiscretePredicate(boolean isWhitelist, String columnName, List<String> discreteConstraintList)
    {
        if (discreteConstraintList.size() == 0) {
            return "";
        }
        else {
            return columnName + (isWhitelist ? " " : " NOT ") + "IN (" + Joiner.on(',').join(discreteConstraintList) + ")";
        }
    }

    /**
     * Get the value for the Marker.
     *
     * @param marker marker in the Domain
     * @return Underlying value for the block in the marker. For string, encapsulating quotes will be added.
     */
    String getMarkerValue(Marker marker)
    {
        if (marker.getType() instanceof VarcharType) {
            Block highBlock = marker.getValueBlock().get();
            Slice slice = highBlock.getSlice(0, 0, highBlock.getSliceLength(0));
            return "\"" + slice.toStringUtf8() + "\"";
        }
        else {
            return marker.getValue().toString();
        }
    }

    private String getOfflineTableName(String table)
    {
        return table + "_OFFLINE";
    }

    private String getRealtimeTableName(String table)
    {
        return table + "_REALTIME";
    }

    private String getTableType(String table)
    {
        if (table.endsWith("_REALTIME")) {
            return "REALTIME";
        }

        if (table.endsWith("_OFFLINE")) {
            return "OFFLINE";
        }

        return null;
    }
}
