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
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.pinot.common.config.TableNameBuilder;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_FAILURE_GETTING_TABLE;
import static com.facebook.presto.pinot.PinotQueryBuilder.getColumnPredicate;
import static com.facebook.presto.pinot.PinotQueryBuilder.getTimePredicate;
import static com.facebook.presto.pinot.PinotUtils.TIME_COLUMN_NAME;
import static com.facebook.presto.pinot.PinotUtils.TIME_COLUMN_VALUE;
import static com.facebook.presto.pinot.PinotUtils.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PinotSplitManager
        implements ConnectorSplitManager
{
    private final PinotConnection pinotPrestoConnection;

    @Inject
    public PinotSplitManager(PinotConnection pinotPrestoConnection)
    {
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
        PinotTable table;
        PinotColumn timeColumn;
        Map<String, Map<String, List<String>>> routingTable;
        Map<String, String> timeBoundary;
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
            setSplits(splits, timeColumn, routingTable, timeBoundary, TableNameBuilder.OFFLINE.tableNameWithType(tableHandle.getTableName()), tableHandle.getConstraintSummary());
            setSplits(splits, timeColumn, routingTable, timeBoundary, TableNameBuilder.REALTIME.tableNameWithType(tableHandle.getTableName()), tableHandle.getConstraintSummary());
        }

        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
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
    static String getPinotPredicate(TupleDomain<ColumnHandle> constraintSummary)
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

    private void setSplits(List<ConnectorSplit> splits, PinotColumn timeColumn, Map<String, Map<String, List<String>>> routingTable, Map<String, String> timeBoundary, String tableName, TupleDomain<ColumnHandle> constraintSummary)
    {
        String pinotFilter = getPinotPredicate(constraintSummary);
        String timeFilter = "";
        long limit = -1;
        if (timeBoundary.containsKey(TIME_COLUMN_NAME) && timeBoundary.containsKey(TIME_COLUMN_VALUE)) {
            timeFilter = getTimePredicate(TableNameBuilder.getTableTypeFromTableName(tableName).toString(), timeBoundary.get(TIME_COLUMN_NAME), timeBoundary.get(TIME_COLUMN_VALUE));
        }
        for (String routingTableName : routingTable.keySet()) {
            if (routingTableName.equalsIgnoreCase(tableName)) {
                Map<String, List<String>> hostToSegmentsMap = routingTable.get(routingTableName);
                for (String host : hostToSegmentsMap.keySet()) {
                    for (String segment : hostToSegmentsMap.get(host)) {
                        splits.add(new PinotSplit(routingTableName, host, segment, timeColumn, timeFilter, pinotFilter, limit));
                    }
                }
            }
        }
    }
}
