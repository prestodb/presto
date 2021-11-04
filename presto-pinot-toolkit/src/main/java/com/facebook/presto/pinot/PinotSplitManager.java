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

import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.pinot.query.PinotQueryGenerator.GeneratedPinotQuery;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.PinotSplit.createBrokerSplit;
import static com.facebook.presto.pinot.PinotSplit.createSegmentSplit;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.TABLE_NAME_SUFFIX_TEMPLATE;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.TIME_BOUNDARY_FILTER_TEMPLATE;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class PinotSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final PinotConnection pinotPrestoConnection;

    @Inject
    public PinotSplitManager(ConnectorId connectorId, PinotConnection pinotPrestoConnection)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotPrestoConnection = requireNonNull(pinotPrestoConnection, "pinotPrestoConnection is null");
    }

    protected ConnectorSplitSource generateSplitForBrokerBasedScan(PinotQueryGenerator.GeneratedPinotQuery brokerPinotQuery,
            List<PinotColumnHandle> expectedColumnHandles)
    {
        return new FixedSplitSource(singletonList(createBrokerSplit(connectorId, expectedColumnHandles, brokerPinotQuery)));
    }

    protected ConnectorSplitSource generateSplitsForSegmentBasedScan(
            PinotTableLayoutHandle pinotLayoutHandle,
            ConnectorSession session,
            List<PinotColumnHandle> expectedColumnHandles)
    {
        PinotTableHandle tableHandle = pinotLayoutHandle.getTable();
        String tableName = tableHandle.getTableName();
        Map<String, Map<String, List<String>>> routingTable;

        routingTable = pinotPrestoConnection.getRoutingTable(tableName);

        List<ConnectorSplit> splits = new ArrayList<>();
        if (!routingTable.isEmpty()) {
            GeneratedPinotQuery segmentPql = tableHandle.getPinotQuery().orElseThrow(() -> new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Expected to find realtime and offline pql in " + tableHandle));
            PinotClusterInfoFetcher.TimeBoundary timeBoundary = pinotPrestoConnection.getTimeBoundary(tableName);
            String realtime = getSegmentPql(segmentPql, "_REALTIME", timeBoundary.getOnlineTimePredicate());
            String offline = getSegmentPql(segmentPql, "_OFFLINE", timeBoundary.getOfflineTimePredicate());
            generateSegmentSplits(splits, expectedColumnHandles, routingTable, tableName, "_REALTIME", session, realtime);
            generateSegmentSplits(splits, expectedColumnHandles, routingTable, tableName, "_OFFLINE", session, offline);
        }

        Collections.shuffle(splits);
        return new FixedSplitSource(splits);
    }

    private String getSegmentPql(GeneratedPinotQuery basePql, String suffix, Optional<String> timePredicate)
    {
        String pql = basePql.getQuery().replace(TABLE_NAME_SUFFIX_TEMPLATE, suffix);
        if (timePredicate.isPresent()) {
            String tp = timePredicate.get();
            pql = pql.replace(TIME_BOUNDARY_FILTER_TEMPLATE, basePql.isHaveFilter() ? " AND " + tp : " WHERE " + tp);
        }
        else {
            pql = pql.replace(TIME_BOUNDARY_FILTER_TEMPLATE, "");
        }
        return pql;
    }

    protected void generateSegmentSplits(
            List<ConnectorSplit> splits,
            List<PinotColumnHandle> expectedColumnHandles,
            Map<String, Map<String, List<String>>> routingTable,
            String tableName,
            String tableNameSuffix,
            ConnectorSession session,
            String pql)
    {
        final String finalTableName = tableName + tableNameSuffix;
        int segmentsPerSplitConfigured = PinotSessionProperties.getNumSegmentsPerSplit(session);
        for (String routingTableName : routingTable.keySet()) {
            if (!routingTableName.equalsIgnoreCase(finalTableName)) {
                continue;
            }

            Map<String, List<String>> hostToSegmentsMap = routingTable.get(routingTableName);
            hostToSegmentsMap.forEach((host, segments) -> {
                int numSegmentsInThisSplit = Math.min(segments.size(), segmentsPerSplitConfigured);
                // segments is already shuffled
                Iterables.partition(segments, numSegmentsInThisSplit).forEach(
                        segmentsForThisSplit -> splits.add(
                                createSegmentSplit(connectorId, pql, expectedColumnHandles, segmentsForThisSplit, host, getGrpcPort(host))));
            });
        }
    }

    private int getGrpcPort(String host)
    {
        return pinotPrestoConnection.getGrpcPort(host);
    }

    public static class QueryNotAdequatelyPushedDownException
            extends PinotException
    {
        private final String connectorId;
        private final ConnectorTableHandle connectorTableHandle;

        public QueryNotAdequatelyPushedDownException(
                PinotErrorCode errorCode,
                ConnectorTableHandle connectorTableHandle,
                String connectorId)
        {
            super(requireNonNull(errorCode, "error code is null"), Optional.empty(), "Query uses unsupported expressions that cannot be pushed into Pinot.");
            this.connectorId = requireNonNull(connectorId, "connector id is null");
            this.connectorTableHandle = requireNonNull(connectorTableHandle, "connector table handle is null");
        }

        @Override
        public String getMessage()
        {
            return super.getMessage() + String.format(" table: %s:%s", connectorId, connectorTableHandle);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        PinotTableLayoutHandle pinotLayoutHandle = (PinotTableLayoutHandle) layout;
        PinotTableHandle pinotTableHandle = pinotLayoutHandle.getTable();
        Supplier<PrestoException> errorSupplier = () -> new QueryNotAdequatelyPushedDownException(PinotErrorCode.PINOT_PUSH_DOWN_QUERY_NOT_PRESENT, pinotTableHandle, connectorId);
        if (!pinotTableHandle.getIsQueryShort().orElseThrow(errorSupplier)) {
            if (PinotSessionProperties.isForbidSegmentQueries(session)) {
                throw errorSupplier.get();
            }
            return generateSplitsForSegmentBasedScan(pinotLayoutHandle, session, pinotTableHandle.getExpectedColumnHandles().orElseThrow(errorSupplier));
        }
        else {
            return generateSplitForBrokerBasedScan(pinotTableHandle.getPinotQuery().orElseThrow(errorSupplier), pinotTableHandle.getExpectedColumnHandles().orElseThrow(errorSupplier));
        }
    }
}
