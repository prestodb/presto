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
package io.prestosql.plugin.atop;

import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.ValueSet;

import javax.inject.Inject;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Objects.requireNonNull;

public class AtopSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final ZoneId timeZone;
    private final int maxHistoryDays;

    @Inject
    public AtopSplitManager(NodeManager nodeManager, AtopConnectorConfig config)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(config, "config is null");
        timeZone = config.getTimeZoneId();
        maxHistoryDays = config.getMaxHistoryDays();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        AtopTableLayoutHandle handle = (AtopTableLayoutHandle) layoutHandle;

        AtopTableHandle table = handle.getTableHandle();

        List<ConnectorSplit> splits = new ArrayList<>();
        ZonedDateTime end = ZonedDateTime.now(timeZone);
        for (Node node : nodeManager.getWorkerNodes()) {
            ZonedDateTime start = end.minusDays(maxHistoryDays - 1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            while (start.isBefore(end)) {
                ZonedDateTime splitEnd = start.withHour(23).withMinute(59).withSecond(59).withNano(0);
                Domain splitDomain = Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP_WITH_TIME_ZONE, 1000 * start.toEpochSecond(), true, 1000 * splitEnd.toEpochSecond(), true)), false);
                if (handle.getStartTimeConstraint().overlaps(splitDomain) && handle.getEndTimeConstraint().overlaps(splitDomain)) {
                    splits.add(new AtopSplit(table.getTable(), node.getHostAndPort(), start.toEpochSecond(), start.getZone()));
                }
                start = start.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            }
        }

        return new FixedSplitSource(splits);
    }
}
