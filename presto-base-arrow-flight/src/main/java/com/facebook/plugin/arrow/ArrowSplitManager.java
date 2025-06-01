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
package com.facebook.plugin.arrow;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.arrow.flight.FlightInfo;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ArrowSplitManager
        implements ConnectorSplitManager
{
    private final BaseArrowFlightClientHandler clientHandler;

    @Inject
    public ArrowSplitManager(BaseArrowFlightClientHandler clientHandler)
    {
        this.clientHandler = requireNonNull(clientHandler, "clientHandler is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
    {
        ArrowTableLayoutHandle tableLayoutHandle = (ArrowTableLayoutHandle) layout;
        ArrowTableHandle tableHandle = tableLayoutHandle.getTable();
        FlightInfo flightInfo = clientHandler.getFlightInfoForTableScan(session, tableLayoutHandle);
        List<ArrowSplit> splits = flightInfo.getEndpoints()
                .stream()
                .map(info -> new ArrowSplit(
                        tableHandle.getSchema(),
                        tableHandle.getTable(),
                        info.serialize().array()))
                .collect(toImmutableList());
        return new FixedSplitSource(splits);
    }
}
