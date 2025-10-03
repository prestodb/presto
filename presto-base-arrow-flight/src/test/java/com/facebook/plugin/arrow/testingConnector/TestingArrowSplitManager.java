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
package com.facebook.plugin.arrow.testingConnector;

import com.facebook.plugin.arrow.ArrowSplit;
import com.facebook.plugin.arrow.ArrowSplitManager;
import com.facebook.plugin.arrow.ArrowTableHandle;
import com.facebook.plugin.arrow.ArrowTableLayoutHandle;
import com.facebook.plugin.arrow.BaseArrowFlightClientHandler;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import jakarta.inject.Inject;
import org.apache.arrow.flight.FlightInfo;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestingArrowSplitManager
        extends ArrowSplitManager
{
    @Inject
    public TestingArrowSplitManager(BaseArrowFlightClientHandler clientHandler)
    {
        super(clientHandler);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
    {
        ArrowTableLayoutHandle tableLayoutHandle = (ArrowTableLayoutHandle) layout;
        ArrowTableHandle tableHandle = tableLayoutHandle.getTable();

        if (!(tableHandle instanceof QueryArrowTableHandle)) {
            return super.getSplits(transactionHandle, session, layout, splitSchedulingContext);
        }

        QueryArrowTableHandle queryArrowTableHandle = (QueryArrowTableHandle) tableHandle;
        FlightInfo flightInfo = getClientHandler().getFlightInfoForTableScan(session, tableLayoutHandle);
        List<ArrowSplit> splits = flightInfo.getEndpoints()
                .stream()
                .map(info -> new ArrowSplit(
                        queryArrowTableHandle.getSchema(),
                        queryArrowTableHandle.getTable(),
                        info.serialize().array(),
                        Optional.of(queryArrowTableHandle.getColumns())))
                .collect(toImmutableList());
        return new FixedSplitSource(splits);
    }
}
