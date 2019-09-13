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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.pinot.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PinotPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final String connectorId;
    private final PinotConfig pinotConfig;
    private final PinotScatterGatherQueryClient pinotQueryClient;

    @Inject
    public PinotPageSourceProvider(PinotConnectorId connectorId, PinotConfig pinotConfig, PinotScatterGatherQueryClient pinotQueryClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
        this.pinotQueryClient = requireNonNull(pinotQueryClient, "pinotQueryClient is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        requireNonNull(split, "partitionChunk is null");
        PinotSplit pinotSplit = checkType(split, PinotSplit.class, "split");
        checkArgument(pinotSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        List<PinotColumnHandle> handles = new ArrayList<>();
        if (columns.isEmpty()) {
            // For COUNT(*) and COUNT(1), no columns are passed down to Pinot
            // Since this is the only known type of queries for this scenario, we just select time column from Pinot to facilitate the COUNT
            handles.add(new PinotColumnHandle(this.connectorId, pinotSplit.getTimeColumn().getName(), pinotSplit.getTimeColumn().getType(), 0));
        }
        else {
            for (ColumnHandle handle : columns) {
                handles.add(checkType(handle, PinotColumnHandle.class, "handle"));
            }
        }
        return new PinotPageSource(this.pinotConfig, this.pinotQueryClient, pinotSplit, handles);
    }
}
