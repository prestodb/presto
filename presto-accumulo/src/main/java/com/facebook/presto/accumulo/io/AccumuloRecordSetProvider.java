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
package com.facebook.presto.accumulo.io;

import com.facebook.presto.accumulo.AccumuloConnectorId;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.AccumuloSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.client.Connector;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.accumulo.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of a ConnectorRecordSetProvider for Accumulo. Generates {@link AccumuloRecordSet} objects for a provided split.
 *
 * @see AccumuloRecordSet
 * @see AccumuloRecordCursor
 */
public class AccumuloRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final Connector connector;
    private final String connectorId;
    private final String username;

    @Inject
    public AccumuloRecordSetProvider(
            Connector connector,
            AccumuloConnectorId connectorId,
            AccumuloConfig config)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.username = requireNonNull(config, "config is null").getUsername();
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        // Convert split
        AccumuloSplit accSplit = checkType(split, AccumuloSplit.class, "split");
        checkArgument(accSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        // Convert all columns handles
        ImmutableList.Builder<AccumuloColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add(checkType(handle, AccumuloColumnHandle.class, "handle"));
        }

        // Return new record set
        return new AccumuloRecordSet(connector, session, accSplit, username, handles.build());
    }
}
