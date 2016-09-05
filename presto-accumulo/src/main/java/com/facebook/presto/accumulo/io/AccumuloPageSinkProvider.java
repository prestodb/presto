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

import com.facebook.presto.accumulo.AccumuloClient;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.model.AccumuloTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.accumulo.core.client.Connector;

import javax.inject.Inject;

import static com.facebook.presto.accumulo.Types.checkType;
import static java.util.Objects.requireNonNull;

/**
 * Page sink provider for Accumulo connector. Creates {@link AccumuloPageSink} objects for output tables (CTAS) and inserts.
 *
 * @see AccumuloPageSink
 */
public class AccumuloPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final AccumuloClient client;
    private final AccumuloConfig config;
    private final Connector connector;

    @Inject
    public AccumuloPageSinkProvider(
            Connector connector,
            AccumuloConfig config,
            AccumuloClient client)
    {
        this.client = requireNonNull(client, "client is null");
        this.config = requireNonNull(config, "config is null");
        this.connector = requireNonNull(connector, "connector is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        AccumuloTableHandle tableHandle = checkType(outputTableHandle, AccumuloTableHandle.class, "tableHandle");
        return new AccumuloPageSink(connector, config, client.getTable(tableHandle.toSchemaTableName()));
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return createPageSink(transactionHandle, session, checkType(insertTableHandle, ConnectorOutputTableHandle.class, "tHandle"));
    }
}
