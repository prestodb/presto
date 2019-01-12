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
package io.prestosql.plugin.accumulo.io;

import io.prestosql.plugin.accumulo.AccumuloClient;
import io.prestosql.plugin.accumulo.conf.AccumuloConfig;
import io.prestosql.plugin.accumulo.model.AccumuloTableHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import org.apache.accumulo.core.client.Connector;

import javax.inject.Inject;

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
    private final Connector connector;
    private final String username;

    @Inject
    public AccumuloPageSinkProvider(
            Connector connector,
            AccumuloConfig config,
            AccumuloClient client)
    {
        this.client = requireNonNull(client, "client is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.username = requireNonNull(config, "config is null").getUsername();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        AccumuloTableHandle tableHandle = (AccumuloTableHandle) outputTableHandle;
        return new AccumuloPageSink(connector, client.getTable(tableHandle.toSchemaTableName()), username);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return createPageSink(transactionHandle, session, (ConnectorOutputTableHandle) insertTableHandle);
    }
}
