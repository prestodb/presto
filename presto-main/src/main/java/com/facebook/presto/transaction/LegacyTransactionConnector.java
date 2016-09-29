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
package com.facebook.presto.transaction;

import com.facebook.presto.security.LegacyConnectorAccessControl;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class LegacyTransactionConnector
        implements Connector
{
    private final com.facebook.presto.spi.Connector connector;
    private final ConcurrentMap<ConnectorTransactionHandle, LegacyConnectorMetadata> metadatas = new ConcurrentHashMap<>();

    public LegacyTransactionConnector(com.facebook.presto.spi.Connector connector)
    {
        this.connector = requireNonNull(connector, "connector is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(connector.getIsolationLevel(), isolationLevel);
        return LegacyTransactionHandle.create();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadatas.computeIfAbsent(transactionHandle, handle -> new LegacyConnectorMetadata(connector.getMetadata(), Optional.ofNullable(connector.getIndexResolver())));
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new LegacyConnectorSplitManager(connector.getSplitManager());
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return new LegacyConnectorPageSourceProvider(connector.getPageSourceProvider());
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return new LegacyConnectorRecordSetProvider(connector.getRecordSetProvider());
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return new LegacyConnectorPageSinkProvider(connector.getPageSinkProvider());
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return new LegacyConnectorRecordSinkProvider(connector.getRecordSinkProvider());
    }

    @Override
    public ConnectorIndexProvider getIndexProvider()
    {
        ConnectorIndexResolver indexResolver = connector.getIndexResolver();
        if (indexResolver == null) {
            throw new UnsupportedOperationException();
        }
        return new LegacyConnectorIndexProvider(indexResolver);
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return connector.getSystemTables();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return connector.getSessionProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return connector.getTableProperties();
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return new LegacyConnectorAccessControl(connector.getAccessControl());
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        metadatas.remove(transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        LegacyConnectorMetadata metadata = metadatas.remove(transactionHandle);
        if (metadata != null) {
            metadata.tryRollback();
        }
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return true;
    }

    @Override
    public void shutdown()
    {
        connector.shutdown();
    }
}
