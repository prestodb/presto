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

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.transaction.TransactionalConnector;
import com.facebook.presto.spi.transaction.TransactionalConnectorMetadata;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.StandardErrorCode.UNSUPPORTED_ISOLATION_LEVEL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LegacyTransactionConnector
        implements TransactionalConnector
{
    private final String connectorId;
    private final Connector connector;
    private final ConcurrentMap<ConnectorTransactionHandle, LegacyTransactionalConnectorMetadata> metadatas = new ConcurrentHashMap<>();

    public LegacyTransactionConnector(String connectorId, Connector connector)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.connector = requireNonNull(connector, "connector is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        if (!connector.getIsolationLevel().meetsRequirementOf(isolationLevel)) {
            throw new PrestoException(UNSUPPORTED_ISOLATION_LEVEL, format("Connector supported isolation level %s does not meet requested isolation level %s", connector.getIsolationLevel(), isolationLevel));
        }
        return LegacyTransactionHandle.create(connectorId);
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new LegacyTransactionHandleResolver(connectorId, connector.getHandleResolver());
    }

    @Override
    public TransactionalConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadatas.computeIfAbsent(transactionHandle, handle -> new LegacyTransactionalConnectorMetadata(connector.getMetadata()));
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return connector.getSplitManager();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return connector.getPageSourceProvider();
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return connector.getRecordSetProvider();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return connector.getPageSinkProvider();
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return connector.getRecordSinkProvider();
    }

    @Override
    public ConnectorIndexResolver getIndexResolver()
    {
        return connector.getIndexResolver();
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
        return connector.getAccessControl();
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        metadatas.remove(transactionHandle);
    }

    @Override
    public void abort(ConnectorTransactionHandle transactionHandle)
    {
        LegacyTransactionalConnectorMetadata metadata = metadatas.remove(transactionHandle);
        if (metadata != null) {
            metadata.tryRollback();
        }
    }

    @Override
    public void shutdown()
    {
        connector.shutdown();
    }
}
