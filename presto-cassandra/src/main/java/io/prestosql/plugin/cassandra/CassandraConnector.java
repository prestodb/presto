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
package io.prestosql.plugin.cassandra;

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class CassandraConnector
        implements Connector
{
    private static final Logger log = Logger.get(CassandraConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final CassandraMetadata metadata;
    private final CassandraSplitManager splitManager;
    private final ConnectorRecordSetProvider recordSetProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public CassandraConnector(
            LifeCycleManager lifeCycleManager,
            CassandraMetadata metadata,
            CassandraSplitManager splitManager,
            CassandraRecordSetProvider recordSetProvider,
            CassandraPageSinkProvider pageSinkProvider,
            CassandraSessionProperties sessionProperties)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties.getSessionProperties(), "sessionProperties is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        return CassandraTransactionHandle.INSTANCE;
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return true;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
