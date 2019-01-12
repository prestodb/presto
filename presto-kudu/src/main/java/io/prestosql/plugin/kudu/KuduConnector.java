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
package io.prestosql.plugin.kudu;

import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.prestosql.plugin.kudu.properties.KuduTableProperties;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class KuduConnector
        implements Connector
{
    private static final Logger log = Logger.get(KuduConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final KuduMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorRecordSetProvider recordSetProvider;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final KuduTableProperties tableProperties;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final Set<Procedure> procedures;

    @Inject
    public KuduConnector(
            LifeCycleManager lifeCycleManager,
            KuduMetadata metadata,
            ConnectorSplitManager splitManager,
            ConnectorRecordSetProvider recordSetProvider,
            KuduTableProperties tableProperties,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            Set<Procedure> procedures)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
            boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return KuduTransactionHandle.INSTANCE;
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
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return tableProperties.getColumnProperties();
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
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
