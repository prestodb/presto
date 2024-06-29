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
package com.facebook.presto.lance;

import com.facebook.presto.lance.metadata.LanceMetadata;
import com.facebook.presto.lance.metadata.LanceTransactionHandle;
import com.facebook.presto.lance.ingestion.LancePageSinkProvider;
import com.facebook.presto.lance.scan.LancePageSourceProvider;
import com.facebook.presto.lance.splits.LanceSplitManager;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdaterProvider;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.ConnectorTypeSerdeProvider;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LanceConnector
        implements Connector
{
    private final LanceMetadata metadata;
    private final LanceSplitManager splitManager;
    private final LancePageSourceProvider pageSourceProvider;
    private final LancePageSinkProvider pageSinkProvider;

    @Inject
    public LanceConnector(
            LanceMetadata metadata,
            LanceSplitManager splitManager,
            LancePageSourceProvider pageSourceProvider,
            LancePageSinkProvider pageSinkProvider)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvide is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return LanceTransactionHandle.INSTANCE;
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
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return Connector.super.getRecordSetProvider();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorIndexProvider getIndexProvider()
    {
        return Connector.super.getIndexProvider();
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return Connector.super.getNodePartitioningProvider();
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider()
    {
        return Connector.super.getConnectorPlanOptimizerProvider();
    }

    @Override
    public ConnectorMetadataUpdaterProvider getConnectorMetadataUpdaterProvider()
    {
        return Connector.super.getConnectorMetadataUpdaterProvider();
    }

    @Override
    public ConnectorTypeSerdeProvider getConnectorTypeSerdeProvider()
    {
        return Connector.super.getConnectorTypeSerdeProvider();
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return Connector.super.getSystemTables();
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return Connector.super.getProcedures();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return Connector.super.getSessionProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return Connector.super.getSchemaProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return Connector.super.getAnalyzeProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return Connector.super.getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return Connector.super.getColumnProperties();
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return Connector.super.getAccessControl();
    }

    @Override
    public ConnectorCommitHandle commit(ConnectorTransactionHandle transactionHandle)
    {
        return Connector.super.commit(transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        Connector.super.rollback(transactionHandle);
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return Connector.super.isSingleStatementWritesOnly();
    }

    @Override
    public void shutdown()
    {
        Connector.super.shutdown();
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return Connector.super.getCapabilities();
    }
}
