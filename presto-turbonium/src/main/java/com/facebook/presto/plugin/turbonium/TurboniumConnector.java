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

package com.facebook.presto.plugin.turbonium;

import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TurboniumConnector
        implements Connector
{
    private final TurboniumMetadata metadata;
    private final TurboniumSplitManager splitManager;
    private final TurboniumPageSourceProvider pageSourceProvider;
    private final TurboniumPageSinkProvider pageSinkProvider;
    private final TurboniumNodePartitioningProvider partitioningProvider;
    private final Set<SystemTable> systemTables;
    private final Set<Procedure> procedures;

    @Inject
    public TurboniumConnector(
            TurboniumMetadata metadata,
            TurboniumSplitManager splitManager,
            TurboniumPageSourceProvider pageSourceProvider,
            TurboniumPageSinkProvider pageSinkProvider,
            TurboniumNodePartitioningProvider partitioningProvider,
            Set<SystemTable> systemTables,
            Set<Procedure> procedures)
    {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.pageSinkProvider = pageSinkProvider;
        this.partitioningProvider = partitioningProvider;
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
        this.procedures = requireNonNull(procedures);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return TurboniumTransactionHandle.INSTANCE;
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
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return partitioningProvider;
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }
}
