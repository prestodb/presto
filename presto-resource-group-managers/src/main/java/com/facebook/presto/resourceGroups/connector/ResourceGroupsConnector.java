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
package com.facebook.presto.resourceGroups.connector;

import com.facebook.presto.resourceGroups.connector.ResourceGroupsHandleResolver.ResourceGroupsTransactionHandle;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ResourceGroupsConnector
        implements Connector
{
    private final Set<SystemTable> systemTables;
    private final Set<Procedure> procedures;
    private final ResourceGroupsSplitManager splitManager;
    private final ResourceGroupsMetadata metadata;
    private final ResourceGroupsRecordSetProvider recordSetProvider;

    @Inject
    public ResourceGroupsConnector(
            ResourceGroupsMetadata metadata,
            ResourceGroupsSplitManager splitManager,
            ResourceGroupsRecordSetProvider recordSetProvider,
            Set<SystemTable> systemTables,
            Set<Procedure> procedures)
    {
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
        this.procedures = requireNonNull(procedures, "procedures is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.metadata = metadata;
        this.recordSetProvider = recordSetProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return ResourceGroupsTransactionHandle.INSTANCE;
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
        //return new ConnectorSplitManager() {};
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }
}
