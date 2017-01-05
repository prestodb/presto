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
package com.facebook.presto.connector.system;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.transaction.InternalConnector;
import com.facebook.presto.transaction.TransactionId;

import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class SystemConnector
        implements InternalConnector
{
    private final ConnectorId connectorId;
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final Function<TransactionId, ConnectorTransactionHandle> transactionHandleFunction;

    public SystemConnector(
            ConnectorId connectorId,
            InternalNodeManager nodeManager,
            Set<SystemTable> tables,
            Function<TransactionId, ConnectorTransactionHandle> transactionHandleFunction)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(tables, "tables is null");
        requireNonNull(transactionHandleFunction, "transactionHandleFunction is null");

        this.connectorId = connectorId;
        this.metadata = new SystemTablesMetadata(connectorId, tables);
        this.splitManager = new SystemSplitManager(nodeManager, tables);
        this.pageSourceProvider = new SystemPageSourceProvider(tables);
        this.transactionHandleFunction = transactionHandleFunction;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel, boolean readOnly)
    {
        return new SystemTransactionHandle(connectorId, transactionId, transactionHandleFunction);
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
}
