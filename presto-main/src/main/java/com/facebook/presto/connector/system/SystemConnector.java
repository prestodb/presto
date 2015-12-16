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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TransactionalConnectorRecordSetProvider;
import com.facebook.presto.spi.TransactionalConnectorSplitManager;
import com.facebook.presto.spi.transaction.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.transaction.TransactionalConnectorMetadata;
import com.facebook.presto.transaction.InternalTransactionalConnector;
import com.facebook.presto.transaction.TransactionId;

import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class SystemConnector
        implements InternalTransactionalConnector
{
    private final String connectorId;
    private final SystemHandleResolver handleResolver;
    private final TransactionalConnectorMetadata metadata;
    private final TransactionalConnectorSplitManager splitManager;
    private final TransactionalConnectorRecordSetProvider recordSetProvider;
    private final Function<TransactionId, ConnectorTransactionHandle> transactionHandleFunction;

    public SystemConnector(
            String connectorId,
            NodeManager nodeManager,
            Set<SystemTable> tables,
            Function<TransactionId, ConnectorTransactionHandle> transactionHandleFunction)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(tables, "tables is null");
        requireNonNull(transactionHandleFunction, "transactionHandleFunction is null");

        this.connectorId = connectorId;
        this.handleResolver = new SystemHandleResolver(connectorId);
        this.metadata = new SystemTablesMetadata(connectorId, tables);
        this.splitManager = new SystemSplitManager(nodeManager, tables);
        this.recordSetProvider = new SystemRecordSetProvider(tables);
        this.transactionHandleFunction = transactionHandleFunction;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel, boolean readOnly)
    {
        return new SystemTransactionHandle(connectorId, transactionHandleFunction.apply(transactionId));
    }

    @Override
    public TransactionalConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public TransactionalConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
    }

    @Override
    public TransactionalConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }
}
