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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.TransactionalConnectorPageSourceProvider;
import com.facebook.presto.spi.TransactionalConnectorSplitManager;
import com.facebook.presto.spi.transaction.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.transaction.TransactionalConnectorMetadata;
import com.facebook.presto.transaction.InternalTransactionalConnector;
import com.facebook.presto.transaction.TransactionId;

import static java.util.Objects.requireNonNull;

public class InformationSchemaConnector
        implements InternalTransactionalConnector
{
    private final String connectorId;
    private final ConnectorHandleResolver handleResolver;
    private final TransactionalConnectorMetadata metadata;
    private final TransactionalConnectorSplitManager splitManager;
    private final TransactionalConnectorPageSourceProvider pageSourceProvider;

    public InformationSchemaConnector(String connectorId, String catalogName, NodeManager nodeManager, Metadata metadata)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(metadata, "metadata is null");

        this.connectorId = connectorId;
        this.handleResolver = new InformationSchemaHandleResolver(connectorId);
        this.metadata = new InformationSchemaMetadata(connectorId, catalogName);
        this.splitManager = new InformationSchemaSplitManager(nodeManager);
        this.pageSourceProvider = new InformationSchemaPageSourceProvider(metadata);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel, boolean readOnly)
    {
        return new InformationSchemaTransactionHandle(connectorId, transactionId);
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
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
    public TransactionalConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }
}
