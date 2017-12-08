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

import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.transaction.InternalConnector;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;

import static java.util.Objects.requireNonNull;

public class InformationSchemaConnector
        implements InternalConnector
{
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;

    public InformationSchemaConnector(String catalogName, InternalNodeManager nodeManager, Metadata metadata, AccessControl accessControl, TransactionManager transactionManager)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(transactionManager, "transactionManager is null");

        this.metadata = new InformationSchemaMetadata(catalogName);
        this.splitManager = new InformationSchemaSplitManager(nodeManager);
        this.pageSourceProvider = new InformationSchemaPageSourceProvider(metadata, accessControl, transactionManager);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel, boolean readOnly)
    {
        return new InformationSchemaTransactionHandle(transactionId);
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
