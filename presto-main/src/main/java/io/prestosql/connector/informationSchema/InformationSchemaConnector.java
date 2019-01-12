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
package io.prestosql.connector.informationSchema;

import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.transaction.InternalConnector;
import io.prestosql.transaction.TransactionId;

import static java.util.Objects.requireNonNull;

public class InformationSchemaConnector
        implements InternalConnector
{
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;

    public InformationSchemaConnector(String catalogName, InternalNodeManager nodeManager, Metadata metadata, AccessControl accessControl)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(metadata, "metadata is null");

        this.metadata = new InformationSchemaMetadata(catalogName, metadata);
        this.splitManager = new InformationSchemaSplitManager(nodeManager);
        this.pageSourceProvider = new InformationSchemaPageSourceProvider(metadata, accessControl);
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
