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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.conf.ElasticsearchSessionProperties;
import com.facebook.presto.elasticsearch.io.ElasticsearchPageSinkProvider;
import com.facebook.presto.elasticsearch.io.ElasticsearchPageSourceProvider;
import com.facebook.presto.elasticsearch.model.ElasticsearchTransactionHandle;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ElasticsearchConnector
        implements Connector
{
    private static final Logger LOG = Logger.get(ElasticsearchConnector.class);
    private final ConcurrentMap<ConnectorTransactionHandle, ElasticsearchMetadata> transactions = new ConcurrentHashMap<>();

    private final ElasticsearchConnectorId connectorId;
    private final BaseClient client;
    private final ElasticsearchSplitManager splitManager;
    private final ElasticsearchPageSourceProvider pageSourceProvider;
    private final ElasticsearchPageSinkProvider pageSinkProvider;
    private final ElasticsearchSessionProperties sessionProperties;

    @Inject
    public ElasticsearchConnector(
            ElasticsearchConnectorId connectorId,
            BaseClient client,
            ElasticsearchSplitManager splitManager,
            ElasticsearchPageSourceProvider pageSourceProvider,
            ElasticsearchPageSinkProvider pageSinkProvider,
            ElasticsearchSessionProperties sessionProperties)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.client = requireNonNull(client, "client is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return this.splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return this.pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return this.pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        ConnectorMetadata metadata = transactions.get(transactionHandle);
        checkArgument(metadata != null, "no such transaction: %s", transactionHandle);
        return metadata;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        ConnectorTransactionHandle transaction = new ElasticsearchTransactionHandle();
        transactions.put(transaction, new ElasticsearchMetadata(connectorId, client));
        return transaction;
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        checkArgument(transactions.remove(transactionHandle) != null, "no such transaction: %s", transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        ElasticsearchMetadata metadata = transactions.remove(transactionHandle);
        checkArgument(metadata != null, "no such transaction: %s", transactionHandle);
        metadata.rollback();
    }
}
