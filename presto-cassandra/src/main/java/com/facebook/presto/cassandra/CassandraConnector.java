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
package com.facebook.presto.cassandra;

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import jakarta.inject.Inject;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.connector.EmptyConnectorCommitHandle.INSTANCE;
import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CassandraConnector
        implements Connector
{
    private static final Logger log = Logger.get(CassandraConnector.class);

    private final CassandraConnectorId connectorId;
    private final LifeCycleManager lifeCycleManager;
    private final CassandraPartitionManager partitionManager;
    private final CassandraClientConfig config;
    private final CassandraSession cassandraSession;
    private final CassandraSplitManager splitManager;
    private final ConnectorRecordSetProvider recordSetProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;
    private final ConcurrentMap<ConnectorTransactionHandle, CassandraMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public CassandraConnector(
            CassandraConnectorId connectorId,
            LifeCycleManager lifeCycleManager,
            CassandraSplitManager splitManager,
            CassandraRecordSetProvider recordSetProvider,
            CassandraPageSinkProvider pageSinkProvider,
            CassandraSessionProperties sessionProperties,
            CassandraSession cassandraSession,
            CassandraPartitionManager partitionManager,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec,
            CassandraClientConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties.getSessionProperties(), "sessionProperties is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
        this.config = requireNonNull(config, "config is null");
        this.extraColumnMetadataCodec = requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        CassandraTransactionHandle transaction = new CassandraTransactionHandle();
        transactions.put(transaction,
                new CassandraMetadata(connectorId, cassandraSession, partitionManager, extraColumnMetadataCodec, config));
        return transaction;
    }

    @Override
    public ConnectorCommitHandle commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
        return INSTANCE;
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        CassandraMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        metadata.rollback();
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return true;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        CassandraMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
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
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
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
