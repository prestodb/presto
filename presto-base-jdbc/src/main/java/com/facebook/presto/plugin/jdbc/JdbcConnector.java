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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class JdbcConnector
        implements Connector
{
    private static final Logger log = Logger.get(JdbcConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final JdbcMetadataFactory jdbcMetadataFactory;
    private final JdbcSplitManager jdbcSplitManager;
    private final JdbcRecordSetProvider jdbcRecordSetProvider;
    private final JdbcRecordSinkProvider jdbcRecordSinkProvider;

    private final ConcurrentMap<ConnectorTransactionHandle, JdbcMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public JdbcConnector(
            LifeCycleManager lifeCycleManager,
            JdbcMetadataFactory jdbcMetadataFactory,
            JdbcSplitManager jdbcSplitManager,
            JdbcRecordSetProvider jdbcRecordSetProvider,
            JdbcRecordSinkProvider jdbcRecordSinkProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.jdbcMetadataFactory = requireNonNull(jdbcMetadataFactory, "jdbcMetadataFactory is null");
        this.jdbcSplitManager = requireNonNull(jdbcSplitManager, "jdbcSplitManager is null");
        this.jdbcRecordSetProvider = requireNonNull(jdbcRecordSetProvider, "jdbcRecordSetProvider is null");
        this.jdbcRecordSinkProvider = requireNonNull(jdbcRecordSinkProvider, "jdbcRecordSinkProvider is null");
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return true;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        JdbcTransactionHandle transaction = new JdbcTransactionHandle();
        transactions.put(transaction, jdbcMetadataFactory.create());
        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        JdbcMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        JdbcMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        metadata.rollback();
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return jdbcSplitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return jdbcRecordSetProvider;
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return jdbcRecordSinkProvider;
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
