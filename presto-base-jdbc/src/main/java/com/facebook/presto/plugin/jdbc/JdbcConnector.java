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

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.jdbc.optimization.JdbcPlanOptimizerProvider;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.immutableEnumSet;
import static java.util.Objects.requireNonNull;

public class JdbcConnector
        implements Connector
{
    private static final Logger log = Logger.get(JdbcConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final JdbcMetadataFactory jdbcMetadataFactory;
    private final JdbcSplitManager jdbcSplitManager;
    private final JdbcRecordSetProvider jdbcRecordSetProvider;
    private final JdbcPageSinkProvider jdbcPageSinkProvider;
    private final Optional<ConnectorAccessControl> accessControl;
    private final Set<Procedure> procedures;

    private final ConcurrentMap<ConnectorTransactionHandle, JdbcMetadata> transactions = new ConcurrentHashMap<>();
    private final FunctionMetadataManager functionManager;
    private final StandardFunctionResolution functionResolution;
    private final RowExpressionService rowExpressionService;
    private final JdbcClient jdbcClient;

    @Inject
    public JdbcConnector(
            LifeCycleManager lifeCycleManager,
            JdbcMetadataFactory jdbcMetadataFactory,
            JdbcSplitManager jdbcSplitManager,
            JdbcRecordSetProvider jdbcRecordSetProvider,
            JdbcPageSinkProvider jdbcPageSinkProvider,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures,
            FunctionMetadataManager functionManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JdbcClient jdbcClient)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.jdbcMetadataFactory = requireNonNull(jdbcMetadataFactory, "jdbcMetadataFactory is null");
        this.jdbcSplitManager = requireNonNull(jdbcSplitManager, "jdbcSplitManager is null");
        this.jdbcRecordSetProvider = requireNonNull(jdbcRecordSetProvider, "jdbcRecordSetProvider is null");
        this.jdbcPageSinkProvider = requireNonNull(jdbcPageSinkProvider, "jdbcPageSinkProvider is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider()
    {
        return new JdbcPlanOptimizerProvider(
                jdbcClient,
                functionManager,
                functionResolution,
                rowExpressionService.getDeterminismEvaluator(),
                rowExpressionService.getExpressionOptimizer());
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
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return jdbcPageSinkProvider;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl.orElseThrow(UnsupportedOperationException::new);
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
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

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(NOT_NULL_COLUMN_CONSTRAINT);
    }
}
