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
package com.facebook.presto.dispatcher;

import com.facebook.presto.Session;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tracing.NoopTracerProvider;
import com.facebook.presto.tracing.QueryStateTracingListener;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.StatementUtils.isTransactionControlStatement;
import static java.util.Objects.requireNonNull;

public class LocalDispatchQueryFactory
        implements DispatchQueryFactory
{
    private final QueryManager queryManager;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;

    private final ClusterSizeMonitor clusterSizeMonitor;

    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;
    private final ListeningExecutorService executor;

    private final QueryPrerequisitesManager queryPrerequisitesManager;

    @Inject
    public LocalDispatchQueryFactory(
            QueryManager queryManager,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata,
            QueryMonitor queryMonitor,
            LocationFactory locationFactory,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories,
            ClusterSizeMonitor clusterSizeMonitor,
            DispatchExecutor dispatchExecutor,
            QueryPrerequisitesManager queryPrerequisitesManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.executionFactories = requireNonNull(executionFactories, "executionFactories is null");

        this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");

        this.executor = requireNonNull(dispatchExecutor, "executorService is null").getExecutor();
        this.queryPrerequisitesManager = requireNonNull(queryPrerequisitesManager, "queryPrerequisitesManager is null");
    }

    @Override
    public DispatchQuery createDispatchQuery(
            Session session,
            String query,
            PreparedQuery preparedQuery,
            String slug,
            int retryCount,
            ResourceGroupId resourceGroup,
            Optional<QueryType> queryType,
            WarningCollector warningCollector,
            Consumer<DispatchQuery> queryQueuer)
    {
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                query,
                session,
                locationFactory.createQueryLocation(session.getQueryId()),
                resourceGroup,
                queryType,
                isTransactionControlStatement(preparedQuery.getStatement()),
                transactionManager,
                accessControl,
                executor,
                metadata,
                warningCollector);

        stateMachine.addStateChangeListener(new QueryStateTracingListener(stateMachine.getSession().getTracer().orElse(NoopTracerProvider.NOOP_TRACER)));
        queryMonitor.queryCreatedEvent(stateMachine.getBasicQueryInfo(Optional.empty()));

        ListenableFuture<QueryExecution> queryExecutionFuture = executor.submit(() -> {
            QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(preparedQuery.getStatement().getClass());
            if (queryExecutionFactory == null) {
                throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type: " + preparedQuery.getStatement().getClass().getSimpleName());
            }

            return queryExecutionFactory.createQueryExecution(preparedQuery, stateMachine, slug, retryCount, warningCollector, queryType);
        });

        return new LocalDispatchQuery(
                stateMachine,
                queryMonitor,
                queryExecutionFuture,
                clusterSizeMonitor,
                executor,
                queryQueuer,
                queryManager::createQuery,
                retryCount > 0,
                queryPrerequisitesManager);
    }
}
