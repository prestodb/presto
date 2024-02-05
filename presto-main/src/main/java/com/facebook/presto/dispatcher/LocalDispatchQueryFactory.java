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
import com.facebook.presto.common.analyzer.PreparedQuery;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.ExecutionFactoriesManager;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.AnalyzerProvider;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.tracing.NoopTracerProvider;
import com.facebook.presto.tracing.QueryStateTracingListener;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.inject.Inject;

import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * The local dispatch query factory is responsible for creating a query in {@link QueryManager} that will begin to execute the query
 */
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

    private final ExecutionFactoriesManager executionFactoriesManager;
    private final ListeningExecutorService executor;

    private final QueryPrerequisitesManager queryPrerequisitesManager;

    /**
     * Instantiates a new Local dispatch query factory.
     *
     * @param queryManager the query manager
     * @param transactionManager the transaction manager
     * @param accessControl the access control
     * @param metadata the metadata
     * @param queryMonitor the query monitor
     * @param locationFactory the location factory
     * @param executionFactoriesManager the execution factories manager
     * @param clusterSizeMonitor the cluster size monitor
     * @param dispatchExecutor the dispatch executor
     * @param queryPrerequisitesManager the query prerequisites manager
     */
    @Inject
    public LocalDispatchQueryFactory(
            QueryManager queryManager,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata,
            QueryMonitor queryMonitor,
            LocationFactory locationFactory,
            ExecutionFactoriesManager executionFactoriesManager,
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
        this.executionFactoriesManager = requireNonNull(executionFactoriesManager, "executionFactoriesManager is null");

        this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");

        this.executor = requireNonNull(dispatchExecutor, "executorService is null").getExecutor();
        this.queryPrerequisitesManager = requireNonNull(queryPrerequisitesManager, "queryPrerequisitesManager is null");
    }

    /**
     *  This method instantiates a new dispatch query object as part of preparing phase for pre-query execution.
     *
     *  The dispatch query is submitted to the {@link ResourceGroupManager} which enqueues the query.
     *  The event of creating the dispatch query is logged after registering to the query tracker which is used to keep track of the state of the query.
     *  The log is done by adding a state change listener to the query.
     *  The state transition listener is useful to understand the state when a query has moved from created to running, running to error completed.
     *  Once dispatch query object is created and it's registered with the query tracker, start sending heard beat to indicate that this query is now running
     *  to the {@link ResourceGroupManager}. This is no-op for no disaggregated coordinator setup
     *
     * @param session the session
     * @param analyzerProvider the analyzer provider
     * @param query the query
     * @param preparedQuery the prepared query
     * @param slug the unique query slug for each {@code Query} object
     * @param retryCount the query retry count
     * @param resourceGroup the resource group to be used
     * @param queryType the query type derived from the {@code PreparedQuery statement}
     * @param warningCollector the warning collector
     * @param queryQueuer the query queuer is invoked when a query is to submit to the {@link com.facebook.presto.execution.resourceGroups.ResourceGroupManager}
     * @return
     */
    @Override
    public DispatchQuery createDispatchQuery(
            Session session,
            AnalyzerProvider analyzerProvider,
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
                preparedQuery.getPrepareSql(),
                session,
                locationFactory.createQueryLocation(session.getQueryId()),
                resourceGroup,
                queryType,
                preparedQuery.isTransactionControlStatement(),
                transactionManager,
                accessControl,
                executor,
                metadata,
                warningCollector);

        stateMachine.addStateChangeListener(new QueryStateTracingListener(stateMachine.getSession().getTracer().orElse(NoopTracerProvider.NOOP_TRACER)));
        queryMonitor.queryCreatedEvent(stateMachine.getBasicQueryInfo(Optional.empty()));

        ListenableFuture<QueryExecution> queryExecutionFuture = executor.submit(() -> {
            QueryExecutionFactory<?> queryExecutionFactory = executionFactoriesManager.getExecutionFactory(preparedQuery);
            if (queryExecutionFactory == null) {
                throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type: " + preparedQuery.getStatementClass().getSimpleName());
            }

            return queryExecutionFactory.createQueryExecution(analyzerProvider, preparedQuery, stateMachine, slug, retryCount, warningCollector, queryType);
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
