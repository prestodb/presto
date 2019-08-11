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
package com.facebook.presto.execution;

import com.facebook.presto.ExceededCpuLimitException;
import com.facebook.presto.Session;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.facebook.presto.execution.QueryExecution.QueryOutputInfo;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.execution.warnings.WarningCollectorFactory;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.server.SessionPropertyDefaults;
import com.facebook.presto.server.SessionSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.facebook.presto.sql.SqlEnvironmentConfig;
import com.facebook.presto.sql.SqlPath;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.version.EmbedVersion;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.getQueryMaxCpuTime;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryStateMachine.QUERY_STATE_LOG;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static com.facebook.presto.util.Failures.toFailure;
import static com.facebook.presto.util.StatementUtils.getQueryType;
import static com.facebook.presto.util.StatementUtils.isTransactionControlStatement;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

@ThreadSafe
public class SqlQueryManager
        implements QueryManager
{
    private static final Logger log = Logger.get(SqlQueryManager.class);

    private final QueryPreparer queryPreparer;

    private final EmbedVersion embedVersion;
    private final ExecutorService unboundedExecutorService;
    private final Executor boundedExecutor;
    private final ThreadPoolExecutorMBean queryExecutorMBean;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final ClusterMemoryManager memoryManager;

    private final Optional<String> path;
    private final int maxQueryLength;
    private final Duration maxQueryCpuTime;

    private final QueryTracker<QueryExecution> queryTracker;

    private final ScheduledExecutorService queryManagementExecutor;
    private final ThreadPoolExecutorMBean queryManagementExecutorMBean;

    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;

    private final TransactionManager transactionManager;
    private final AccessControl accessControl;

    private final QueryIdGenerator queryIdGenerator;

    private final SessionSupplier sessionSupplier;
    private final SessionPropertyDefaults sessionPropertyDefaults;

    private final ClusterSizeMonitor clusterSizeMonitor;

    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;

    private final SqlQueryManagerStats stats = new SqlQueryManagerStats();

    private final WarningCollectorFactory warningCollectorFactory;

    @Inject
    public SqlQueryManager(
            QueryPreparer queryPreparer,
            EmbedVersion embedVersion,
            NodeSchedulerConfig nodeSchedulerConfig,
            QueryManagerConfig queryManagerConfig,
            SqlEnvironmentConfig sqlEnvironmentConfig,
            QueryMonitor queryMonitor,
            ResourceGroupManager<?> resourceGroupManager,
            ClusterMemoryManager memoryManager,
            LocationFactory locationFactory,
            TransactionManager transactionManager,
            AccessControl accessControl,
            QueryIdGenerator queryIdGenerator,
            SessionSupplier sessionSupplier,
            SessionPropertyDefaults sessionPropertyDefaults,
            ClusterSizeMonitor clusterSizeMonitor,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories,
            WarningCollectorFactory warningCollectorFactory)
    {
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");

        this.embedVersion = requireNonNull(embedVersion, "embedVersion is null");
        this.executionFactories = requireNonNull(executionFactories, "executionFactories is null");

        this.unboundedExecutorService = newCachedThreadPool(threadsNamed("query-scheduler-%s"));
        this.boundedExecutor = new BoundedExecutor(unboundedExecutorService, queryManagerConfig.getQuerySubmissionMaxThreads());
        this.queryExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) unboundedExecutorService);

        requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null");
        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");

        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");

        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");

        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");

        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");

        this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");

        this.path = sqlEnvironmentConfig.getPath();
        this.maxQueryLength = queryManagerConfig.getMaxQueryLength();
        this.maxQueryCpuTime = queryManagerConfig.getQueryMaxCpuTime();

        this.warningCollectorFactory = requireNonNull(warningCollectorFactory, "warningCollectorFactory is null");

        queryManagementExecutor = Executors.newScheduledThreadPool(queryManagerConfig.getQueryManagerExecutorPoolSize(), threadsNamed("query-management-%s"));
        queryManagementExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryManagementExecutor);

        this.queryTracker = new QueryTracker<>(queryManagerConfig, queryManagementExecutor);
    }

    @PostConstruct
    public void start()
    {
        queryTracker.start();
        queryManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                enforceMemoryLimits();
            }
            catch (Throwable e) {
                log.error(e, "Error enforcing memory limits");
            }

            try {
                enforceCpuLimits();
            }
            catch (Throwable e) {
                log.error(e, "Error enforcing query CPU time limits");
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryTracker.stop();
        queryManagementExecutor.shutdownNow();
        unboundedExecutorService.shutdownNow();
    }

    @Override
    public List<BasicQueryInfo> getQueries()
    {
        return queryTracker.getAllQueries().stream()
                .map(queryExecution -> {
                    try {
                        return queryExecution.getBasicQueryInfo();
                    }
                    catch (RuntimeException ignored) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    @Override
    public void addOutputInfoListener(QueryId queryId, Consumer<QueryOutputInfo> listener)
    {
        requireNonNull(listener, "listener is null");

        queryTracker.getQuery(queryId).addOutputInfoListener(listener);
    }

    @Override
    public void addStateChangeListener(QueryId queryId, StateChangeListener<QueryState> listener)
    {
        requireNonNull(listener, "listener is null");

        queryTracker.getQuery(queryId).addStateChangeListener(listener);
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(query -> query.getStateChange(currentState))
                .orElseGet(() -> immediateFailedFuture(new NoSuchElementException()));
    }

    @Override
    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getBasicQueryInfo();
    }

    @Override
    public QueryInfo getFullQueryInfo(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getQueryInfo();
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getQueryPlan();
    }

    public void addFinalQueryInfoListener(QueryId queryId, StateChangeListener<QueryInfo> stateChangeListener)
    {
        queryTracker.getQuery(queryId).addFinalQueryInfoListener(stateChangeListener);
    }

    @Override
    public QueryState getQueryState(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getState();
    }

    @Override
    public void recordHeartbeat(QueryId queryId)
    {
        queryTracker.tryGetQuery(queryId)
                .ifPresent(QueryExecution::recordHeartbeat);
    }

    @Override
    public QueryId createQueryId()
    {
        return queryIdGenerator.createNextQueryId();
    }

    @Override
    public ListenableFuture<?> createQuery(QueryId queryId, SessionContext sessionContext, String query)
    {
        QueryCreationFuture queryCreationFuture = new QueryCreationFuture();
        boundedExecutor.execute(embedVersion.embedVersion(() -> {
            try {
                createQueryInternal(queryId, sessionContext, query, resourceGroupManager);
                queryCreationFuture.set(null);
            }
            catch (Throwable e) {
                queryCreationFuture.setException(e);
            }
        }));
        return queryCreationFuture;
    }

    private <C> void createQueryInternal(QueryId queryId, SessionContext sessionContext, String query, ResourceGroupManager<C> resourceGroupManager)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(sessionContext, "sessionFactory is null");
        requireNonNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query must not be empty string");
        checkArgument(!queryTracker.tryGetQuery(queryId).isPresent(), "query %s already exists", queryId);

        Session session = null;
        SelectionContext<C> selectionContext = null;
        QueryExecution queryExecution;
        PreparedQuery preparedQuery;
        Optional<QueryType> queryType = Optional.empty();
        try {
            clusterSizeMonitor.verifyInitialMinimumWorkersRequirement();

            if (query.length() > maxQueryLength) {
                int queryLength = query.length();
                query = query.substring(0, maxQueryLength);
                throw new PrestoException(QUERY_TEXT_TOO_LARGE, format("Query text length (%s) exceeds the maximum length (%s)", queryLength, maxQueryLength));
            }

            // decode session
            session = sessionSupplier.createSession(queryId, sessionContext);

            WarningCollector warningCollector = warningCollectorFactory.create();

            // prepare query
            preparedQuery = queryPreparer.prepareQuery(session, query, warningCollector);

            // select resource group
            queryType = getQueryType(preparedQuery.getStatement().getClass());
            selectionContext = resourceGroupManager.selectGroup(new SelectionCriteria(
                    sessionContext.getIdentity().getPrincipal().isPresent(),
                    sessionContext.getIdentity().getUser(),
                    Optional.ofNullable(sessionContext.getSource()),
                    sessionContext.getClientTags(),
                    sessionContext.getResourceEstimates(),
                    queryType.map(Enum::name)));

            // apply system defaults for query
            session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, queryType.map(Enum::name), selectionContext.getResourceGroupId());

            // mark existing transaction as active
            transactionManager.activateTransaction(session, isTransactionControlStatement(preparedQuery.getStatement()), accessControl);

            // create query execution
            QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(preparedQuery.getStatement().getClass());
            if (queryExecutionFactory == null) {
                throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type: " + preparedQuery.getStatement().getClass().getSimpleName());
            }
            queryExecution = queryExecutionFactory.createQueryExecution(
                    query,
                    session,
                    preparedQuery,
                    selectionContext.getResourceGroupId(),
                    warningCollector,
                    queryType);
        }
        catch (RuntimeException e) {
            // This is intentionally not a method, since after the state change listener is registered
            // it's not safe to do any of this, and we had bugs before where people reused this code in a method

            // if session creation failed, create a minimal session object
            if (session == null) {
                session = Session.builder(new SessionPropertyManager())
                        .setQueryId(queryId)
                        .setIdentity(sessionContext.getIdentity())
                        .setPath(new SqlPath(Optional.empty()))
                        .build();
            }
            QUERY_STATE_LOG.debug(e, "Query %s failed", session.getQueryId());

            // query failure fails the transaction
            session.getTransactionId().ifPresent(transactionManager::fail);

            QueryExecution execution = new FailedQueryExecution(
                    session,
                    query,
                    locationFactory.createQueryLocation(queryId),
                    Optional.ofNullable(selectionContext).map(SelectionContext::getResourceGroupId),
                    queryType,
                    unboundedExecutorService,
                    e);

            try {
                queryTracker.addQuery(execution);

                BasicQueryInfo queryInfo = execution.getBasicQueryInfo();
                queryMonitor.queryCreatedEvent(queryInfo);
                queryMonitor.queryImmediateFailureEvent(queryInfo, toFailure(e));
                stats.queryQueued();
                stats.queryStarted();
                stats.queryStopped();
                stats.queryFinished(execution.getQueryInfo());
            }
            finally {
                // execution MUST be added to the expiration queue or there will be a leak
                queryTracker.expireQuery(queryId);
            }

            return;
        }

        queryMonitor.queryCreatedEvent(queryExecution.getBasicQueryInfo());

        queryExecution.addFinalQueryInfoListener(finalQueryInfo -> {
            try {
                stats.queryFinished(finalQueryInfo);
                queryMonitor.queryCompletedEvent(finalQueryInfo);
            }
            finally {
                // execution MUST be added to the expiration queue or there will be a leak
                queryTracker.expireQuery(queryId);
            }
        });

        addStatsListeners(queryExecution);

        if (!queryTracker.addQuery(queryExecution)) {
            // query already created, so just exit
            return;
        }

        // start the query in the background
        try {
            resourceGroupManager.submit(preparedQuery.getStatement(), queryExecution, selectionContext, unboundedExecutorService);
        }
        catch (Throwable e) {
            failQuery(queryId, e);
        }
    }

    @Override
    public void failQuery(QueryId queryId, Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        queryTracker.tryGetQuery(queryId)
                .ifPresent(query -> query.fail(cause));
    }

    @Override
    public void cancelQuery(QueryId queryId)
    {
        log.debug("Cancel query %s", queryId);

        queryTracker.tryGetQuery(queryId)
                .ifPresent(QueryExecution::cancelQuery);
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        log.debug("Cancel stage %s", stageId);

        queryTracker.tryGetQuery(stageId.getQueryId())
                .ifPresent(query -> query.cancelStage(stageId));
    }

    @Override
    @Managed
    @Flatten
    public SqlQueryManagerStats getStats()
    {
        return stats;
    }

    @Managed(description = "Query scheduler executor")
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return queryExecutorMBean;
    }

    @Managed(description = "Query query management executor")
    @Nested
    public ThreadPoolExecutorMBean getManagementExecutor()
    {
        return queryManagementExecutorMBean;
    }

    @Managed
    public long getRunningTaskCount()
    {
        return queryTracker.getRunningTaskCount();
    }

    @Managed
    public long getQueriesKilledDueToTooManyTask()
    {
        return queryTracker.getQueriesKilledDueToTooManyTask();
    }

    /**
     * Enforce memory limits at the query level
     */
    private void enforceMemoryLimits()
    {
        List<QueryExecution> runningQueries = queryTracker.getAllQueries().stream()
                .filter(query -> query.getState() == RUNNING)
                .collect(toImmutableList());
        memoryManager.process(runningQueries, this::getQueries);
    }

    /**
     * Enforce query CPU time limits
     */
    private void enforceCpuLimits()
    {
        for (QueryExecution query : queryTracker.getAllQueries()) {
            Duration cpuTime = query.getTotalCpuTime();
            Duration sessionLimit = getQueryMaxCpuTime(query.getSession());
            Duration limit = Ordering.natural().min(maxQueryCpuTime, sessionLimit);
            if (cpuTime.compareTo(limit) > 0) {
                query.fail(new ExceededCpuLimitException(limit));
            }
        }
    }

    private void addStatsListeners(QueryExecution queryExecution)
    {
        Object lock = new Object();

        // QUEUED is the initial state, the counter can be incremented immediately
        stats.queryQueued();

        AtomicBoolean started = new AtomicBoolean();
        queryExecution.addStateChangeListener(newValue -> {
            synchronized (lock) {
                if (newValue == RUNNING && !started.getAndSet(true)) {
                    stats.queryStarted();
                }
            }
        });

        AtomicBoolean stopped = new AtomicBoolean();
        queryExecution.addStateChangeListener(newValue -> {
            synchronized (lock) {
                if (newValue.isDone() && !stopped.getAndSet(true) && started.get()) {
                    stats.queryStopped();
                }
            }
        });
    }

    private static class QueryCreationFuture
            extends AbstractFuture<QueryInfo>
    {
        @Override
        protected boolean set(QueryInfo value)
        {
            return super.set(value);
        }

        @Override
        protected boolean setException(Throwable throwable)
        {
            return super.setException(throwable);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            // query submission can not be canceled
            return false;
        }
    }
}
