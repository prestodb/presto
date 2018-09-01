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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.facebook.presto.execution.QueryExecution.QueryOutputInfo;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.server.SessionSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.facebook.presto.sql.SqlEnvironmentConfig;
import com.facebook.presto.sql.SqlPath;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.util.StatementUtils;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.getQueryMaxCpuTime;
import static com.facebook.presto.execution.ParameterExtractor.getParameterCount;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_QUERY;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.analyzer.ConstantExpressionVerifier.verifyExpressionIsConstant;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

@ThreadSafe
public class SqlQueryManager
        implements QueryManager
{
    private static final Logger log = Logger.get(SqlQueryManager.class);

    private final SqlParser sqlParser;

    private final ExecutorService queryExecutor;
    private final ThreadPoolExecutorMBean queryExecutorMBean;
    private final ResourceGroupManager resourceGroupManager;
    private final ClusterMemoryManager memoryManager;

    private final Optional<String> path;
    private final boolean isIncludeCoordinator;
    private final int maxQueryHistory;
    private final Duration minQueryExpireAge;
    private final int maxQueryLength;
    private final int initializationRequiredWorkers;
    private final Duration initializationTimeout;
    private final long initialNanos;
    private final Duration maxQueryCpuTime;

    private final ConcurrentMap<QueryId, QueryExecution> queries = new ConcurrentHashMap<>();
    private final Queue<QueryExecution> expirationQueue = new LinkedBlockingQueue<>();

    private final Duration clientTimeout;

    private final ScheduledExecutorService queryManagementExecutor;
    private final ThreadPoolExecutorMBean queryManagementExecutorMBean;

    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;

    private final Metadata metadata;
    private final TransactionManager transactionManager;

    private final QueryIdGenerator queryIdGenerator;

    private final SessionSupplier sessionSupplier;

    private final InternalNodeManager internalNodeManager;

    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;

    private final SqlQueryManagerStats stats = new SqlQueryManagerStats();

    private final AtomicBoolean acceptQueries = new AtomicBoolean();

    @Inject
    public SqlQueryManager(
            SqlParser sqlParser,
            NodeSchedulerConfig nodeSchedulerConfig,
            QueryManagerConfig queryManagerConfig,
            SqlEnvironmentConfig sqlEnvironmentConfig,
            QueryMonitor queryMonitor,
            ResourceGroupManager resourceGroupManager,
            ClusterMemoryManager memoryManager,
            LocationFactory locationFactory,
            TransactionManager transactionManager,
            QueryIdGenerator queryIdGenerator,
            SessionSupplier sessionSupplier,
            InternalNodeManager internalNodeManager,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories,
            Metadata metadata)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");

        this.executionFactories = requireNonNull(executionFactories, "executionFactories is null");

        this.queryExecutor = newCachedThreadPool(threadsNamed("query-scheduler-%s"));
        this.queryExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryExecutor);

        requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null");
        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");

        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");

        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadata = requireNonNull(metadata, "transactionManager is null");

        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");

        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");

        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");

        this.path = sqlEnvironmentConfig.getPath();
        this.isIncludeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();
        this.minQueryExpireAge = queryManagerConfig.getMinQueryExpireAge();
        this.maxQueryHistory = queryManagerConfig.getMaxQueryHistory();
        this.clientTimeout = queryManagerConfig.getClientTimeout();
        this.maxQueryLength = queryManagerConfig.getMaxQueryLength();
        this.maxQueryCpuTime = queryManagerConfig.getQueryMaxCpuTime();
        this.initializationRequiredWorkers = queryManagerConfig.getInitializationRequiredWorkers();
        this.initializationTimeout = queryManagerConfig.getInitializationTimeout();
        this.initialNanos = System.nanoTime();

        queryManagementExecutor = Executors.newScheduledThreadPool(queryManagerConfig.getQueryManagerExecutorPoolSize(), threadsNamed("query-management-%s"));
        queryManagementExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryManagementExecutor);
    }

    @PostConstruct
    public void start()
    {
        queryManagementExecutor.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    failAbandonedQueries();
                }
                catch (Throwable e) {
                    log.error(e, "Error cancelling abandoned queries");
                }

                try {
                    enforceMemoryLimits();
                }
                catch (Throwable e) {
                    log.error(e, "Error enforcing memory limits");
                }

                try {
                    enforceTimeLimits();
                }
                catch (Throwable e) {
                    log.error(e, "Error enforcing query timeout limits");
                }

                try {
                    enforceCpuLimits();
                }
                catch (Throwable e) {
                    log.error(e, "Error enforcing query CPU time limits");
                }

                try {
                    removeExpiredQueries();
                }
                catch (Throwable e) {
                    log.error(e, "Error removing expired queries");
                }

                try {
                    pruneExpiredQueries();
                }
                catch (Throwable e) {
                    log.error(e, "Error pruning expired queries");
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        boolean queryCancelled = false;
        for (QueryExecution queryExecution : queries.values()) {
            if (queryExecution.getState().isDone()) {
                continue;
            }

            log.info("Server shutting down. Query %s has been cancelled", queryExecution.getQueryId());
            queryExecution.fail(new PrestoException(SERVER_SHUTTING_DOWN, "Server is shutting down. Query " + queryExecution.getQueryId() + " has been cancelled"));
            queryCancelled = true;
        }
        if (queryCancelled) {
            try {
                TimeUnit.SECONDS.sleep(5);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        queryManagementExecutor.shutdownNow();
        queryExecutor.shutdownNow();
    }

    @Override
    public List<BasicQueryInfo> getQueries()
    {
        return queries.values().stream()
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

        getQueryExecution(queryId).addOutputInfoListener(listener);
    }

    @Override
    public void addStateChangeListener(QueryId queryId, StateChangeListener<QueryState> listener)
    {
        requireNonNull(listener, "listener is null");

        getQueryExecution(queryId).addStateChangeListener(listener);
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState)
    {
        return tryGetQueryExecution(queryId)
                .map(query -> query.getStateChange(currentState))
                .orElseGet(() -> immediateFailedFuture(new NoSuchElementException()));
    }

    @Override
    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return getQueryExecution(queryId).getBasicQueryInfo();
    }

    @Override
    public QueryInfo getFullQueryInfo(QueryId queryId)
    {
        return getQueryExecution(queryId).getQueryInfo();
    }

    @Override
    public Optional<ResourceGroupId> getQueryResourceGroup(QueryId queryId)
    {
        return tryGetQueryExecution(queryId)
                .flatMap(QueryExecution::getResourceGroup);
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return getQueryExecution(queryId).getQueryPlan();
    }

    @Override
    public QueryState getQueryState(QueryId queryId)
    {
        return getQueryExecution(queryId).getState();
    }

    @Override
    public void recordHeartbeat(QueryId queryId)
    {
        tryGetQueryExecution(queryId)
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
        queryExecutor.submit(() -> {
            try {
                createQueryInternal(queryId, sessionContext, query);
                queryCreationFuture.set(null);
            }
            catch (Throwable e) {
                queryCreationFuture.setException(e);
            }
        });
        return queryCreationFuture;
    }

    private void createQueryInternal(QueryId queryId, SessionContext sessionContext, String query)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(sessionContext, "sessionFactory is null");
        requireNonNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query must not be empty string");
        checkArgument(!queries.containsKey(queryId), "query %s already exists", queryId);

        Session session = null;
        SelectionContext<?> selectionContext;
        QueryExecution queryExecution;
        Statement statement;
        try {
            if (!acceptQueries.get()) {
                int activeWorkerCount = internalNodeManager.getNodes(ACTIVE).size();
                if (!isIncludeCoordinator) {
                    activeWorkerCount--;
                }
                if (nanosSince(initialNanos).compareTo(initializationTimeout) < 0 && activeWorkerCount < initializationRequiredWorkers) {
                    throw new PrestoException(
                            SERVER_STARTING_UP,
                            String.format("Cluster is still initializing, there are insufficient active worker nodes (%s) to run query", activeWorkerCount));
                }
                acceptQueries.set(true);
            }

            if (query.length() > maxQueryLength) {
                int queryLength = query.length();
                query = query.substring(0, maxQueryLength);
                throw new PrestoException(QUERY_TEXT_TOO_LARGE, format("Query text length (%s) exceeds the maximum length (%s)", queryLength, maxQueryLength));
            }

            Optional<String> queryType = getQueryType(query);
            selectionContext = resourceGroupManager.selectGroup(new SelectionCriteria(
                    sessionContext.getIdentity().getPrincipal().isPresent(),
                    sessionContext.getIdentity().getUser(),
                    Optional.ofNullable(sessionContext.getSource()),
                    sessionContext.getClientTags(),
                    sessionContext.getResourceEstimates(),
                    queryType));

            session = sessionSupplier.createSession(queryId, sessionContext, queryType, selectionContext.getResourceGroupId());
            Statement wrappedStatement = sqlParser.createStatement(query, createParsingOptions(session));
            statement = unwrapExecuteStatement(wrappedStatement, sqlParser, session);
            List<Expression> parameters = wrappedStatement instanceof Execute ? ((Execute) wrappedStatement).getParameters() : emptyList();
            validateParameters(statement, parameters);
            QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(statement.getClass());
            if (queryExecutionFactory == null) {
                throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type: " + statement.getClass().getSimpleName());
            }
            if (statement instanceof Explain && ((Explain) statement).isAnalyze()) {
                Statement innerStatement = ((Explain) statement).getStatement();
                Optional<QueryType> innerQueryType = StatementUtils.getQueryType(innerStatement.getClass());
                if (!innerQueryType.isPresent() || innerQueryType.get() == QueryType.DATA_DEFINITION) {
                    throw new PrestoException(NOT_SUPPORTED, "EXPLAIN ANALYZE doesn't support statement type: " + innerStatement.getClass().getSimpleName());
                }
            }
            queryExecution = queryExecutionFactory.createQueryExecution(queryId, query, session, statement, parameters);
        }
        catch (ParsingException | PrestoException | SemanticException e) {
            // This is intentionally not a method, since after the state change listener is registered
            // it's not safe to do any of this, and we had bugs before where people reused this code in a method
            URI self = locationFactory.createQueryLocation(queryId);

            // if session creation failed, create a minimal session object
            if (session == null) {
                session = Session.builder(new SessionPropertyManager())
                        .setQueryId(queryId)
                        .setIdentity(sessionContext.getIdentity())
                        .setPath(new SqlPath(Optional.empty()))
                        .build();
            }
            QueryExecution execution = new FailedQueryExecution(
                    queryId,
                    query,
                    Optional.empty(),
                    session,
                    self,
                    transactionManager,
                    queryExecutor,
                    metadata,
                    e);

            try {
                queries.putIfAbsent(queryId, execution);

                QueryInfo queryInfo = execution.getQueryInfo();
                queryMonitor.queryCreatedEvent(queryInfo);
                queryMonitor.queryCompletedEvent(queryInfo);
                stats.queryQueued();
                stats.queryStarted();
                stats.queryStopped();
                stats.queryFinished(queryInfo);
            }
            finally {
                // execution MUST be added to the expiration queue or there will be a leak
                expirationQueue.add(execution);
            }

            return;
        }

        QueryInfo queryInfo = queryExecution.getQueryInfo();
        queryMonitor.queryCreatedEvent(queryInfo);

        queryExecution.addFinalQueryInfoListener(finalQueryInfo -> {
            try {
                QueryInfo info = queryExecution.getQueryInfo();
                stats.queryFinished(info);
                queryMonitor.queryCompletedEvent(info);
            }
            finally {
                // execution MUST be added to the expiration queue or there will be a leak
                expirationQueue.add(queryExecution);
            }
        });

        addStatsListeners(queryExecution);

        if (queries.putIfAbsent(queryId, queryExecution) != null) {
            // query already created, so just exit
            return;
        }

        // start the query in the background
        resourceGroupManager.submit(statement, queryExecution, selectionContext, queryExecutor);
    }

    private Optional<String> getQueryType(String query)
    {
        Statement statement = sqlParser.createStatement(query, new ParsingOptions(AS_DECIMAL));
        return StatementUtils.getQueryType(statement.getClass()).map(Enum::name);
    }

    public static Statement unwrapExecuteStatement(Statement statement, SqlParser sqlParser, Session session)
    {
        if ((!(statement instanceof Execute))) {
            return statement;
        }

        String sql = session.getPreparedStatementFromExecute((Execute) statement);
        return sqlParser.createStatement(sql, createParsingOptions(session));
    }

    public static void validateParameters(Statement node, List<Expression> parameterValues)
    {
        int parameterCount = getParameterCount(node);
        if (parameterValues.size() != parameterCount) {
            throw new SemanticException(INVALID_PARAMETER_USAGE, node, "Incorrect number of parameters: expected %s but found %s", parameterCount, parameterValues.size());
        }
        for (Expression expression : parameterValues) {
            verifyExpressionIsConstant(emptySet(), expression);
        }
    }

    @Override
    public void failQuery(QueryId queryId, Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        tryGetQueryExecution(queryId)
                .ifPresent(query -> query.fail(cause));
    }

    @Override
    public void cancelQuery(QueryId queryId)
    {
        log.debug("Cancel query %s", queryId);

        tryGetQueryExecution(queryId)
                .ifPresent(QueryExecution::cancelQuery);
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        log.debug("Cancel stage %s", stageId);

        tryGetQueryExecution(stageId.getQueryId())
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

    @Managed(description = "Query garbage collector executor")
    @Nested
    public ThreadPoolExecutorMBean getManagementExecutor()
    {
        return queryManagementExecutorMBean;
    }

    /**
     * Enforce memory limits at the query level
     */
    public void enforceMemoryLimits()
    {
        List<QueryExecution> runningQueries = queries.values().stream()
                .filter(query -> query.getState() == RUNNING)
                .collect(toImmutableList());
        memoryManager.process(runningQueries, this::getQueries);
    }

    /**
     * Enforce query max runtime/execution time limits
     */
    public void enforceTimeLimits()
    {
        for (QueryExecution query : queries.values()) {
            if (query.getState().isDone()) {
                continue;
            }
            Duration queryMaxRunTime = SystemSessionProperties.getQueryMaxRunTime(query.getSession());
            Duration queryMaxExecutionTime = SystemSessionProperties.getQueryMaxExecutionTime(query.getSession());
            DateTime executionStartTime = query.getQueryInfo().getQueryStats().getExecutionStartTime();
            DateTime createTime = query.getQueryInfo().getQueryStats().getCreateTime();
            if (executionStartTime != null && executionStartTime.plus(queryMaxExecutionTime.toMillis()).isBeforeNow()) {
                query.fail(new PrestoException(EXCEEDED_TIME_LIMIT, "Query exceeded the maximum execution time limit of " + queryMaxExecutionTime));
            }
            if (createTime.plus(queryMaxRunTime.toMillis()).isBeforeNow()) {
                query.fail(new PrestoException(EXCEEDED_TIME_LIMIT, "Query exceeded maximum time limit of " + queryMaxRunTime));
            }
        }
    }

    /**
     * Enforce query CPU time limits
     */
    public void enforceCpuLimits()
    {
        for (QueryExecution query : queries.values()) {
            Duration cpuTime = query.getTotalCpuTime();
            Duration sessionLimit = getQueryMaxCpuTime(query.getSession());
            Duration limit = Ordering.natural().min(maxQueryCpuTime, sessionLimit);
            if (cpuTime.compareTo(limit) > 0) {
                query.fail(new ExceededCpuLimitException(limit));
            }
        }
    }

    /**
     * Prune extraneous info from old queries
     */
    private void pruneExpiredQueries()
    {
        if (expirationQueue.size() <= maxQueryHistory) {
            return;
        }

        int count = 0;
        // we're willing to keep full info for up to maxQueryHistory queries
        for (QueryExecution query : expirationQueue) {
            if (expirationQueue.size() - count <= maxQueryHistory) {
                break;
            }
            query.pruneInfo();
            count++;
        }
    }

    /**
     * Remove completed queries after a waiting period
     */
    private void removeExpiredQueries()
    {
        DateTime timeHorizon = DateTime.now().minus(minQueryExpireAge.toMillis());

        // we're willing to keep queries beyond timeHorizon as long as we have fewer than maxQueryHistory
        while (expirationQueue.size() > maxQueryHistory) {
            QueryInfo queryInfo = expirationQueue.peek().getQueryInfo();

            // expirationQueue is FIFO based on query end time. Stop when we see the
            // first query that's too young to expire
            if (queryInfo.getQueryStats().getEndTime().isAfter(timeHorizon)) {
                return;
            }

            // only expire them if they are older than minQueryExpireAge. We need to keep them
            // around for a while in case clients come back asking for status
            QueryId queryId = queryInfo.getQueryId();

            log.debug("Remove query %s", queryId);
            queries.remove(queryId);
            expirationQueue.remove();
        }
    }

    public void failAbandonedQueries()
    {
        for (QueryExecution queryExecution : queries.values()) {
            try {
                QueryInfo queryInfo = queryExecution.getQueryInfo();
                if (queryInfo.getState().isDone()) {
                    continue;
                }

                if (isAbandoned(queryInfo)) {
                    log.info("Failing abandoned query %s", queryExecution.getQueryId());
                    queryExecution.fail(new PrestoException(ABANDONED_QUERY, format("Query %s has not been accessed since %s: currentTime %s", queryInfo.getQueryId(), queryInfo.getQueryStats().getLastHeartbeat(), DateTime.now())));
                }
            }
            catch (RuntimeException e) {
                log.error(e, "Exception failing abandoned query %s", queryExecution.getQueryId());
            }
        }
    }

    private boolean isAbandoned(QueryInfo queryInfo)
    {
        DateTime oldestAllowedHeartbeat = DateTime.now().minus(clientTimeout.toMillis());
        DateTime lastHeartbeat = queryInfo.getQueryStats().getLastHeartbeat();

        return lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat);
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
        synchronized (lock) {
            // Need to do this check in case the state changed before we added the previous state change listener
            if (queryExecution.getState() == RUNNING && !started.getAndSet(true)) {
                stats.queryStarted();
            }
        }

        AtomicBoolean stopped = new AtomicBoolean();
        queryExecution.addStateChangeListener(newValue -> {
            synchronized (lock) {
                if (newValue.isDone() && !stopped.getAndSet(true) && started.get()) {
                    stats.queryStopped();
                }
            }
        });
        synchronized (lock) {
            // Need to do this check in case the state changed before we added the previous state change listener
            if (queryExecution.getState().isDone() && !stopped.getAndSet(true) && started.get()) {
                stats.queryStopped();
            }
        }
    }

    private QueryExecution getQueryExecution(QueryId queryId)
    {
        return tryGetQueryExecution(queryId)
                .orElseThrow(NoSuchElementException::new);
    }

    private Optional<QueryExecution> tryGetQueryExecution(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        return Optional.ofNullable(queries.get(queryId));
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
