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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.facebook.presto.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.SessionSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

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

import static com.facebook.presto.execution.ParameterExtractor.getParameterCount;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_QUERY;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.verifyExpressionIsConstant;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.threadsNamed;
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
    private final QueryQueueManager queueManager;
    private final ClusterMemoryManager memoryManager;

    private final int maxQueryHistory;
    private final Duration minQueryExpireAge;
    private final int maxQueryLength;

    private final ConcurrentMap<QueryId, QueryExecution> queries = new ConcurrentHashMap<>();
    private final Queue<QueryExecution> expirationQueue = new LinkedBlockingQueue<>();

    private final Duration clientTimeout;

    private final ScheduledExecutorService queryManagementExecutor;
    private final ThreadPoolExecutorMBean queryManagementExecutorMBean;

    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;

    private final TransactionManager transactionManager;

    private final AccessControl accessControl;

    private final QueryIdGenerator queryIdGenerator;

    private final SessionPropertyManager sessionPropertyManager;

    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;

    private final SqlQueryManagerStats stats = new SqlQueryManagerStats();

    @Inject
    public SqlQueryManager(
            SqlParser sqlParser,
            QueryManagerConfig config,
            QueryMonitor queryMonitor,
            QueryQueueManager queueManager,
            ClusterMemoryManager memoryManager,
            LocationFactory locationFactory,
            TransactionManager transactionManager,
            AccessControl accessControl,
            QueryIdGenerator queryIdGenerator,
            SessionPropertyManager sessionPropertyManager,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");

        this.executionFactories = requireNonNull(executionFactories, "executionFactories is null");

        this.queryExecutor = newCachedThreadPool(threadsNamed("query-scheduler-%s"));
        this.queryExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryExecutor);

        requireNonNull(config, "config is null");
        this.queueManager = requireNonNull(queueManager, "queueManager is null");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");

        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");

        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");

        this.accessControl = requireNonNull(accessControl, "accessControl is null");

        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");

        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");

        this.minQueryExpireAge = config.getMinQueryExpireAge();
        this.maxQueryHistory = config.getMaxQueryHistory();
        this.clientTimeout = config.getClientTimeout();
        this.maxQueryLength = config.getMaxQueryLength();

        queryManagementExecutor = Executors.newScheduledThreadPool(config.getQueryManagerExecutorPoolSize(), threadsNamed("query-management-%s"));
        queryManagementExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryManagementExecutor);
        queryManagementExecutor.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    failAbandonedQueries();
                }
                catch (Throwable e) {
                    log.warn(e, "Error cancelling abandoned queries");
                }

                try {
                    enforceMemoryLimits();
                }
                catch (Throwable e) {
                    log.warn(e, "Error enforcing memory limits");
                }

                try {
                    enforceQueryMaxRunTimeLimits();
                }
                catch (Throwable e) {
                    log.warn(e, "Error enforcing query timeout limits");
                }

                try {
                    removeExpiredQueries();
                }
                catch (Throwable e) {
                    log.warn(e, "Error removing expired queries");
                }

                try {
                    pruneExpiredQueries();
                }
                catch (Throwable e) {
                    log.warn(e, "Error pruning expired queries");
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
    public List<QueryInfo> getAllQueryInfo()
    {
        return queries.values().stream()
                .map(queryExecution -> {
                    try {
                        return queryExecution.getQueryInfo();
                    }
                    catch (RuntimeException ignored) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    @Override
    public Duration waitForStateChange(QueryId queryId, QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(maxWait, "maxWait is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            return maxWait;
        }

        return query.waitForStateChange(currentState, maxWait);
    }

    @Override
    public QueryInfo getQueryInfo(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            throw new NoSuchElementException();
        }

        return query.getQueryInfo();
    }

    @Override
    public Optional<QueryState> getQueryState(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");

        return Optional.ofNullable(queries.get(queryId))
                .map(QueryExecution::getState);
    }

    @Override
    public void recordHeartbeat(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            return;
        }

        query.recordHeartbeat();
    }

    @Override
    public QueryInfo createQuery(SessionSupplier sessionSupplier, String query)
    {
        requireNonNull(sessionSupplier, "sessionFactory is null");
        requireNonNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query must not be empty string");

        QueryId queryId = queryIdGenerator.createNextQueryId();

        Session session = null;
        QueryExecution queryExecution;
        Statement statement;
        try {
            session = sessionSupplier.createSession(queryId, transactionManager, accessControl, sessionPropertyManager);
            if (query.length() > maxQueryLength) {
                int queryLength = query.length();
                query = query.substring(0, maxQueryLength);
                throw new PrestoException(QUERY_TEXT_TOO_LARGE, format("Query text length (%s) exceeds the maximum length (%s)", queryLength, maxQueryLength));
            }

            Statement wrappedStatement = sqlParser.createStatement(query);
            statement = unwrapExecuteStatement(wrappedStatement, sqlParser, session);
            List<Expression> parameters = wrappedStatement instanceof Execute ? ((Execute) wrappedStatement).getParameters() : emptyList();
            validateParameters(statement, parameters);
            QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(statement.getClass());
            if (queryExecutionFactory == null) {
                throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type: " + statement.getClass().getSimpleName());
            }
            if (statement instanceof Explain && ((Explain) statement).isAnalyze()) {
                Statement innerStatement = ((Explain) statement).getStatement();
                if (!(executionFactories.get(innerStatement.getClass()) instanceof SqlQueryExecutionFactory)) {
                    throw new PrestoException(NOT_SUPPORTED, "EXPLAIN ANALYZE only supported for statements that are queries");
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
                        .setIdentity(sessionSupplier.getIdentity())
                        .build();
            }
            QueryExecution execution = new FailedQueryExecution(queryId, query, session, self, transactionManager, queryExecutor, e);

            QueryInfo queryInfo = null;
            try {
                queries.put(queryId, execution);

                queryInfo = execution.getQueryInfo();
                queryMonitor.queryCreatedEvent(queryInfo);
                queryMonitor.queryCompletedEvent(queryInfo);
                stats.queryFinished(queryInfo);
            }
            finally {
                // execution MUST be added to the expiration queue or there will be a leak
                expirationQueue.add(execution);
            }

            return queryInfo;
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

        addStatsListener(queryExecution);

        queries.put(queryId, queryExecution);

        // start the query in the background
        queueManager.submit(statement, queryExecution, queryExecutor);

        return queryInfo;
    }

    public static Statement unwrapExecuteStatement(Statement statement, SqlParser sqlParser, Session session)
    {
        if ((!(statement instanceof Execute))) {
            return statement;
        }

        String sql = session.getPreparedStatementFromExecute((Execute) statement);
        return sqlParser.createStatement(sql);
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
    public void cancelQuery(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");

        log.debug("Cancel query %s", queryId);

        QueryExecution query = queries.get(queryId);
        if (query != null) {
            query.cancelQuery();
        }
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        log.debug("Cancel stage %s", stageId);

        QueryExecution query = queries.get(stageId.getQueryId());
        if (query != null) {
            query.cancelStage(stageId);
        }
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
        memoryManager.process(queries.values().stream()
                .filter(query -> query.getState() == RUNNING)
                .collect(toImmutableList()));
    }

    /**
     * Enforce timeout at the query level
     */
    public void enforceQueryMaxRunTimeLimits()
    {
        for (QueryExecution query : queries.values()) {
            if (query.getState().isDone()) {
                continue;
            }
            Duration queryMaxRunTime = SystemSessionProperties.getQueryMaxRunTime(query.getSession());
            DateTime executionStartTime = query.getQueryInfo().getQueryStats().getCreateTime();
            if (executionStartTime.plus(queryMaxRunTime.toMillis()).isBeforeNow()) {
                query.fail(new PrestoException(EXCEEDED_TIME_LIMIT, "Query exceeded maximum time limit of " + queryMaxRunTime));
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
            QueryInfo queryInfo = queryExecution.getQueryInfo();
            if (queryInfo.getState().isDone()) {
                continue;
            }

            if (isAbandoned(queryInfo)) {
                log.info("Failing abandoned query %s", queryExecution.getQueryId());
                queryExecution.fail(new PrestoException(ABANDONED_QUERY, format("Query %s has not been accessed since %s: currentTime %s", queryInfo.getQueryId(), queryInfo.getQueryStats().getLastHeartbeat(), DateTime.now())));
            }
        }
    }

    private boolean isAbandoned(QueryInfo queryInfo)
    {
        DateTime oldestAllowedHeartbeat = DateTime.now().minus(clientTimeout.toMillis());
        DateTime lastHeartbeat = queryInfo.getQueryStats().getLastHeartbeat();

        return lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat);
    }

    private void addStatsListener(QueryExecution queryExecution)
    {
        Object lock = new Object();

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

    /**
     * Set up a callback to fire when a query is completed. The callback will be called at most once.
     */
    static void addCompletionCallback(QueryExecution queryExecution, Runnable callback)
    {
        AtomicBoolean taskExecuted = new AtomicBoolean();
        queryExecution.addStateChangeListener(newValue -> {
            if (newValue.isDone() && taskExecuted.compareAndSet(false, true)) {
                callback.run();
            }
        });
        // Need to do this check in case the state changed before we added the previous state change listener
        if (queryExecution.getState().isDone() && taskExecuted.compareAndSet(false, true)) {
            callback.run();
        }
    }
}
