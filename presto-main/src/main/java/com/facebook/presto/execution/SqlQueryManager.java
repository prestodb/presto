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
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.AsyncSemaphore;
import io.airlift.concurrent.SetThreadName;
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
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.SystemSessionProperties.isBigQueryEnabled;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_QUEUE_FULL;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@ThreadSafe
public class SqlQueryManager
        implements QueryManager
{
    private static final Logger log = Logger.get(SqlQueryManager.class);

    private final SqlParser sqlParser;

    private final ExecutorService queryExecutor;
    private final ThreadPoolExecutorMBean queryExecutorMBean;
    private final QueryStarter queryStarter;

    private final int maxQueryHistory;
    private final Duration maxQueryAge;

    private final ConcurrentMap<QueryId, QueryExecution> queries = new ConcurrentHashMap<>();
    private final Queue<QueryExecution> expirationQueue = new LinkedBlockingQueue<>();

    private final Duration clientTimeout;

    private final ScheduledExecutorService queryManagementExecutor;
    private final ThreadPoolExecutorMBean queryManagementExecutorMBean;

    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;
    private final QueryIdGenerator queryIdGenerator;

    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;

    private final SqlQueryManagerStats stats = new SqlQueryManagerStats();

    @Inject
    public SqlQueryManager(
            SqlParser sqlParser,
            QueryManagerConfig config,
            QueryMonitor queryMonitor,
            QueryIdGenerator queryIdGenerator,
            LocationFactory locationFactory,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories)
    {
        this.sqlParser = checkNotNull(sqlParser, "sqlParser is null");

        this.executionFactories = checkNotNull(executionFactories, "executionFactories is null");

        this.queryExecutor = newCachedThreadPool(threadsNamed("query-scheduler-%d"));
        this.queryExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryExecutor);

        checkNotNull(config, "config is null");
        this.queryStarter = new QueryStarter(queryExecutor, stats, config);

        this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
        this.queryIdGenerator = checkNotNull(queryIdGenerator, "queryIdGenerator is null");

        this.maxQueryAge = config.getMaxQueryAge();
        this.maxQueryHistory = config.getMaxQueryHistory();
        this.clientTimeout = config.getClientTimeout();

        queryManagementExecutor = Executors.newScheduledThreadPool(config.getQueryManagerExecutorPoolSize(), threadsNamed("query-management-%d"));
        queryManagementExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryManagementExecutor);
        queryManagementExecutor.scheduleAtFixedRate(new Runnable()
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
                    removeExpiredQueries();
                }
                catch (Throwable e) {
                    log.warn(e, "Error removing expired queries");
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
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
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(maxWait, "maxWait is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            return maxWait;
        }

        query.recordHeartbeat();
        return query.waitForStateChange(currentState, maxWait);
    }

    @Override
    public QueryInfo getQueryInfo(QueryId queryId)
    {
        checkNotNull(queryId, "queryId is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            throw new NoSuchElementException();
        }

        query.recordHeartbeat();
        return query.getQueryInfo();
    }

    @Override
    public QueryInfo createQuery(Session session, String query)
    {
        checkNotNull(query, "query is null");
        Preconditions.checkArgument(!query.isEmpty(), "query must not be empty string");

        QueryId queryId = queryIdGenerator.createNextQueryId();

        Statement statement;
        try {
            statement = sqlParser.createStatement(query);
        }
        catch (ParsingException e) {
            return createFailedQuery(session, query, queryId, e);
        }

        QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(statement.getClass());
        Preconditions.checkState(queryExecutionFactory != null, "Unsupported statement type %s", statement.getClass().getName());
        final QueryExecution queryExecution = queryExecutionFactory.createQueryExecution(queryId, query, session, statement);
        queryMonitor.createdEvent(queryExecution.getQueryInfo());

        queryExecution.addStateChangeListener(new StateChangeListener<QueryState>()
        {
            @Override
            public void stateChanged(QueryState newValue)
            {
                if (newValue.isDone()) {
                    QueryInfo info = queryExecution.getQueryInfo();

                    stats.queryFinished(info);
                    queryMonitor.completionEvent(info);
                    expirationQueue.add(queryExecution);
                }
            }
        });

        queries.put(queryId, queryExecution);

        // start the query in the background
        if (!queryStarter.submit(queryExecution)) {
            return createFailedQuery(session, query, queryId, new PrestoException(QUERY_QUEUE_FULL, "Too many queued queries!"));
        }

        return queryExecution.getQueryInfo();
    }

    @Override
    public void cancelQuery(QueryId queryId)
    {
        checkNotNull(queryId, "queryId is null");

        log.debug("Cancel query %s", queryId);

        QueryExecution query = queries.get(queryId);
        if (query != null) {
            query.cancel();
        }
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        Preconditions.checkNotNull(stageId, "stageId is null");

        log.debug("Cancel stage %s", stageId);

        QueryExecution query = queries.get(stageId.getQueryId());
        if (query != null) {
            query.cancelStage(stageId);
        }
    }

    @Managed
    public int getQueryQueueSize()
    {
        return queryStarter.getQueryQueueSize();
    }

    @Managed
    public int getBigQueryQueueSize()
    {
        return queryStarter.getBigQueryQueueSize();
    }

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

    public void removeQuery(QueryId queryId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");

        log.debug("Remove query %s", queryId);

        QueryExecution query = queries.remove(queryId);
        if (query != null) {
            query.cancel();
        }
    }

    /**
     * Remove completed queries after a waiting period
     */
    public void removeExpiredQueries()
    {
        DateTime timeHorizon = DateTime.now().minus(maxQueryAge.toMillis());

        // we're willing to keep queries beyond timeHorizon as long as we have fewer than maxQueryHistory
        while (expirationQueue.size() > maxQueryHistory) {
            QueryInfo info = expirationQueue.peek().getQueryInfo();

            // expirationQueue is FIFO based on query end time. Stop when we see the
            // first query that's too young to expire
            if (info.getQueryStats().getEndTime().isAfter(timeHorizon)) {
                return;
            }

            // only expire them if they are older than maxQueryAge. We need to keep them
            // around for a while in case clients come back asking for status
            removeQuery(info.getQueryId());
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

            if (isAbandoned(queryExecution)) {
                log.info("Failing abandoned query %s", queryExecution.getQueryInfo().getQueryId());
                queryExecution.fail(new AbandonedException("Query " + queryInfo.getQueryId(), queryInfo.getQueryStats().getLastHeartbeat(), DateTime.now()));
            }
        }
    }

    private boolean isAbandoned(QueryExecution query)
    {
        DateTime oldestAllowedHeartbeat = DateTime.now().minus(clientTimeout.toMillis());
        DateTime lastHeartbeat = query.getQueryInfo().getQueryStats().getLastHeartbeat();

        return lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat);
    }

    private QueryInfo createFailedQuery(Session session, String query, QueryId queryId, Throwable cause)
    {
        URI self = locationFactory.createQueryLocation(queryId);
        QueryExecution execution = new FailedQueryExecution(queryId, query, session, self, queryExecutor, cause);

        queries.put(queryId, execution);
        stats.queryStarted();
        queryMonitor.createdEvent(execution.getQueryInfo());
        queryMonitor.completionEvent(execution.getQueryInfo());
        stats.queryFinished(execution.getQueryInfo());
        expirationQueue.add(execution);

        return execution.getQueryInfo();
    }

    private static class QueryStarter
    {
        private final int maxQueuedQueries;
        private final AtomicInteger queryQueueSize = new AtomicInteger();
        private final AsyncSemaphore<QueryExecution> queryAsyncSemaphore;

        private final int maxQueuedBigQueries;
        private final AtomicInteger bigQueryQueueSize = new AtomicInteger();
        private final AsyncSemaphore<QueryExecution> bigQueryAsyncSemaphore;

        public QueryStarter(Executor queryExecutor, SqlQueryManagerStats stats, QueryManagerConfig config)
        {
            checkNotNull(queryExecutor, "queryExecutor is null");
            checkNotNull(stats, "stats is null");

            this.maxQueuedQueries = config.getMaxQueuedQueries();
            this.queryAsyncSemaphore = new AsyncSemaphore<>(config.getMaxConcurrentQueries(), queryExecutor, new QuerySubmitter(queryExecutor, stats, queryQueueSize));
            this.maxQueuedBigQueries = config.getMaxQueuedBigQueries();
            this.bigQueryAsyncSemaphore = new AsyncSemaphore<>(config.getMaxConcurrentBigQueries(), queryExecutor, new QuerySubmitter(queryExecutor, stats, bigQueryQueueSize));
        }

        public boolean submit(QueryExecution queryExecution)
        {
            AtomicInteger queueSize;
            int maxQueueSize;
            AsyncSemaphore<QueryExecution> asyncSemaphore;
            if (isBigQueryEnabled(queryExecution.getQueryInfo().getSession(), false)) {
                queueSize = bigQueryQueueSize;
                maxQueueSize = maxQueuedBigQueries;
                asyncSemaphore = bigQueryAsyncSemaphore;
            }
            else {
                queueSize = queryQueueSize;
                maxQueueSize = maxQueuedQueries;
                asyncSemaphore = queryAsyncSemaphore;
            }
            if (queueSize.incrementAndGet() > maxQueueSize) {
                queueSize.decrementAndGet();
                return false;
            }
            asyncSemaphore.submit(queryExecution);
            return true;
        }

        public int getQueryQueueSize()
        {
            return queryQueueSize.get();
        }

        public int getBigQueryQueueSize()
        {
            return bigQueryQueueSize.get();
        }

        private static class QuerySubmitter
                implements Function<QueryExecution, ListenableFuture<?>>
        {
            private final Executor queryExecutor;
            private final SqlQueryManagerStats stats;
            private final AtomicInteger queueSize;

            public QuerySubmitter(Executor queryExecutor, SqlQueryManagerStats stats, AtomicInteger queueSize)
            {
                this.queryExecutor = checkNotNull(queryExecutor, "queryExecutor is null");
                this.stats = checkNotNull(stats, "stats is null");
                this.queueSize = checkNotNull(queueSize, "queueSize is null");
            }

            @Override
            public ListenableFuture<?> apply(final QueryExecution queryExecution)
            {
                queueSize.decrementAndGet();
                final SettableFuture<?> settableFuture = SettableFuture.create();
                queryExecution.addStateChangeListener(new StateChangeListener<QueryState>()
                {
                    @Override
                    public void stateChanged(QueryState newValue)
                    {
                        if (newValue.isDone()) {
                            settableFuture.set(null);
                        }
                    }
                });
                if (queryExecution.getQueryInfo().getState().isDone()) {
                    settableFuture.set(null);
                }
                else {
                    queryExecutor.execute(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            try (SetThreadName setThreadName = new SetThreadName("Query-%s", queryExecution.getQueryInfo().getQueryId())) {
                                stats.queryStarted();
                                queryExecution.start();
                            }
                        }
                    });
                }
                return settableFuture;
            }
        }
    }
}
