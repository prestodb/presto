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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.isBigQueryEnabled;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_QUEUE_FULL;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
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

        this.queryExecutor = newCachedThreadPool(threadsNamed("query-scheduler-%s"));
        this.queryExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryExecutor);

        checkNotNull(config, "config is null");
        this.queryStarter = new QueryStarter(queryExecutor, stats, config);

        this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
        this.queryIdGenerator = checkNotNull(queryIdGenerator, "queryIdGenerator is null");

        this.maxQueryAge = config.getMaxQueryAge();
        this.maxQueryHistory = config.getMaxQueryHistory();
        this.clientTimeout = config.getClientTimeout();

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
            query.fail(new PrestoException(USER_CANCELED, "Query was canceled"));
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

    /**
     * Remove completed queries after a waiting period
     */
    public void removeExpiredQueries()
    {
        DateTime timeHorizon = DateTime.now().minus(maxQueryAge.toMillis());

        // we're willing to keep queries beyond timeHorizon as long as we have fewer than maxQueryHistory
        while (expirationQueue.size() > maxQueryHistory) {
            QueryInfo queryInfo = expirationQueue.peek().getQueryInfo();

            // expirationQueue is FIFO based on query end time. Stop when we see the
            // first query that's too young to expire
            if (queryInfo.getQueryStats().getEndTime().isAfter(timeHorizon)) {
                return;
            }

            // only expire them if they are older than maxQueryAge. We need to keep them
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

    /**
     * Set up a callback to fire when a query is completed. The callback will be called at most once.
     */
    private static void addCompletionCallback(QueryExecution queryExecution, Runnable callback)
    {
        AtomicBoolean taskExecuted = new AtomicBoolean();
        queryExecution.addStateChangeListener(newValue -> {
            if (newValue.isDone() && taskExecuted.compareAndSet(false, true)) {
                callback.run();
            }
        });
        // Need to do this check in case the state changed before we added the previous state change listener
        if (queryExecution.getQueryInfo().getState().isDone() && taskExecuted.compareAndSet(false, true)) {
            callback.run();
        }
    }

    @ThreadSafe
    private static class QueryStarter
    {
        private final QueryQueue queryQueue;
        private final QueryQueue bigQueryQueue;

        public QueryStarter(Executor queryExecutor, SqlQueryManagerStats stats, QueryManagerConfig config)
        {
            checkNotNull(queryExecutor, "queryExecutor is null");
            checkNotNull(stats, "stats is null");
            checkNotNull(config, "config is null");

            this.queryQueue = new QueryQueue(queryExecutor, stats, config.getMaxQueuedQueries(), config.getMaxConcurrentQueries());
            this.bigQueryQueue = new QueryQueue(queryExecutor, stats, config.getMaxQueuedBigQueries(), config.getMaxConcurrentBigQueries());
        }

        public boolean submit(QueryExecution queryExecution)
        {
            if (isBigQueryEnabled(queryExecution.getQueryInfo().getSession(), false)) {
                return bigQueryQueue.enqueue(queryExecution);
            }
            else {
                return queryQueue.enqueue(queryExecution);
            }
        }

        public int getQueryQueueSize()
        {
            return queryQueue.getQueueSize();
        }

        public int getBigQueryQueueSize()
        {
            return bigQueryQueue.getQueueSize();
        }

        private static class QueryQueue
        {
            private final int maxQueuedQueries;
            private final AtomicInteger queryQueueSize = new AtomicInteger();
            private final AsyncSemaphore<QueueEntry> asyncSemaphore;

            private QueryQueue(Executor queryExecutor, SqlQueryManagerStats stats, int maxQueuedQueries, int maxConcurrentQueries)
            {
                checkNotNull(queryExecutor, "queryExecutor is null");
                checkNotNull(stats, "stats is null");
                checkArgument(maxQueuedQueries > 0, "maxQueuedQueries must be greater than zero");
                checkArgument(maxConcurrentQueries > 0, "maxConcurrentQueries must be greater than zero");

                this.maxQueuedQueries = maxQueuedQueries;
                this.asyncSemaphore = new AsyncSemaphore<>(maxConcurrentQueries,
                        queryExecutor,
                        queueEntry -> {
                            QueryExecution queryExecution = queueEntry.dequeue();
                            if (queryExecution == null) {
                                // Entry was dequeued earlier and so this query is already done
                                return Futures.immediateFuture(null);
                            }
                            else {
                                SettableFuture<?> settableFuture = SettableFuture.create();
                                addCompletionCallback(queryExecution, () -> settableFuture.set(null));
                                if (!settableFuture.isDone()) { // Only execute if the query is not already completed (e.g. cancelled)
                                    queryExecutor.execute(() -> {
                                        try (SetThreadName setThreadName = new SetThreadName("Query-%s", queryExecution.getQueryInfo().getQueryId())) {
                                            stats.queryStarted();
                                            queryExecution.start();
                                        }
                                    });
                                }
                                return settableFuture;
                            }
                        });
            }

            public int getQueueSize()
            {
                return queryQueueSize.get();
            }

            public boolean enqueue(QueryExecution queryExecution)
            {
                if (queryQueueSize.incrementAndGet() > maxQueuedQueries) {
                    queryQueueSize.decrementAndGet();
                    return false;
                }

                QueueEntry queueEntry = new QueueEntry(queryExecution, aVoid -> queryQueueSize.decrementAndGet());
                // Add a callback to dequeue the entry if it is ever completed.
                // This enables us to remove the entry sooner if is cancelled before starting,
                // and has no effect if called after starting.
                addCompletionCallback(queryExecution, queueEntry::dequeue);
                asyncSemaphore.submit(queueEntry);
                return true;
            }

            private static class QueueEntry
            {
                private final AtomicBoolean dequeued = new AtomicBoolean();
                private final AtomicReference<QueryExecution> queryExecution;
                private final Consumer<Void> onDequeue;

                private QueueEntry(QueryExecution queryExecution, Consumer<Void> onDequeue)
                {
                    checkNotNull(queryExecution, "queryExecution is null");
                    checkNotNull(onDequeue, "onDequeue is null");

                    this.queryExecution = new AtomicReference<>(queryExecution);
                    this.onDequeue = onDequeue;
                }

                /**
                 * Can be called multiple times on the same QueueEntry, but the onDequeue Consumer will only be called once
                 * and only one caller will get the QueryExecution.
                 */
                public QueryExecution dequeue()
                {
                    if (dequeued.compareAndSet(false, true)) {
                        onDequeue.accept(null);
                    }
                    return queryExecution.getAndSet(null);
                }
            }
        }
    }
}
