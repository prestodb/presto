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

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.SetThreadName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.isNull;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlQueryManager
        implements QueryManager
{
    private static final Logger log = Logger.get(SqlQueryManager.class);

    private final ExecutorService queryExecutor;
    private final ThreadPoolExecutorMBean queryExecutorMBean;

    private final int maxQueryHistory;
    private final Duration maxQueryAge;

    private final ConcurrentMap<QueryId, QueryExecution> queries = new ConcurrentHashMap<>();

    private final Duration clientTimeout;

    private final ScheduledExecutorService queryManagementExecutor;
    private final ThreadPoolExecutorMBean queryManagementExecutorMBean;

    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;
    private final QueryIdGenerator queryIdGenerator;

    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;

    private final SqlQueryManagerStats stats = new SqlQueryManagerStats();

    @Inject
    public SqlQueryManager(QueryManagerConfig config,
            QueryMonitor queryMonitor,
            QueryIdGenerator queryIdGenerator,
            LocationFactory locationFactory,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories)
    {
        checkNotNull(config, "config is null");

        this.executionFactories = checkNotNull(executionFactories, "executionFactories is null");

        this.queryExecutor = Executors.newCachedThreadPool(threadsNamed("query-scheduler-%d"));
        this.queryExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryExecutor);

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
                    removeExpiredQueries();
                }
                catch (Throwable e) {
                    log.warn(e, "Error removing old queries");
                }
                try {
                    failAbandonedQueries();
                }
                catch (Throwable e) {
                    log.warn(e, "Error removing old queries");
                }
            }
        }, 200, 200, TimeUnit.MILLISECONDS);
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
        return ImmutableList.copyOf(filter(transform(queries.values(), new Function<QueryExecution, QueryInfo>()
        {
            @Override
            public QueryInfo apply(QueryExecution queryExecution)
            {
                try {
                    return queryExecution.getQueryInfo();
                }
                catch (RuntimeException ignored) {
                    return null;
                }
            }
        }), Predicates.notNull()));
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
            statement = SqlParser.createStatement(query);
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
                }
            }
        });

        queries.put(queryId, queryExecution);

        // start the query in the background
        queryExecutor.submit(new QueryStarter(queryExecution, stats));

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
        List<QueryExecution> sortedQueries = IterableTransformer.on(queries.values())
                .select(compose(not(isNull()), endTimeGetter()))
                .orderBy(Ordering.natural().onResultOf(endTimeGetter()))
                .list();

        int toRemove = Math.max(sortedQueries.size() - maxQueryHistory, 0);
        DateTime oldestAllowedQuery = DateTime.now().minus(maxQueryAge.toMillis());

        for (QueryExecution queryExecution : sortedQueries) {
            try {
                DateTime endTime = queryExecution.getQueryInfo().getQueryStats().getEndTime();
                if ((endTime.isBefore(oldestAllowedQuery) || toRemove > 0) && isAbandoned(queryExecution)) {
                    removeQuery(queryExecution.getQueryInfo().getQueryId());
                    --toRemove;
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Error while inspecting age of query %s", queryExecution.getQueryInfo().getQueryId());
            }
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

                if (isAbandoned(queryExecution)) {
                    log.info("Failing abandoned query %s", queryExecution.getQueryInfo().getQueryId());
                    queryExecution.fail(new AbandonedException("Query " + queryInfo.getQueryId(), queryInfo.getQueryStats().getLastHeartbeat(), DateTime.now()));
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Error while inspecting age of query %s", queryExecution.getQueryInfo().getQueryId());
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
        queryMonitor.createdEvent(execution.getQueryInfo());
        queryMonitor.completionEvent(execution.getQueryInfo());

        return execution.getQueryInfo();
    }

    private static Function<QueryExecution, DateTime> endTimeGetter()
    {
        return new Function<QueryExecution, DateTime>()
        {
            @Nullable
            @Override
            public DateTime apply(QueryExecution input)
            {
                return input.getQueryInfo().getQueryStats().getEndTime();
            }
        };
    }

    private static class QueryStarter
            implements Runnable
    {
        private final QueryExecution queryExecution;
        private final SqlQueryManagerStats stats;

        public QueryStarter(QueryExecution queryExecution, SqlQueryManagerStats stats)
        {
            this.queryExecution = queryExecution;
            this.stats = stats;
        }

        @Override
        public void run()
        {
            try (SetThreadName setThreadName = new SetThreadName("Query-%s", queryExecution.getQueryInfo().getQueryId())) {
                stats.queryStarted();
                queryExecution.start();
            }
        }
    }
}
