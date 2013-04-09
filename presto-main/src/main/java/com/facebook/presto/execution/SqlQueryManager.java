/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlQueryManager
        implements QueryManager
{
    private static final Logger log = Logger.get(SqlQueryManager.class);

    private final ExecutorService queryExecutor;
    private final Duration maxQueryAge;

    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final ConcurrentMap<QueryId, QueryExecution> queries = new ConcurrentHashMap<>();

    private final Duration clientTimeout;

    private final ScheduledExecutorService queryManagementExecutor;
    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;

    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;

    @Inject
    public SqlQueryManager(QueryManagerConfig config,
            QueryMonitor queryMonitor,
            LocationFactory locationFactory,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories)
    {
        checkNotNull(config, "config is null");

        this.executionFactories = checkNotNull(executionFactories, "executionFactories is null");
        this.queryExecutor = Executors.newCachedThreadPool(threadsNamed("query-scheduler-%d"));
        this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");

        this.maxQueryAge = config.getMaxQueryAge();
        this.clientTimeout = config.getClientTimeout();

        queryManagementExecutor = Executors.newScheduledThreadPool(config.getQueryManagerExecutorPoolSize(), threadsNamed("query-management-%d"));

        queryManagementExecutor.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                for (QueryExecution queryExecution : queries.values()) {
                    try {
                        queryExecution.updateState(false);
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error updating state for query %s", queryExecution.getQueryInfo().getQueryId());
                    }
                }
            }
        }, 200, 200, TimeUnit.MILLISECONDS);

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
                catch (Exception ignored) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }

    @Override
    public QueryInfo getQueryInfo(QueryId queryId, boolean forceRefresh)
    {
        checkNotNull(queryId, "queryId is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            throw new NoSuchElementException();
        }
        // todo should this be a method on QueryExecution?
        query.getQueryInfo().getQueryStats().recordHeartBeat();
        if (forceRefresh) {
            query.updateState(forceRefresh);
        }
        return query.getQueryInfo();
    }

    @Override
    public QueryInfo createQuery(Session session, String query)
    {
        checkNotNull(query, "query is null");
        Preconditions.checkArgument(!query.isEmpty(), "query must not be empty string");

        QueryId queryId = new QueryId(String.valueOf(nextQueryId.getAndIncrement()));

        Statement statement;
        try {
            statement = SqlParser.createStatement(query);
        }
        catch (ParsingException e) {
            return createFailedQuery(session, query, queryId, e);
        }

        QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(statement.getClass());
        Preconditions.checkState(queryExecutionFactory != null, "Unsupported statement type %s", statement.getClass().getName());
        assert queryExecutionFactory != null; // IDEA-60343
        QueryExecution queryExecution = queryExecutionFactory.createQueryExecution(queryId, query, session, statement);

        queryMonitor.createdEvent(queryExecution.getQueryInfo());

        queries.put(queryId, queryExecution);

        // start the query in the background
        queryExecutor.submit(new QueryStarter(queryExecution));

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
        DateTime oldestAllowedQuery = DateTime.now().minus((long) maxQueryAge.toMillis());
        for (QueryExecution queryExecution : queries.values()) {
            try {
                QueryInfo queryInfo = queryExecution.getQueryInfo();
                DateTime endTime = queryInfo.getQueryStats().getEndTime();
                if (endTime != null && endTime.isBefore(oldestAllowedQuery)) {
                    removeQuery(queryExecution.getQueryInfo().getQueryId());
                }
            }
            catch (Exception e) {
                log.warn(e, "Error while inspecting age of query %s", queryExecution.getQueryInfo().getQueryId());
            }
        }
    }

    public void failAbandonedQueries()
    {
        DateTime now = DateTime.now();
        DateTime oldestAllowedHeartBeat = now.minus((long) clientTimeout.toMillis());
        for (QueryExecution queryExecution : queries.values()) {
            try {
                QueryInfo queryInfo = queryExecution.getQueryInfo();
                if (queryInfo.getState().isDone()) {
                    continue;
                }
                DateTime lastHeartBeat = queryInfo.getQueryStats().getLastHeartBeat();
                if (lastHeartBeat != null && lastHeartBeat.isBefore(oldestAllowedHeartBeat)) {
                    log.info("Failing abandoned query %s", queryInfo.getQueryId());
                    queryExecution.fail(new AbandonedException("Query " + queryInfo.getQueryId(), lastHeartBeat, now));
                }
            }
            catch (Exception e) {
                log.warn(e, "Error while inspecting age of query %s", queryExecution.getQueryInfo().getQueryId());
            }
        }
    }

    private QueryInfo createFailedQuery(Session session, String query, QueryId queryId, Throwable cause)
    {
        URI self = locationFactory.createQueryLocation(queryId);
        QueryExecution execution = new FailedQueryExecution(queryId, query, session, self, cause);

        queries.put(queryId, execution);
        queryMonitor.createdEvent(execution.getQueryInfo());
        queryMonitor.completionEvent(execution.getQueryInfo());

        return execution.getQueryInfo();
    }

    private static class QueryStarter
            implements Runnable
    {
        private final QueryExecution queryExecution;

        public QueryStarter(QueryExecution queryExecution)
        {
            this.queryExecution = queryExecution;
        }

        @Override
        public void run()
        {
            queryExecution.start();
        }
    }
}
