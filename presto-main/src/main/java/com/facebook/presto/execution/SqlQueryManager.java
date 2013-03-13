/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.QueryInfo.QueryInfoFactory;
import com.facebook.presto.execution.SimpleSqlExecution.SimpleSqlExecutionFactory;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Session;
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
    private final Metadata metadata;
    private final SplitManager splitManager;
    private final NodeManager nodeManager;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final Duration maxQueryAge;
    private final QueryMonitor queryMonitor;

    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final ConcurrentMap<String, QueryExecution> queries = new ConcurrentHashMap<>();

    private final Duration clientTimeout;
    private final int maxPendingSplitsPerNode;

    private final ScheduledExecutorService queryManagementExecutor;

    private final Map<Class<? extends Statement>, SimpleSqlExecutionFactory<?>> simpleExecutions;

    @Inject
    public SqlQueryManager(Metadata metadata,
            SplitManager splitManager,
            LocationFactory locationFactory,
            QueryManagerConfig config,
            NodeManager nodeManager,
            RemoteTaskFactory remoteTaskFactory,
            QueryMonitor queryMonitor,
            Map<Class<? extends Statement>, SimpleSqlExecutionFactory<?>> simpleExecutions)
    {
        checkNotNull(metadata, "metadata is null");
        checkNotNull(splitManager, "splitManager is null");
        checkNotNull(nodeManager, "nodeManager is null");
        checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        checkNotNull(locationFactory, "locationFactory is null");
        checkNotNull(config, "config is null");
        checkNotNull(queryMonitor, "queryMonitor is null");
        checkNotNull(simpleExecutions, "simpleExecutions is null");

        this.queryExecutor = Executors.newCachedThreadPool(threadsNamed("query-scheduler-%d"));

        this.metadata = metadata;
        this.splitManager = splitManager;
        this.nodeManager = nodeManager;
        this.remoteTaskFactory = remoteTaskFactory;
        this.locationFactory = locationFactory;
        this.queryMonitor = queryMonitor;
        this.simpleExecutions = simpleExecutions;

        this.maxQueryAge = config.getMaxQueryAge();
        this.clientTimeout = config.getClientTimeout();
        this.maxPendingSplitsPerNode = config.getMaxPendingSplitsPerNode();

        queryManagementExecutor = Executors.newScheduledThreadPool(100, threadsNamed("query-management-%d"));

        queryManagementExecutor.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                for (QueryExecution queryExecution : queries.values()) {
                    try {
                        queryExecution.updateState();
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error updating state for query %s", queryExecution.getQueryId());
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
    public QueryInfo getQueryInfo(String queryId, boolean forceRefresh)
    {
        checkNotNull(queryId, "queryId is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            throw new NoSuchElementException();
        }
        // todo should this be a method on QueryExecution?
        query.getQueryInfo().getQueryStats().recordHeartBeat();
        if (forceRefresh) {
            query.updateState();
        }
        return query.getQueryInfo();
    }

    @Override
    public QueryInfo createQuery(Session session, String query)
    {
        checkNotNull(query, "query is null");
        Preconditions.checkArgument(query.length() > 0, "query must not be empty string");

        String queryId = String.valueOf(nextQueryId.getAndIncrement());

        QueryInfoFactory queryInfoFactory = new QueryInfoFactory(queryId, query, session);
        QueryExecution queryExecution;

        // parse the SQL query
        Statement statement = SqlParser.createStatement(query);

        SimpleSqlExecutionFactory<?> queryExecutionFactory = simpleExecutions.get(statement.getClass());
        if (queryExecutionFactory != null) {
            queryExecution = queryExecutionFactory.createQueryExecution(queryId,
                    statement,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    queryInfoFactory);
        }
        else {
            queryExecution = new SqlQueryExecution(queryId,
                    statement,
                    session,
                    metadata,
                    splitManager,
                    nodeManager,
                    remoteTaskFactory,
                    locationFactory,
                    queryMonitor,
                    maxPendingSplitsPerNode,
                    queryExecutor,
                    queryInfoFactory);
        }

        queryMonitor.createdEvent(queryExecution.getQueryInfo());
        queries.put(queryExecution.getQueryId(), queryExecution);

        // start the query in the background
        queryExecutor.submit(new QueryStarter(queryExecution));

        return queryExecution.getQueryInfo();
    }

    @Override
    public void cancelQuery(String queryId)
    {
        checkNotNull(queryId, "queryId is null");

        log.debug("Cancel query %s", queryId);

        QueryExecution query = queries.get(queryId);
        if (query != null) {
            query.cancel();
        }
    }

    @Override
    public void cancelStage(String queryId, String stageId)
    {
        checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");

        log.debug("Cancel query %s stage %s", queryId, stageId);

        QueryExecution query = queries.get(queryId);
        if (query != null) {
            query.cancelStage(stageId);
        }
    }

    public void removeQuery(String queryId)
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
                    removeQuery(queryExecution.getQueryId());
                }
            }
            catch (Exception e) {
                log.warn(e, "Error while inspecting age of query %s", queryExecution.getQueryId());
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
                log.warn(e, "Error while inspecting age of query %s", queryExecution.getQueryId());
            }
        }
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
