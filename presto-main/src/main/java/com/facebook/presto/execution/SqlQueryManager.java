/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.event.queryinfo.QueryEventFactory;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Session;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.event.client.EventClient;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.List;
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
    private final ImportClientFactory importClientFactory;
    private final ImportManager importManager;
    private final Metadata metadata;
    private final NodeManager nodeManager;
    private final SplitManager splitManager;
    private final StageManager stageManager;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final Duration maxQueryAge;
    private final QueryEventFactory queryEventFactory;
    private final EventClient eventClient;

    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final ConcurrentMap<String, QueryExecution> queries = new ConcurrentHashMap<>();

    private final boolean importsEnabled;
    private final Duration clientTimeout;


    @Inject
    public SqlQueryManager(ImportClientFactory importClientFactory,
            ImportManager importManager,
            Metadata metadata,
            NodeManager nodeManager,
            SplitManager splitManager,
            StageManager stageManager,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            QueryManagerConfig config,
            QueryEventFactory queryEventFactory,
            EventClient eventClient)
    {
        checkNotNull(importClientFactory, "importClientFactory is null");
        checkNotNull(importManager, "importManager is null");
        checkNotNull(metadata, "metadata is null");
        checkNotNull(nodeManager, "nodeManager is null");
        checkNotNull(splitManager, "splitManager is null");
        checkNotNull(stageManager, "stageManager is null");
        checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        checkNotNull(locationFactory, "locationFactory is null");
        checkNotNull(config, "config is null");
        checkNotNull(queryEventFactory, "queryEventFactory is null");
        checkNotNull(eventClient, "eventClient is null");

        this.queryExecutor = Executors.newCachedThreadPool(threadsNamed("query-processor-%d"));

        this.importClientFactory = importClientFactory;
        this.importManager = importManager;
        this.metadata = metadata;
        this.nodeManager = nodeManager;
        this.splitManager = splitManager;
        this.stageManager = stageManager;
        this.remoteTaskFactory = remoteTaskFactory;
        this.locationFactory = locationFactory;
        this.queryEventFactory = queryEventFactory;
        this.eventClient = eventClient;
        this.importsEnabled = config.isImportsEnabled();
        this.maxQueryAge = config.getMaxQueryAge();
        this.clientTimeout = config.getClientTimeout();

        ScheduledExecutorService queryManagementExecutor = Executors.newScheduledThreadPool(100, threadsNamed("query-management-%d"));

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
                    cancelAbandonedQueries();
                }
                catch (Throwable e) {
                    log.warn(e, "Error removing old queries");
                }
            }
        }, 200, 200, TimeUnit.MILLISECONDS);
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
    public QueryInfo createQuery(String query)
    {
        checkNotNull(query, "query is null");
        Preconditions.checkArgument(query.length() > 0, "query must not be empty string");

        String queryId = String.valueOf(nextQueryId.getAndIncrement());
        QueryExecution queryExecution;
        if (query.startsWith("import-table:")) {
            Preconditions.checkState(importsEnabled, "Imports are currently disabled");

            // todo this is a hack until we have language support for import or create table as select
            ImmutableList<String> strings = ImmutableList.copyOf(Splitter.on(":").split(query));
            queryExecution = new ImportTableExecution(queryId,
                    locationFactory.createQueryLocation(queryId),
                    importClientFactory,
                    importManager,
                    metadata,
                    strings.get(1),
                    strings.get(2),
                    strings.get(3),
                    query);
        }
        else {
            queryExecution = new SqlQueryExecution(queryId,
                    query,
                    new Session(),
                    metadata,
                    nodeManager,
                    splitManager,
                    stageManager,
                    remoteTaskFactory,
                    locationFactory,
                    queryEventFactory,
                    eventClient);
        }
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

    public void cancelAbandonedQueries()
    {
        DateTime oldestAllowedHeartBeat = DateTime.now().minus((long) clientTimeout.toMillis());
        for (QueryExecution queryExecution : queries.values()) {
            try {
                QueryInfo queryInfo = queryExecution.getQueryInfo();
                if (queryInfo.getState().isDone()) {
                    continue;
                }
                DateTime lastHeartBeat = queryInfo.getQueryStats().getLastHeartBeat();
                if (lastHeartBeat != null && lastHeartBeat.isBefore(oldestAllowedHeartBeat)) {
                    cancelQuery(queryExecution.getQueryId());
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
