/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

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
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlQueryManager
        implements QueryManager
{
    private static final Logger log = Logger.get(SqlQueryManager.class);

    private final TaskScheduler taskScheduler;
    private final ExecutorService queryExecutor;
    private final ImportClientFactory importClientFactory;
    private final ImportManager importManager;
    private final Metadata metadata;
    private final NodeManager nodeManager;
    private final SplitManager splitManager;

    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final ConcurrentMap<String, QueryExecution> queries = new ConcurrentHashMap<>();

    @Inject
    public SqlQueryManager(
            TaskScheduler taskScheduler,
            ImportClientFactory importClientFactory,
            ImportManager importManager,
            Metadata metadata,
            NodeManager nodeManager,
            SplitManager splitManager)
    {
        Preconditions.checkNotNull(taskScheduler, "taskScheduler is null");
        Preconditions.checkNotNull(importClientFactory, "importClientFactory is null");
        Preconditions.checkNotNull(importManager, "importManager is null");
        Preconditions.checkNotNull(metadata, "metadata is null");

        this.taskScheduler = taskScheduler;
        this.queryExecutor = new ThreadPoolExecutor(1000,
                1000,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(),
                threadsNamed("query-processor-%d"));

        this.importClientFactory = importClientFactory;
        this.importManager = importManager;
        this.metadata = metadata;
        this.nodeManager = nodeManager;
        this.splitManager = splitManager;
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
    public QueryInfo getQueryInfo(String queryId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");

        QueryExecution query = queries.get(queryId);
        if (query == null) {
            throw new NoSuchElementException();
        }
        try {
            return query.getQueryInfo();
        }
        catch (RuntimeException e) {
            // todo need better signal for a failed task
            queries.remove(queryId);
            throw e;
        }
    }

    @Override
    public QueryInfo createQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkArgument(query.length() > 0, "query must not be empty string");

        QueryExecution queryExecution;
        if (query.startsWith("sql:")) {
            // e.g.: sql:select count(*) from hivedba_query_stats
            String sql = query.substring("sql:".length());
            queryExecution = new SqlQueryExecution(String.valueOf(nextQueryId.getAndIncrement()),
                    sql,
                    taskScheduler,
                    new Session(),
                    metadata,
                    nodeManager,
                    splitManager);
        }
        else {
            // todo this is a hack until we have language support for import or create table as select
            ImmutableList<String> strings = ImmutableList.copyOf(Splitter.on(":").split(query));
            String queryBase = strings.get(0);

            switch (queryBase) {
                // e.g.: import-table:hive:default:hivedba_query_stats
                case "import-table":
                    queryExecution = new ImportTableExecution(String.valueOf(nextQueryId.getAndIncrement()),
                            importClientFactory,
                            importManager,
                            metadata,
                            strings.get(1),
                            strings.get(2),
                            strings.get(3));
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported query " + query);
            }
        }
        queries.put(queryExecution.getQueryId(), queryExecution);

        // start the query in the background
        queryExecutor.submit(new QueryStarter(queryExecution));

        return queryExecution.getQueryInfo();
    }

    @Override
    public void cancelQuery(String queryId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");

        log.debug("Cancel query %s", queryId);

        QueryExecution query = queries.remove(queryId);
        if (query != null) {
            query.cancel();
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
