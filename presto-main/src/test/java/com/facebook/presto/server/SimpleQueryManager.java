/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SimpleQueryManager implements QueryManager
{
    private final SimpleQueryTaskManager simpleQueryTaskManager;
    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final ConcurrentMap<String, SimpleQuery> queries = new ConcurrentHashMap<>();

    @Inject
    public SimpleQueryManager(SimpleQueryTaskManager simpleQueryTaskManager)
    {
        Preconditions.checkNotNull(simpleQueryTaskManager, "simpleQueryTaskManager is null");
        this.simpleQueryTaskManager = simpleQueryTaskManager;
    }

    @Override
    public List<QueryInfo> getAllQueryInfo()
    {
        return ImmutableList.copyOf(filter(transform(queries.values(), new Function<SimpleQuery, QueryInfo>()
        {
            @Override
            public QueryInfo apply(SimpleQuery queryWorker)
            {
                try {
                    return queryWorker.getQueryInfo();
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

        SimpleQuery query = queries.get(queryId);
        if (query == null) {
            throw new NoSuchElementException();
        }
        return query.getQueryInfo();
    }

    @Override
    public QueryInfo createQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");

        String queryId = String.valueOf(nextQueryId.getAndIncrement());

        QueryTaskInfo outputTask = simpleQueryTaskManager.createQueryTask(null, ImmutableList.<String>of("out"), ImmutableMap.<String, List<PlanFragmentSource>>of());
        SimpleQuery simpleQuery = new SimpleQuery(queryId, outputTask.getTaskId(), simpleQueryTaskManager);
        queries.put(queryId, simpleQuery);
        return simpleQuery.getQueryInfo();
    }

    @Override
    public void cancelQuery(String queryId)
    {
        queries.remove(queryId);
    }

    private static class SimpleQuery
    {
        private final String queryId;
        private final String outputTaskId;
        private final SimpleQueryTaskManager simpleQueryTaskManager;

        private SimpleQuery(String queryId, String outputTaskId, SimpleQueryTaskManager simpleQueryTaskManager)
        {
            this.queryId = queryId;
            this.outputTaskId = outputTaskId;
            this.simpleQueryTaskManager = simpleQueryTaskManager;
        }

        private QueryInfo getQueryInfo()
        {
            QueryTaskInfo outputTask = simpleQueryTaskManager.getQueryTaskInfo(outputTaskId);

            return new QueryInfo(queryId,
                    outputTask.getTupleInfos(),
                    outputTask.getState(),
                    "out",
                    ImmutableMap.<String, List<QueryTaskInfo>>of("out", ImmutableList.of(outputTask)));
        }
    }
}
