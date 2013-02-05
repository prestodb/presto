/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.FailureInfo;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class MockQueryManager
        implements QueryManager
{
    public static final List<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_VARBINARY);

    private final MockTaskManager mockTaskManager;
    private final LocationFactory locationFactory;
    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final ConcurrentMap<String, SimpleQuery> queries = new ConcurrentHashMap<>();

    @Inject
    public MockQueryManager(MockTaskManager mockTaskManager, LocationFactory locationFactory)
    {
        Preconditions.checkNotNull(mockTaskManager, "mockTaskManager is null");
        Preconditions.checkNotNull(locationFactory, "locationFactory is null");
        this.mockTaskManager = mockTaskManager;
        this.locationFactory = locationFactory;
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
    public QueryInfo getQueryInfo(String queryId, boolean forceRefresh)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");

        SimpleQuery query = queries.get(queryId);
        if (query == null) {
            throw new NoSuchElementException();
        }
        return query.getQueryInfo();
    }

    @Override
    public QueryInfo createQuery(Session session, String query)
    {
        Preconditions.checkNotNull(query, "query is null");

        String queryId = String.valueOf(nextQueryId.getAndIncrement());

        TaskInfo outputTask = mockTaskManager.createTask(session,
                "queryId",
                "stageId",
                "queryId",
                null,
                ImmutableMap.<PlanNodeId, ExchangePlanFragmentSource>of(),
                ImmutableList.<String>of("out")
        );

        SimpleQuery simpleQuery = new SimpleQuery(queryId, locationFactory.createQueryLocation(queryId), outputTask.getTaskId(), mockTaskManager, locationFactory);
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
        private final URI self;
        private final String outputTaskId;
        private final MockTaskManager mockTaskManager;
        private final LocationFactory locationFactory;

        private SimpleQuery(String queryId, URI self, String outputTaskId, MockTaskManager mockTaskManager, LocationFactory locationFactory)
        {
            this.queryId = queryId;
            this.self = self;
            this.outputTaskId = outputTaskId;
            this.mockTaskManager = mockTaskManager;
            this.locationFactory = locationFactory;
        }

        private QueryInfo getQueryInfo()
        {
            TaskInfo outputTask = mockTaskManager.getTaskInfo(outputTaskId);

            QueryState state;
            switch (outputTask.getState()) {
                case PLANNED:
                case QUEUED:
                case RUNNING:
                    state = QueryState.RUNNING;
                    break;
                case FINISHED:
                    state = QueryState.FINISHED;
                    break;
                case CANCELED:
                    state = QueryState.CANCELED;
                    break;
                case FAILED:
                    state = QueryState.FAILED;
                    break;
                default:
                    throw new IllegalStateException("Unknown task state " + outputTask.getState());
            }
            String stageId = queryId + "-0";
            return new QueryInfo(queryId,
                    new Session(null, "test_catalog", "test_schema"),
                    state,
                    self,
                    ImmutableList.of("out"),
                    "query",
                    new QueryStats(),
                    new StageInfo(queryId,
                            stageId,
                            StageState.FINISHED,
                            locationFactory.createStageLocation(stageId),
                            null,
                            TUPLE_INFOS,
                            ImmutableList.<TaskInfo>of(outputTask),
                            ImmutableList.<StageInfo>of(),
                            ImmutableList.<FailureInfo>of()),
                    ImmutableList.<FailureInfo>of());
        }
    }
}
