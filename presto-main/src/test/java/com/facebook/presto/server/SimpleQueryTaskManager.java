/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

public class SimpleQueryTaskManager
        implements QueryTaskManager
{
    private final int pageBufferMax;
    private final int initialPages;

    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final ConcurrentMap<String, SimpleQueryTask> queries = new ConcurrentHashMap<>();

    @Inject
    public SimpleQueryTaskManager()
    {
        this(20, 12);
    }

    public SimpleQueryTaskManager(int pageBufferMax, int initialPages)
    {
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");
        Preconditions.checkArgument(initialPages >= 0, "initialPages is negative");
        Preconditions.checkArgument(initialPages <= pageBufferMax, "initialPages is greater than pageBufferMax");
        this.pageBufferMax = pageBufferMax;
        this.initialPages = initialPages;
    }

    @Override
    public List<QueryTaskInfo> getAllQueryTaskInfo()
    {
        ImmutableList.Builder<QueryTaskInfo> builder = ImmutableList.builder();
        for (QueryTask queryTask : queries.values()) {
            builder.add(queryTask.getQueryTaskInfo());
        }
        return builder.build();
    }

    @Override
    public QueryTaskInfo getQueryTaskInfo(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        QueryTask queryState = queries.get(taskId);
        if (queryState == null) {
            throw new NoSuchElementException();
        }
        return queryState.getQueryTaskInfo();
    }

    @Override
    public QueryTask createQueryTask(PlanFragment planFragment, List<String> outputIds, Map<String, List<PlanFragmentSource>> fragmentSources)
    {
        // todo add output id suport
        String queryId = String.valueOf(nextQueryId.getAndIncrement());
        QueryState queryState = new QueryState(ImmutableList.of(SINGLE_VARBINARY), 1, pageBufferMax);
        SimpleQueryTask queryTask = new SimpleQueryTask(queryId, queryState);
        queries.put(queryId, queryTask);

        List<String> data = ImmutableList.of("apple", "banana", "cherry", "date");

        // load initial pages
        for (int i = 0; i < initialPages; i++) {
            try {
                queryState.addPage(new Page(createStringsBlock(Iterables.concat(Collections.nCopies(i + 1, data)))));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
        queryState.sourceFinished();

        return queryTask;
    }

    @Override
    public List<Page> getQueryTaskResults(String taskId, String outputName, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputName, "outputName is null");

        SimpleQueryTask queryTask = queries.get(taskId);
        if (queryTask == null) {
            throw new NoSuchElementException();
        }
        return queryTask.getNextPages(maxPageCount, maxWaitTime);
    }

    @Override
    public void cancelQueryTask(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        QueryTask queryTask = queries.remove(taskId);
        if (queryTask != null) {
            queryTask.cancel();
        }
    }

    private static class SimpleQueryTask
    implements  QueryTask{

        private final String taskId;
        private final QueryState queryState;

        private SimpleQueryTask(String taskId, QueryState queryState)
        {
            this.taskId = taskId;
            this.queryState = queryState;
        }

        @Override
        public String getTaskId()
        {
            return taskId;
        }

        @Override
        public QueryTaskInfo getQueryTaskInfo()
        {
            return queryState.toQueryTaskInfo(taskId);
        }

        public List<Page> getNextPages(int maxPageCount, Duration maxWait)
                throws InterruptedException
        {
            return queryState.getNextPages(maxPageCount, maxWait);
        }

        @Override
        public void cancel()
        {
            queryState.cancel();
        }
    }
}
