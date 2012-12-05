/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.tuple.TupleInfo;
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
    private static final ImmutableList<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_VARBINARY);
    private final int pageBufferMax;
    private final int initialPages;

    private final AtomicInteger nextTaskId = new AtomicInteger();
    private final ConcurrentMap<String, TaskOutput> tasks = new ConcurrentHashMap<>();

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
        for (TaskOutput queryTask : tasks.values()) {
            builder.add(queryTask.getQueryTaskInfo());
        }
        return builder.build();
    }

    @Override
    public QueryTaskInfo getQueryTaskInfo(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskOutput queryState = tasks.get(taskId);
        if (queryState == null) {
            throw new NoSuchElementException();
        }
        return queryState.getQueryTaskInfo();
    }

    @Override
    public QueryTaskInfo createQueryTask(PlanFragment fragment,
            List<PlanFragmentSource> splits,
            Map<String, ExchangePlanFragmentSource> exchangeSources, List<String> outputIds)
    {
        String taskId = String.valueOf(nextTaskId.getAndIncrement());
        TaskOutput taskOutput = new TaskOutput(taskId, ImmutableList.copyOf(outputIds), TUPLE_INFOS, pageBufferMax, 0);
        tasks.put(taskId, taskOutput);

        List<String> data = ImmutableList.of("apple", "banana", "cherry", "date");

        // load initial pages
        for (int i = 0; i < initialPages; i++) {
            try {
                taskOutput.addPage(new Page(createStringsBlock(Iterables.concat(Collections.nCopies(i + 1, data)))));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
        taskOutput.finish();

        return taskOutput.getQueryTaskInfo();
    }

    @Override
    public List<Page> getQueryTaskResults(String taskId, String outputId, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputId, "outputId is null");

        TaskOutput taskOutput = tasks.get(taskId);
        if (taskOutput == null) {
            throw new NoSuchElementException();
        }
        return taskOutput.getNextPages(outputId, maxPageCount, maxWaitTime);
    }

    @Override
    public void abortQueryTaskResults(String taskId, String outputId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputId, "outputId is null");

        TaskOutput taskOutput = tasks.get(taskId);
        if (taskOutput == null) {
            throw new NoSuchElementException();
        }
        taskOutput.abortResults(outputId);
    }

    @Override
    public void cancelQueryTask(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskOutput taskOutput = tasks.get(taskId);
        if (taskOutput != null) {
            taskOutput.cancel();
        }
    }
}
