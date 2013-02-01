/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskOutput;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

public class MockTaskManager
        implements TaskManager
{
    private final HttpServerInfo httpServerInfo;
    private final int pageBufferMax;
    private final int initialPages;

    private final ConcurrentMap<String, TaskOutput> tasks = new ConcurrentHashMap<>();

    @Inject
    public MockTaskManager(HttpServerInfo httpServerInfo)
    {
        this(httpServerInfo, 20, 12);
    }

    public MockTaskManager(HttpServerInfo httpServerInfo, int pageBufferMax, int initialPages)
    {
        Preconditions.checkNotNull(httpServerInfo, "httpServerInfo is null");
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");
        Preconditions.checkArgument(initialPages >= 0, "initialPages is negative");
        Preconditions.checkArgument(initialPages <= pageBufferMax, "initialPages is greater than pageBufferMax");
        this.httpServerInfo = httpServerInfo;
        this.pageBufferMax = pageBufferMax;
        this.initialPages = initialPages;
    }

    @Override
    public List<TaskInfo> getAllTaskInfo()
    {
        ImmutableList.Builder<TaskInfo> builder = ImmutableList.builder();
        for (TaskOutput taskOutput : tasks.values()) {
            builder.add(taskOutput.getTaskInfo());
        }
        return builder.build();
    }

    @Override
    public TaskInfo getTaskInfo(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskOutput taskOutput = tasks.get(taskId);
        if (taskOutput == null) {
            throw new NoSuchElementException();
        }
        return taskOutput.getTaskInfo();
    }

    @Override
    public TaskInfo createTask(Session session, String queryId,
            String stageId,
            String taskId,
            PlanFragment fragment,
            List<PlanFragmentSource> splits,
            Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources,
            List<String> outputIds)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkArgument(!taskId.isEmpty(), "taskId is empty");

        URI location = uriBuilderFrom(httpServerInfo.getHttpUri()).appendPath("v1/task").appendPath(taskId).build();
        TaskOutput taskOutput = new TaskOutput(queryId, stageId, taskId, location, ImmutableList.copyOf(outputIds), pageBufferMax, 0);
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

        return taskOutput.getTaskInfo();
    }

    @Override
    public List<Page> getTaskResults(String taskId, String outputId, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputId, "outputId is null");

        TaskOutput taskOutput = tasks.get(taskId);
        if (taskOutput == null) {
            throw new NoSuchElementException();
        }
        return taskOutput.getResults(outputId, maxPageCount, maxWaitTime);
    }

    @Override
    public void abortTaskResults(String taskId, String outputId)
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
    public void cancelTask(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskOutput taskOutput = tasks.get(taskId);
        if (taskOutput != null) {
            taskOutput.cancel();
        }
    }
}
