/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.concurrent.FairBatchExecutor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.PlanFragmentSourceProvider;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

public class SqlTaskManager
        implements TaskManager
{
    private final int pageBufferMax;

    private final ExecutorService taskExecutor;
    private final FairBatchExecutor shardExecutor;
    private final Metadata metadata;
    private final PlanFragmentSourceProvider sourceProvider;
    private final HttpServerInfo httpServerInfo;

    private final ConcurrentMap<String, TaskExecution> tasks = new ConcurrentHashMap<>();

    @Inject
    public SqlTaskManager(
            Metadata metadata,
            PlanFragmentSourceProvider sourceProvider,
            HttpServerInfo httpServerInfo)
    {
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(sourceProvider, "sourceProvider is null");
        Preconditions.checkNotNull(httpServerInfo, "httpServerInfo is null");

        this.metadata = metadata;
        this.sourceProvider = sourceProvider;
        this.httpServerInfo = httpServerInfo;
        this.pageBufferMax = 20;

        int processors = Runtime.getRuntime().availableProcessors();
        taskExecutor = new ThreadPoolExecutor(1000,
                1000,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(),
                threadsNamed("task-processor-%d"));

        shardExecutor = new FairBatchExecutor(8 * processors, threadsNamed("shard-processor-%d"));
    }

    @Override
    public List<TaskInfo> getAllTaskInfo()
    {
        return ImmutableList.copyOf(filter(transform(tasks.values(), new Function<TaskExecution, TaskInfo>()
        {
            @Override
            public TaskInfo apply(TaskExecution taskExecution)
            {
                try {
                    return taskExecution.getTaskInfo();
                }
                catch (Exception ignored) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }

    @Override
    public TaskInfo getTaskInfo(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            throw new NoSuchElementException("Unknown query task " + taskId);
        }
        return taskExecution.getTaskInfo();
    }

    @Override
    public TaskInfo createTask(String queryId,
            String stageId,
            String taskId,
            PlanFragment fragment,
            List<PlanFragmentSource> splits,
            Map<String, ExchangePlanFragmentSource> exchangeSources,
            List<String> outputIds)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkArgument(!taskId.isEmpty(), "taskId is empty");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(outputIds, "outputIds is null");
        Preconditions.checkNotNull(splits, "splits is null");
        Preconditions.checkNotNull(exchangeSources, "exchangeSources is null");

        URI location = uriBuilderFrom(httpServerInfo.getHttpUri()).appendPath("v1/task").appendPath(taskId).build();

        SqlTaskExecution taskExecution = new SqlTaskExecution(queryId, stageId, taskId, location, fragment, splits, exchangeSources,  outputIds, pageBufferMax, sourceProvider, metadata, shardExecutor);
        taskExecutor.submit(new TaskStarter(taskExecution));

        tasks.put(taskId, taskExecution);
        return taskExecution.getTaskInfo();
    }

    @Override
    public List<Page> getTaskResults(String taskId, String outputName, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputName, "outputName is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            throw new NoSuchElementException("Unknown query task " + taskId);
        }
        return taskExecution.getResults(outputName, maxPageCount, maxWaitTime);
    }

    @Override
    public void abortTaskResults(String taskId, String outputId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputId, "outputId is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            throw new NoSuchElementException();
        }
        taskExecution.abortResults(outputId);
    }

    @Override
    public void cancelTask(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskExecution taskExecution = tasks.remove(taskId);
        if (taskExecution != null) {
            taskExecution.cancel();
        }
    }

    private static class TaskStarter
            implements Runnable
    {
        private final TaskExecution taskExecution;

        public TaskStarter(TaskExecution taskExecution)
        {
            this.taskExecution = taskExecution;
        }

        @Override
        public void run()
        {
            taskExecution.run();
        }
    }
}
