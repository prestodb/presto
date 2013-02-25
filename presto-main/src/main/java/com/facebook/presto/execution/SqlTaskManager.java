/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.concurrent.FairBatchExecutor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Page;
import com.facebook.presto.server.ExchangeOperatorFactory;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SqlTaskManager
        implements TaskManager
{
    private static final Logger log = Logger.get(SqlTaskManager.class);

    private final int pageBufferMax;

    private final FairBatchExecutor shardExecutor;
    private final ScheduledExecutorService taskManagementExecutor;
    private final Metadata metadata;
    private final DataStreamProvider dataStreamProvider;
    private final ExchangeOperatorFactory exchangeOperatorFactory;
    private final HttpServerInfo httpServerInfo;
    private final DataSize maxOperatorMemoryUsage;
    private final Duration maxTaskAge;
    private final Duration clientTimeout;

    private final ConcurrentMap<String, TaskExecution> tasks = new ConcurrentHashMap<>();

    @Inject
    public SqlTaskManager(
            Metadata metadata,
            DataStreamProvider dataStreamProvider,
            ExchangeOperatorFactory exchangeOperatorFactory,
            HttpServerInfo httpServerInfo,
            QueryManagerConfig config)
    {
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        Preconditions.checkNotNull(exchangeOperatorFactory, "exchangeOperatorFactory is null");
        Preconditions.checkNotNull(httpServerInfo, "httpServerInfo is null");
        Preconditions.checkNotNull(config, "config is null");

        this.metadata = metadata;
        this.dataStreamProvider = dataStreamProvider;
        this.exchangeOperatorFactory = exchangeOperatorFactory;
        this.httpServerInfo = httpServerInfo;
        this.pageBufferMax = 20;
        this.maxOperatorMemoryUsage = config.getMaxOperatorMemoryUsage();
        // Just to be nice, allow tasks to live an extra 30 seconds so queries will be removed first
        this.maxTaskAge = new Duration(config.getMaxQueryAge().toMillis() + SECONDS.toMillis(30), MILLISECONDS);
        this.clientTimeout = config.getClientTimeout();

        shardExecutor = new FairBatchExecutor(config.getMaxShardProcessorThreads(), threadsNamed("shard-processor-%d"));

        taskManagementExecutor = Executors.newScheduledThreadPool(5, threadsNamed("task-management-%d"));
        taskManagementExecutor.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    removeOldTasks();
                }
                catch (Throwable e) {
                    log.warn(e, "Error removing old tasks");
                }
                try {
                    failAbandonedTasks();
                }
                catch (Throwable e) {
                    log.warn(e, "Error canceling abandoned tasks");
                }
            }
        }, 200, 200, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        shardExecutor.shutdown();
        taskManagementExecutor.shutdownNow();
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
        TaskInfo taskInfo = taskExecution.getTaskInfo();
        taskInfo.getStats().recordHeartBeat();
        return taskInfo;
    }

    @Override
    public TaskInfo createTask(Session session,
            String queryId,
            String stageId,
            String taskId,
            PlanFragment fragment,
            Map<PlanNodeId, Set<Split>> fixedSources,
            List<String> outputIds)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkArgument(!taskId.isEmpty(), "taskId is empty");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(outputIds, "outputIds is null");
        Preconditions.checkNotNull(fixedSources, "fixedSources is null");

        URI location = uriBuilderFrom(httpServerInfo.getHttpUri()).appendPath("v1/task").appendPath(taskId).build();

        SqlTaskExecution taskExecution = new SqlTaskExecution(session,
                queryId,
                stageId,
                taskId,
                location,
                fragment,
                fixedSources,
                outputIds,
                pageBufferMax,
                dataStreamProvider,
                exchangeOperatorFactory,
                metadata,
                shardExecutor,
                maxOperatorMemoryUsage
        );
        
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
    public void addSplit(String taskId, PlanNodeId sourceId, Split split)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(sourceId, "sourceId is null");
        Preconditions.checkNotNull(split, "split is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            throw new NoSuchElementException();
        }
        taskExecution.addSplit(sourceId, split);
    }

    @Override
    public void noMoreSplits(String taskId, PlanNodeId sourceId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(sourceId, "sourceId is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            throw new NoSuchElementException();
        }
        taskExecution.noMoreSplits(sourceId);
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
        log.debug("Aborting task %s output %s", taskId, outputId);
        taskExecution.abortResults(outputId);
    }

    @Override
    public void cancelTask(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution != null) {
            log.debug("Cancelling task %s", taskId);
            taskExecution.cancel();
        }
    }


    public void removeTask(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskExecution taskExecution = tasks.remove(taskId);
        if (taskExecution != null) {
            taskExecution.cancel();
        }
    }

    public void removeOldTasks()
    {
        DateTime oldestAllowedTask = DateTime.now().minus((long) maxTaskAge.toMillis());
        for (TaskExecution taskExecution : tasks.values()) {
            try {
                TaskInfo taskInfo = taskExecution.getTaskInfo();
                DateTime endTime = taskInfo.getStats().getEndTime();
                if (endTime != null && endTime.isBefore(oldestAllowedTask)) {
                    removeTask(taskExecution.getTaskId());
                }
            }
            catch (Exception e) {
                log.warn(e, "Error while inspecting age of task %s", taskExecution.getTaskId());
            }
        }
    }

    public void failAbandonedTasks()
    {
        DateTime now = DateTime.now();
        DateTime oldestAllowedHeartBeat = now.minus((long) clientTimeout.toMillis());
        for (TaskExecution taskExecution : tasks.values()) {
            try {
                TaskInfo taskInfo = taskExecution.getTaskInfo();
                if (taskInfo.getState().isDone()) {
                    continue;
                }
                DateTime lastHeartBeat = taskInfo.getStats().getLastHeartBeat();
                if (lastHeartBeat != null && lastHeartBeat.isBefore(oldestAllowedHeartBeat)) {
                    log.info("Failing abandoned task %s", taskExecution.getTaskId());
                    taskExecution.fail(new AbandonedException("Task " + taskInfo.getTaskId(), lastHeartBeat, now));
                }
            }
            catch (Exception e) {
                log.warn(e, "Error while inspecting age of task %s", taskExecution.getTaskId());
            }
        }
    }
}
