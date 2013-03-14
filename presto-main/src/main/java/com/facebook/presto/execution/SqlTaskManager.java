/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Page;
import com.facebook.presto.server.ExchangeOperatorFactory;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.util.Threads.threadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SqlTaskManager
        implements TaskManager
{
    private static final Logger log = Logger.get(SqlTaskManager.class);

    private final int pageBufferMax;

    private final ExecutorService taskMasterExecutor;
    private final ListeningExecutorService shardExecutor;
    private final ScheduledExecutorService taskManagementExecutor;
    private final Metadata metadata;
    private final DataStreamProvider dataStreamProvider;
    private final ExchangeOperatorFactory exchangeOperatorFactory;
    private final HttpServerInfo httpServerInfo;
    private final DataSize maxOperatorMemoryUsage;
    private final Duration maxTaskAge;
    private final Duration clientTimeout;

    private final ConcurrentMap<String, TaskInfo> taskInfos = new ConcurrentHashMap<>();
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
        this.pageBufferMax = config.getSinkMaxBufferedPages() == null ? config.getMaxShardProcessorThreads() * 5 : config.getSinkMaxBufferedPages();
        this.maxOperatorMemoryUsage = config.getMaxOperatorMemoryUsage();
        // Just to be nice, allow tasks to live an extra 30 seconds so queries will be removed first
        this.maxTaskAge = new Duration(config.getMaxQueryAge().toMillis() + SECONDS.toMillis(30), MILLISECONDS);
        this.clientTimeout = config.getClientTimeout();

        // we have an unlimited number of task master threads
        taskMasterExecutor = Executors.newCachedThreadPool(threadsNamed("task-processor-%d"));

        shardExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(config.getMaxShardProcessorThreads(), threadsNamed("shard-processor-%d")));

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
        taskMasterExecutor.shutdownNow();
        shardExecutor.shutdownNow();
        taskManagementExecutor.shutdownNow();
    }

    @Override
    public List<TaskInfo> getAllTaskInfo()
    {
        Map<String, TaskInfo> taskInfos = new TreeMap<>();
        taskInfos.putAll(taskInfos);
        for (TaskExecution taskExecution : tasks.values()) {
            taskInfos.put(taskExecution.getTaskId(), taskExecution.getTaskInfo());
        }
        return ImmutableList.copyOf(taskInfos.values());
    }

    @Override
    public TaskInfo getTaskInfo(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution != null) {
            TaskInfo taskInfo = taskExecution.getTaskInfo();
            taskInfo.getStats().recordHeartBeat();
            return taskInfo;
        }

        TaskInfo taskInfo = taskInfos.get(taskId);
        if (taskInfo == null) {
            throw new NoSuchElementException("Unknown query task " + taskId);
        }
        return taskInfo;
    }

    @Override
    public TaskInfo createTask(Session session,
            String queryId,
            String stageId,
            String taskId,
            PlanFragment fragment,
            Map<PlanNodeId, Set<Split>> initialSources,
            List<String> initialOutputIds)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkArgument(!taskId.isEmpty(), "taskId is empty");
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(initialOutputIds, "initialOutputIds is null");
        Preconditions.checkNotNull(initialSources, "initialSources is null");

        URI location = uriBuilderFrom(httpServerInfo.getHttpUri()).appendPath("v1/task").appendPath(taskId).build();

        SqlTaskExecution taskExecution = SqlTaskExecution.createSqlTaskExecution(session,
                queryId,
                stageId,
                taskId,
                location,
                fragment,
                initialSources,
                initialOutputIds,
                pageBufferMax,
                dataStreamProvider,
                exchangeOperatorFactory,
                metadata,
                taskMasterExecutor,
                shardExecutor,
                maxOperatorMemoryUsage
        );

        tasks.put(taskId, taskExecution);
        return taskExecution.getTaskInfo();
    }

    @Override
    public TaskInfo addResultQueue(String taskId, String outputName)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputName, "outputName is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            TaskInfo taskInfo = taskInfos.get(taskId);
            if (taskInfo != null) {
                // todo this is not safe since task can be expired at any time
                // task was finished early, so the new split should be ignored
                return taskInfo;
            } else {
                throw new NoSuchElementException("Unknown query task " + taskId);
            }
        }
        taskExecution.addResultQueue(outputName);
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
    public TaskInfo noMoreResultQueues(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            TaskInfo taskInfo = taskInfos.get(taskId);
            if (taskInfo != null) {
                // todo this is not safe since task can be expired at any time
                // task was finished early, so the new split should be ignored
                return taskInfo;
            } else {
                throw new NoSuchElementException("Unknown query task " + taskId);
            }
        }
        taskExecution.noMoreResultQueues();
        return taskExecution.getTaskInfo();
    }

    @Override
    public TaskInfo addSplit(String taskId, PlanNodeId sourceId, Split split)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(sourceId, "sourceId is null");
        Preconditions.checkNotNull(split, "split is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            TaskInfo taskInfo = taskInfos.get(taskId);
            if (taskInfo != null) {
                // todo this is not safe since task can be expired at any time
                // task was finished early, so the new split should be ignored
                return taskInfo;
            } else {
                throw new NoSuchElementException("Unknown query task " + taskId);
            }
        }
        taskExecution.addSplit(sourceId, split);
        return taskExecution.getTaskInfo();
    }

    @Override
    public TaskInfo noMoreSplits(String taskId, PlanNodeId sourceId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(sourceId, "sourceId is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            TaskInfo taskInfo = taskInfos.get(taskId);
            if (taskInfo != null) {
                // todo this is not safe since task can be expired at any time
                // task was finished early, so the new split should be ignored
                return taskInfo;
            } else {
                throw new NoSuchElementException("Unknown query task " + taskId);
            }
        }
        taskExecution.noMoreSplits(sourceId);
        return taskExecution.getTaskInfo();
    }

    @Override
    public TaskInfo abortTaskResults(String taskId, String outputId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputId, "outputId is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            TaskInfo taskInfo = taskInfos.get(taskId);
            if (taskInfo != null) {
                // todo this is not safe since task can be expired at any time
                // task was finished early, so the new split should be ignored
                return taskInfo;
            } else {
                throw new NoSuchElementException("Unknown query task " + taskId);
            }
        }
        log.debug("Aborting task %s output %s", taskId, outputId);
        taskExecution.abortResults(outputId);

        // assure task is completed and cache final results
        cancelTask(taskId);
        return taskExecution.getTaskInfo();
    }

    @Override
    public TaskInfo cancelTask(String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskExecution taskExecution = tasks.remove(taskId);
        if (taskExecution == null) {
            return taskInfos.get(taskId);
        }

        // make sure task is finished
        taskExecution.cancel();
        tasks.remove(taskId);

        // cache task info
        TaskInfo taskInfo = taskExecution.getTaskInfo();
        taskInfos.putIfAbsent(taskId, taskInfo);
        return taskExecution.getTaskInfo();
    }

    public void removeOldTasks()
    {
        DateTime oldestAllowedTask = DateTime.now().minus((long) maxTaskAge.toMillis());
        for (TaskExecution taskExecution : tasks.values()) {
            try {
                TaskInfo taskInfo = taskExecution.getTaskInfo();

                // drop references to completed task objects
                if (taskInfo.getState().isDone()) {
                    // assure task is completed and cache final results
                    cancelTask(taskExecution.getTaskId());
                }

                DateTime endTime = taskInfo.getStats().getEndTime();
                if (endTime != null && endTime.isBefore(oldestAllowedTask)) {
                    taskInfos.remove(taskExecution.getTaskId());
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
