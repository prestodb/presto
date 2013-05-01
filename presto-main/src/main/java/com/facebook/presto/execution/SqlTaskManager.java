/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.TaskSource;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.ExecutionStats.ExecutionStatsSnapshot;
import com.facebook.presto.execution.SharedBuffer.QueueState;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.operator.OperatorStats.SplitExecutionStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
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
    private final LocalStorageManager storageManager;
    private final DataStreamProvider dataStreamProvider;
    private final ExchangeOperatorFactory exchangeOperatorFactory;
    private final NodeInfo nodeInfo;
    private final LocationFactory locationFactory;
    private final QueryMonitor queryMonitor;
    private final DataSize maxOperatorMemoryUsage;
    private final Duration maxTaskAge;
    private final Duration clientTimeout;

    private final ConcurrentMap<TaskId, TaskInfo> taskInfos = new ConcurrentHashMap<>();
    private final ConcurrentMap<TaskId, TaskExecution> tasks = new ConcurrentHashMap<>();

    @Inject
    public SqlTaskManager(
            Metadata metadata,
            LocalStorageManager storageManager,
            DataStreamProvider dataStreamProvider,
            ExchangeOperatorFactory exchangeOperatorFactory,
            NodeInfo nodeInfo,
            LocationFactory locationFactory,
            QueryMonitor queryMonitor,
            QueryManagerConfig config)
    {
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(storageManager, "storageManager is null");
        Preconditions.checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        Preconditions.checkNotNull(exchangeOperatorFactory, "exchangeOperatorFactory is null");
        Preconditions.checkNotNull(nodeInfo, "nodeInfo is null");
        Preconditions.checkNotNull(locationFactory, "locationFactory is null");
        Preconditions.checkNotNull(queryMonitor, "queryMonitor is null");
        Preconditions.checkNotNull(config, "config is null");

        this.metadata = metadata;
        this.storageManager = storageManager;
        this.dataStreamProvider = dataStreamProvider;
        this.exchangeOperatorFactory = exchangeOperatorFactory;
        this.nodeInfo = nodeInfo;
        this.locationFactory = locationFactory;
        this.queryMonitor = queryMonitor;
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
    public List<TaskInfo> getAllTaskInfo(boolean full)
    {
        Map<TaskId, TaskInfo> taskInfos = new TreeMap<>();
        taskInfos.putAll(taskInfos);
        for (TaskExecution taskExecution : tasks.values()) {
            taskInfos.put(taskExecution.getTaskId(), taskExecution.getTaskInfo(full));
        }
        return ImmutableList.copyOf(taskInfos.values());
    }

    @Override
    public void waitForStateChange(TaskId taskId, TaskState currentState, Duration maxWait)
            throws InterruptedException
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(maxWait, "maxWait is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            return;
        }

        taskExecution.recordHeartBeat();
        taskExecution.waitForStateChange(currentState, maxWait);
    }

    @Override
    public TaskInfo getTaskInfo(TaskId taskId, boolean full)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution != null) {
            taskExecution.recordHeartBeat();
            return taskExecution.getTaskInfo(full);
        }

        TaskInfo taskInfo = taskInfos.get(taskId);
        if (taskInfo == null) {
            throw new NoSuchElementException("Unknown query task " + taskId);
        }
        return taskInfo;
    }

    @Override
    public TaskInfo updateTask(Session session, TaskId taskId, PlanFragment fragment, List<TaskSource> sources, OutputBuffers outputIds)
    {
        URI location = locationFactory.createLocalTaskLocation(taskId);

        TaskExecution taskExecution;
        synchronized (this) {
            taskExecution = tasks.get(taskId);
            if (taskExecution == null) {
                // is task already complete?
                TaskInfo taskInfo = taskInfos.get(taskId);
                if (taskInfo != null) {
                    return taskInfo;
                }

                taskExecution = SqlTaskExecution.createSqlTaskExecution(session,
                        nodeInfo,
                        taskId,
                        location,
                        fragment,
                        pageBufferMax,
                        dataStreamProvider,
                        exchangeOperatorFactory,
                        metadata,
                        storageManager,
                        taskMasterExecutor,
                        shardExecutor,
                        maxOperatorMemoryUsage,
                        queryMonitor
                );
                tasks.put(taskId, taskExecution);
            }
        }

        taskExecution.recordHeartBeat();
        taskExecution.addSources(sources);
        taskExecution.addResultQueue(outputIds);

        return taskExecution.getTaskInfo(false);
    }

    @Override
    public BufferResult<Page> getTaskResults(TaskId taskId, String outputName, int maxPageCount, Duration maxWaitTime)
            throws InterruptedException
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(outputName, "outputName is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            throw new NoSuchElementException("Unknown query task " + taskId);
        }
        taskExecution.recordHeartBeat();
        return taskExecution.getResults(outputName, maxPageCount, maxWaitTime);
    }

    @Override
    public TaskInfo abortTaskResults(TaskId taskId, String outputId)
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

        return taskExecution.getTaskInfo(false);
    }

    @Override
    public TaskInfo cancelTask(TaskId taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");

        TaskExecution taskExecution = tasks.get(taskId);
        if (taskExecution == null) {
            TaskInfo taskInfo = taskInfos.get(taskId);
            if (taskInfo == null) {
                // task does not exist yet, mark the task as canceled, so later if a late request
                // comes in to create the task, the task remains canceled
                taskInfo = new TaskInfo(taskId,
                        Long.MAX_VALUE,
                        TaskState.CANCELED,
                        URI.create("unknown"),
                        new SharedBufferInfo(QueueState.FINISHED, 0, ImmutableList.<BufferInfo>of()),
                        ImmutableSet.<PlanNodeId>of(),
                        new ExecutionStatsSnapshot(),
                        ImmutableList.<SplitExecutionStats>of(),
                        ImmutableList.<FailureInfo>of(),
                        ImmutableMap.<PlanNodeId, Set<?>>of());
                TaskInfo existingTaskInfo = taskInfos.putIfAbsent(taskId, taskInfo);
                if (existingTaskInfo != null) {
                    taskInfo = existingTaskInfo;
                }
            }
            return taskInfo;
        }

        // make sure task is finished
        taskExecution.cancel();

        // cache task info
        TaskInfo taskInfo = taskExecution.getTaskInfo(false);
        taskInfos.putIfAbsent(taskId, taskInfo);

        // remove task (after caching the task info)
        tasks.remove(taskId);

        return taskInfo;
    }

    public void removeOldTasks()
    {
        DateTime oldestAllowedTask = DateTime.now().minus((long) maxTaskAge.toMillis());
        for (TaskExecution taskExecution : tasks.values()) {
            try {
                TaskInfo taskInfo = taskExecution.getTaskInfo(false);

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
                TaskInfo taskInfo = taskExecution.getTaskInfo(false);
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
