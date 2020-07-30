/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.concurrent.ConcurrentScheduledExecutor;
import com.facebook.airlift.concurrent.ThreadPoolExecutorMBean;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.GcMonitor;
import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.event.SplitMonitor;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.MemoryPoolAssignment;
import com.facebook.presto.memory.MemoryPoolAssignmentsRequest;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spiller.LocalSpillManager;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxBroadcastMemory;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxMemoryPerNode;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxTotalMemoryPerNode;
import static com.facebook.presto.SystemSessionProperties.resourceOvercommit;
import static com.facebook.presto.execution.SqlTask.createSqlTask;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_TASK;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class SqlTaskManager
        implements TaskManager, Closeable
{
    private static final Logger log = Logger.get(SqlTaskManager.class);

    private final ExecutorService taskNotificationExecutor;
    private final ThreadPoolExecutorMBean taskNotificationExecutorMBean;

    private final ScheduledExecutorService taskManagementExecutor;
    private final ScheduledExecutorService driverYieldExecutor;

    private final Duration infoCacheTime;
    private final Duration clientTimeout;

    private final LocalMemoryManager localMemoryManager;
    private final LoadingCache<QueryId, QueryContext> queryContexts;
    private final LoadingCache<TaskId, SqlTask> tasks;

    private final SqlTaskIoStats cachedStats = new SqlTaskIoStats();
    private final SqlTaskIoStats finishedTaskStats = new SqlTaskIoStats();

    @GuardedBy("this")
    private long currentMemoryPoolAssignmentVersion;
    @GuardedBy("this")
    private String coordinatorId;

    private final CounterStat failedTasks = new CounterStat();

    private final LoadingCache<String, Map<TaskId, TaskState>> sessionTaskIdStatusMap;
    private final Executor statusBoundedExecutor;
    private final Duration waitTime;

    private final ScheduledExecutorService timeoutExecutor;

    @Inject
    public SqlTaskManager(
            LocalExecutionPlanner planner,
            LocationFactory locationFactory,
            TaskExecutor taskExecutor,
            SplitMonitor splitMonitor,
            NodeInfo nodeInfo,
            LocalMemoryManager localMemoryManager,
            TaskManagementExecutor taskManagementExecutor,
            TaskManagerConfig config,
            NodeMemoryConfig nodeMemoryConfig,
            LocalSpillManager localSpillManager,
            ExchangeClientSupplier exchangeClientSupplier,
            NodeSpillConfig nodeSpillConfig,
            GcMonitor gcMonitor,
            BlockEncodingSerde blockEncodingSerde,
            OrderingCompiler orderingCompiler)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(config, "config is null");
        infoCacheTime = config.getInfoMaxAge();
        clientTimeout = config.getClientTimeout();

        DataSize maxBufferSize = config.getSinkMaxBufferSize();

        taskNotificationExecutor = newFixedThreadPool(config.getTaskNotificationThreads(), threadsNamed("task-notification-%s"));
        taskNotificationExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) taskNotificationExecutor);

        this.taskManagementExecutor = requireNonNull(taskManagementExecutor, "taskManagementExecutor cannot be null").getExecutor();
        this.driverYieldExecutor = newScheduledThreadPool(config.getTaskYieldThreads(), threadsNamed("task-yield-%s"));

        SqlTaskExecutionFactory sqlTaskExecutionFactory = new SqlTaskExecutionFactory(
                taskNotificationExecutor,
                taskExecutor,
                planner,
                blockEncodingSerde,
                orderingCompiler,
                splitMonitor,
                config);

        this.localMemoryManager = requireNonNull(localMemoryManager, "localMemoryManager is null");
        DataSize maxQueryUserMemoryPerNode = nodeMemoryConfig.getMaxQueryMemoryPerNode();
        DataSize maxQueryTotalMemoryPerNode = nodeMemoryConfig.getMaxQueryTotalMemoryPerNode();
        DataSize maxQuerySpillPerNode = nodeSpillConfig.getQueryMaxSpillPerNode();
        DataSize maxQueryBroadcastMemory = nodeMemoryConfig.getMaxQueryBroadcastMemory();

        queryContexts = CacheBuilder.newBuilder().weakValues().build(CacheLoader.from(
                queryId -> createQueryContext(queryId, localMemoryManager, localSpillManager, gcMonitor, maxQueryUserMemoryPerNode, maxQueryTotalMemoryPerNode, maxQuerySpillPerNode, maxQueryBroadcastMemory)));

        tasks = CacheBuilder.newBuilder().build(CacheLoader.from(
                taskId -> createSqlTask(
                        taskId,
                        locationFactory.createLocalTaskLocation(taskId),
                        nodeInfo.getNodeId(),
                        queryContexts.getUnchecked(taskId.getQueryId()),
                        sqlTaskExecutionFactory,
                        exchangeClientSupplier,
                        taskNotificationExecutor,
                        sqlTask -> {
                            finishedTaskStats.merge(sqlTask.getIoStats());
                            return null;
                        },
                        maxBufferSize,
                        failedTasks)));

        sessionTaskIdStatusMap = CacheBuilder.newBuilder().build(CacheLoader.from(sessionId -> new ConcurrentHashMap<>()));
        statusBoundedExecutor = new BoundedExecutor(newCachedThreadPool(daemonThreadsNamed("task-status-endpoint-%s")), 100);
        waitTime = new Duration(1, TimeUnit.SECONDS);
        timeoutExecutor = new ConcurrentScheduledExecutor(10, 1, "task-status-endpoint-timeout");
    }

    private QueryContext createQueryContext(
            QueryId queryId,
            LocalMemoryManager localMemoryManager,
            LocalSpillManager localSpillManager,
            GcMonitor gcMonitor,
            DataSize maxQueryUserMemoryPerNode,
            DataSize maxQueryTotalMemoryPerNode,
            DataSize maxQuerySpillPerNode,
            DataSize maxQueryBroadcastMemory)
    {
        return new QueryContext(
                queryId,
                maxQueryUserMemoryPerNode,
                maxQueryTotalMemoryPerNode,
                maxQueryBroadcastMemory,
                localMemoryManager.getGeneralPool(),
                gcMonitor,
                taskNotificationExecutor,
                driverYieldExecutor,
                maxQuerySpillPerNode,
                localSpillManager.getSpillSpaceTracker());
    }

    @Override
    public synchronized void updateMemoryPoolAssignments(MemoryPoolAssignmentsRequest assignments)
    {
        if (coordinatorId != null && coordinatorId.equals(assignments.getCoordinatorId()) && assignments.getVersion() <= currentMemoryPoolAssignmentVersion) {
            return;
        }
        currentMemoryPoolAssignmentVersion = assignments.getVersion();
        if (coordinatorId != null && !coordinatorId.equals(assignments.getCoordinatorId())) {
            log.warn("Switching coordinator affinity from " + coordinatorId + " to " + assignments.getCoordinatorId());
        }
        coordinatorId = assignments.getCoordinatorId();

        for (MemoryPoolAssignment assignment : assignments.getAssignments()) {
            if (assignment.getPoolId().equals(GENERAL_POOL)) {
                queryContexts.getUnchecked(assignment.getQueryId()).setMemoryPool(localMemoryManager.getGeneralPool());
            }
            else if (assignment.getPoolId().equals(RESERVED_POOL)) {
                MemoryPool reservedPool = localMemoryManager.getReservedPool()
                        .orElseThrow(() -> new IllegalArgumentException(format("Cannot move %s to the reserved pool as the reserved pool is not enabled", assignment.getQueryId())));
                queryContexts.getUnchecked(assignment.getQueryId()).setMemoryPool(reservedPool);
            }
            else {
                throw new IllegalArgumentException(format("Cannot move %s to %s as the target memory pool id is invalid", assignment.getQueryId(), assignment.getPoolId()));
            }
        }
    }

    @PostConstruct
    public void start()
    {
        taskManagementExecutor.scheduleWithFixedDelay(() -> {
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
        }, 200, 200, TimeUnit.MILLISECONDS);

        taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                updateStats();
            }
            catch (Throwable e) {
                log.warn(e, "Error updating stats");
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    @PreDestroy
    public void close()
    {
        boolean taskCanceled = false;
        for (SqlTask task : tasks.asMap().values()) {
            if (task.getTaskStatus().getState().isDone()) {
                continue;
            }
            task.failed(new PrestoException(SERVER_SHUTTING_DOWN, format("Server is shutting down. Task %s has been canceled", task.getTaskId())));
            taskCanceled = true;
        }
        if (taskCanceled) {
            try {
                TimeUnit.SECONDS.sleep(5);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        taskNotificationExecutor.shutdownNow();
    }

    @Managed
    @Flatten
    public SqlTaskIoStats getIoStats()
    {
        return cachedStats;
    }

    @Managed(description = "Task notification executor")
    @Nested
    public ThreadPoolExecutorMBean getTaskNotificationExecutor()
    {
        return taskNotificationExecutorMBean;
    }

    @Managed(description = "Failed tasks counter")
    @Nested
    public CounterStat getFailedTasks()
    {
        return failedTasks;
    }

    public List<SqlTask> getAllTasks()
    {
        return ImmutableList.copyOf(tasks.asMap().values());
    }

    @Override
    public List<TaskInfo> getAllTaskInfo()
    {
        return ImmutableList.copyOf(transform(tasks.asMap().values(), SqlTask::getTaskInfo));
    }

    @Override
    public TaskInfo getTaskInfo(TaskId taskId)
    {
        requireNonNull(taskId, "taskId is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInfo();
    }

    @Override
    public Map<TaskId, TaskStatus> getAllTaskStatus(String sessionId)
    {
        Map<TaskId, TaskState> lastTaskIdStatusMap = sessionTaskIdStatusMap.getUnchecked(sessionId);
        List<ListenableFuture<TaskStatus>> listenableFutures = new ArrayList<>();
        Map<TaskId, TaskStatus> finalTaskIdTaskStatusMap = new ConcurrentHashMap<>();
        Set<TaskId> taskIds = tasks.asMap().keySet();
        for (TaskId taskId : taskIds) {
            statusBoundedExecutor.execute(() -> {
                TaskState lastTaskState = lastTaskIdStatusMap.getOrDefault(taskId, null);
                if (lastTaskState == null) {
                    TaskStatus taskStatus = getTaskStatus(taskId);
                    finalTaskIdTaskStatusMap.put(taskId, taskStatus);
                }
                else {
                    ListenableFuture<TaskStatus> listenableTaskStateFuture = addTimeout(
                            getTaskStatus(taskId, lastTaskState),
                            () -> getTaskStatus(taskId),
                            waitTime,
                            timeoutExecutor);
                    Futures.addCallback(listenableTaskStateFuture, new FutureCallback<TaskStatus>() {
                        @Override
                        public void onSuccess(@Nullable TaskStatus taskStatus)
                        {
                            finalTaskIdTaskStatusMap.put(taskId, taskStatus);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            log.error("Failed to get t");
                        }
                    }, directExecutor());
                    listenableFutures.add(listenableTaskStateFuture);
                }
            });
        }

        try {
            Futures.allAsList(listenableFutures).get();
        }
        catch (Exception e) {
            log.error("Exception in getAllTaskStatus: " + e);
        }

        return finalTaskIdTaskStatusMap;
    }

    @Override
    public TaskStatus getTaskStatus(TaskId taskId)
    {
        requireNonNull(taskId, "taskId is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskStatus();
    }

    @Override
    public ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, TaskState currentState)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(currentState, "currentState is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInfo(currentState);
    }

    @Override
    public String getTaskInstanceId(TaskId taskId)
    {
        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInstanceId();
    }

    @Override
    public ListenableFuture<TaskStatus> getTaskStatus(TaskId taskId, TaskState currentState)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(currentState, "currentState is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskStatus(currentState);
    }

    @Override
    public TaskInfo updateTask(
            Session session,
            TaskId taskId,
            Optional<PlanFragment> fragment,
            List<TaskSource> sources,
            OutputBuffers outputBuffers,
            Optional<TableWriteInfo> tableWriteInfo)
    {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(fragment, "fragment is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(outputBuffers, "outputBuffers is null");

        if (resourceOvercommit(session)) {
            // TODO: This should have been done when the QueryContext was created. However, the session isn't available at that point.
            queryContexts.getUnchecked(taskId.getQueryId()).setResourceOvercommit();
        }
        else {
            queryContexts.getUnchecked(
                    taskId.getQueryId()).setMemoryLimits(
                    getQueryMaxMemoryPerNode(session),
                    getQueryMaxTotalMemoryPerNode(session),
                    getQueryMaxBroadcastMemory(session));
        }

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.updateTask(session, fragment, sources, outputBuffers, tableWriteInfo);
    }

    @Override
    public ListenableFuture<BufferResult> getTaskResults(TaskId taskId, OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(startingSequenceId >= 0, "startingSequenceId is negative");
        requireNonNull(maxSize, "maxSize is null");

        return tasks.getUnchecked(taskId).getTaskResults(bufferId, startingSequenceId, maxSize);
    }

    @Override
    public void acknowledgeTaskResults(TaskId taskId, OutputBufferId bufferId, long sequenceId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(sequenceId >= 0, "sequenceId is negative");

        tasks.getUnchecked(taskId).acknowledgeTaskResults(bufferId, sequenceId);
    }

    @Override
    public TaskInfo abortTaskResults(TaskId taskId, OutputBufferId bufferId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        return tasks.getUnchecked(taskId).abortTaskResults(bufferId);
    }

    @Override
    public void removeRemoteSource(TaskId taskId, TaskId remoteSourceTaskId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(remoteSourceTaskId, "remoteSourceTaskId is null");

        tasks.getUnchecked(taskId).removeRemoteSource(remoteSourceTaskId);
    }

    @Override
    public TaskInfo cancelTask(TaskId taskId)
    {
        requireNonNull(taskId, "taskId is null");

        return tasks.getUnchecked(taskId).cancel();
    }

    @Override
    public TaskInfo abortTask(TaskId taskId)
    {
        requireNonNull(taskId, "taskId is null");

        return tasks.getUnchecked(taskId).abort();
    }

    public void removeOldTasks()
    {
        DateTime oldestAllowedTask = DateTime.now().minus(infoCacheTime.toMillis());
        for (TaskInfo taskInfo : filter(transform(tasks.asMap().values(), SqlTask::getTaskInfo), notNull())) {
            TaskId taskId = taskInfo.getTaskId();
            try {
                DateTime endTime = taskInfo.getStats().getEndTime();
                if (endTime != null && endTime.isBefore(oldestAllowedTask)) {
                    tasks.asMap().remove(taskId);
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Error while inspecting age of complete task %s", taskId);
            }
        }
    }

    public void failAbandonedTasks()
    {
        DateTime now = DateTime.now();
        DateTime oldestAllowedHeartbeat = now.minus(clientTimeout.toMillis());
        for (SqlTask sqlTask : tasks.asMap().values()) {
            try {
                TaskInfo taskInfo = sqlTask.getTaskInfo();
                TaskStatus taskStatus = taskInfo.getTaskStatus();
                if (taskStatus.getState().isDone()) {
                    continue;
                }
                DateTime lastHeartbeat = taskInfo.getLastHeartbeat();
                if (lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat)) {
                    log.info("Failing abandoned task %s", taskInfo.getTaskId());
                    sqlTask.failed(new PrestoException(ABANDONED_TASK, format("Task %s has not been accessed since %s: currentTime %s", taskInfo.getTaskId(), lastHeartbeat, now)));
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Error while inspecting age of task %s", sqlTask.getTaskId());
            }
        }
    }

    //
    // Jmxutils only calls nested getters once, so we are forced to maintain a single
    // instance and periodically recalculate the stats.
    //
    private void updateStats()
    {
        SqlTaskIoStats tempIoStats = new SqlTaskIoStats();
        tempIoStats.merge(finishedTaskStats);

        // there is a race here between task completion, which merges stats into
        // finishedTaskStats, and getting the stats from the task.  Since we have
        // already merged the final stats, we could miss the stats from this task
        // which would result in an under-count, but we will not get an over-count.
        tasks.asMap().values().stream()
                .filter(task -> !task.getTaskStatus().getState().isDone())
                .forEach(task -> tempIoStats.merge(task.getIoStats()));

        cachedStats.resetTo(tempIoStats);
    }

    @Override
    public void addStateChangeListener(TaskId taskId, StateChangeListener<TaskState> stateChangeListener)
    {
        requireNonNull(taskId, "taskId is null");
        tasks.getUnchecked(taskId).addStateChangeListener(stateChangeListener);
    }

    @VisibleForTesting
    public QueryContext getQueryContext(QueryId queryId)

    {
        return queryContexts.getUnchecked(queryId);
    }
}
