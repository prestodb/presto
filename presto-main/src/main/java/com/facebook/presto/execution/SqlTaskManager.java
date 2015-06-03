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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.Session;
import com.facebook.presto.TaskSource;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.MemoryPoolAssignment;
import com.facebook.presto.memory.MemoryPoolAssignmentsRequest;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class SqlTaskManager
        implements TaskManager, Closeable
{
    private static final Logger log = Logger.get(SqlTaskManager.class);

    private final ExecutorService taskNotificationExecutor;
    private final ThreadPoolExecutorMBean taskNotificationExecutorMBean;

    private final ScheduledExecutorService taskManagementExecutor;
    private final ThreadPoolExecutorMBean taskManagementExecutorMBean;

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

    @Inject
    public SqlTaskManager(
            LocalExecutionPlanner planner,
            LocationFactory locationFactory,
            TaskExecutor taskExecutor,
            QueryMonitor queryMonitor,
            NodeInfo nodeInfo,
            LocalMemoryManager localMemoryManager,
            TaskManagerConfig config,
            MemoryManagerConfig memoryManagerConfig)
    {
        checkNotNull(nodeInfo, "nodeInfo is null");
        checkNotNull(config, "config is null");
        infoCacheTime = config.getInfoMaxAge();
        clientTimeout = config.getClientTimeout();

        DataSize maxBufferSize = config.getSinkMaxBufferSize();

        taskNotificationExecutor = newCachedThreadPool(threadsNamed("task-notification-%s"));
        taskNotificationExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) taskNotificationExecutor);

        taskManagementExecutor = newScheduledThreadPool(5, threadsNamed("task-management-%s"));
        taskManagementExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) taskManagementExecutor);

        SqlTaskExecutionFactory sqlTaskExecutionFactory = new SqlTaskExecutionFactory(taskNotificationExecutor, taskExecutor, planner, queryMonitor, config);

        this.localMemoryManager = requireNonNull(localMemoryManager, "localMemoryManager is null");
        DataSize maxQueryMemoryPerNode = memoryManagerConfig.getMaxQueryMemoryPerNode();
        boolean clusterMemoryManagerEnabled = memoryManagerConfig.isClusterMemoryManagerEnabled();

        queryContexts = CacheBuilder.newBuilder().weakValues().build(new CacheLoader<QueryId, QueryContext>()
        {
            @Override
            public QueryContext load(QueryId key)
                    throws Exception
            {
                return new QueryContext(clusterMemoryManagerEnabled, maxQueryMemoryPerNode, localMemoryManager.getPool(LocalMemoryManager.GENERAL_POOL), taskNotificationExecutor);
            }
        });

        tasks = CacheBuilder.newBuilder().build(new CacheLoader<TaskId, SqlTask>()
        {
            @Override
            public SqlTask load(TaskId taskId)
                    throws Exception
            {
                return new SqlTask(
                        taskId,
                        nodeInfo.getInstanceId(),
                        locationFactory.createLocalTaskLocation(taskId),
                        queryContexts.getUnchecked(taskId.getQueryId()),
                        sqlTaskExecutionFactory,
                        taskNotificationExecutor,
                        sqlTask -> {
                                finishedTaskStats.merge(sqlTask.getIoStats());
                                return null;
                        },
                        maxBufferSize
                );
            }
        });
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
            queryContexts.getUnchecked(assignment.getQueryId()).setMemoryPool(localMemoryManager.getPool(assignment.getPoolId()));
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
        taskNotificationExecutor.shutdownNow();
        taskManagementExecutor.shutdownNow();
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

    @Managed(description = "Task garbage collector executor")
    @Nested
    public ThreadPoolExecutorMBean getTaskManagementExecutor()
    {
        return taskManagementExecutorMBean;
    }

    @Override
    public List<TaskInfo> getAllTaskInfo()
    {
        return ImmutableList.copyOf(transform(tasks.asMap().values(), SqlTask::getTaskInfo));
    }

    @Override
    public TaskInfo getTaskInfo(TaskId taskId)
    {
        checkNotNull(taskId, "taskId is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInfo();
    }

    @Override
    public ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, TaskState currentState)
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(currentState, "currentState is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInfo(currentState);
    }

    @Override
    public TaskInfo updateTask(Session session, TaskId taskId, PlanFragment fragment, List<TaskSource> sources, OutputBuffers outputBuffers)
    {
        checkNotNull(session, "session is null");
        checkNotNull(taskId, "taskId is null");
        checkNotNull(fragment, "fragment is null");
        checkNotNull(sources, "sources is null");
        checkNotNull(outputBuffers, "outputBuffers is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.updateTask(session, fragment, sources, outputBuffers);
    }

    @Override
    public ListenableFuture<BufferResult> getTaskResults(TaskId taskId, TaskId outputName, long startingSequenceId, DataSize maxSize)
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputName, "outputName is null");
        Preconditions.checkArgument(startingSequenceId >= 0, "startingSequenceId is negative");
        checkNotNull(maxSize, "maxSize is null");

        return tasks.getUnchecked(taskId).getTaskResults(outputName, startingSequenceId, maxSize);
    }

    @Override
    public TaskInfo abortTaskResults(TaskId taskId, TaskId outputId)
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        return tasks.getUnchecked(taskId).abortTaskResults(outputId);
    }

    @Override
    public TaskInfo cancelTask(TaskId taskId)
    {
        checkNotNull(taskId, "taskId is null");

        return tasks.getUnchecked(taskId).cancel();
    }

    @Override
    public TaskInfo abortTask(TaskId taskId)
    {
        checkNotNull(taskId, "taskId is null");

        return tasks.getUnchecked(taskId).abort();
    }

    public void removeOldTasks()
    {
        DateTime oldestAllowedTask = DateTime.now().minus(infoCacheTime.toMillis());
        for (TaskInfo taskInfo : filter(transform(tasks.asMap().values(), SqlTask::getTaskInfo), notNull())) {
            try {
                DateTime endTime = taskInfo.getStats().getEndTime();
                if (endTime != null && endTime.isBefore(oldestAllowedTask)) {
                    tasks.asMap().remove(taskInfo.getTaskId());
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Error while inspecting age of complete task %s", taskInfo.getTaskId());
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
                if (taskInfo.getState().isDone()) {
                    continue;
                }
                DateTime lastHeartbeat = taskInfo.getLastHeartbeat();
                if (lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat)) {
                    log.info("Failing abandoned task %s", taskInfo.getTaskId());
                    sqlTask.failed(new AbandonedException("Task " + taskInfo.getTaskId(), lastHeartbeat, now));
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
                .filter(task -> !task.getTaskInfo().getState().isDone())
                .forEach(task -> tempIoStats.merge(task.getIoStats()));

        cachedStats.resetTo(tempIoStats);
    }
}
