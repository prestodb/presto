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
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class SqlTask
{
    private static final Logger log = Logger.get(SqlTask.class);

    private final TaskId taskId;
    private final String taskInstanceId;
    private final URI location;
    private final TaskStateMachine taskStateMachine;
    private final SharedBuffer sharedBuffer;
    private final QueryContext queryContext;

    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;

    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
    private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskStatus.STARTING_VERSION);

    private final AtomicReference<TaskHolder> taskHolderReference = new AtomicReference<>(new TaskHolder());
    private final AtomicBoolean needsPlan = new AtomicBoolean(true);

    public SqlTask(
            TaskId taskId,
            URI location,
            QueryContext queryContext,
            SqlTaskExecutionFactory sqlTaskExecutionFactory,
            ExecutorService taskNotificationExecutor,
            final Function<SqlTask, ?> onDone,
            DataSize maxBufferSize)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = UUID.randomUUID().toString();
        this.location = requireNonNull(location, "location is null");
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.sqlTaskExecutionFactory = requireNonNull(sqlTaskExecutionFactory, "sqlTaskExecutionFactory is null");
        requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        requireNonNull(onDone, "onDone is null");
        requireNonNull(maxBufferSize, "maxBufferSize is null");

        sharedBuffer = new SharedBuffer(taskId, taskInstanceId, taskNotificationExecutor, maxBufferSize, new UpdateSystemMemory(queryContext));
        taskStateMachine = new TaskStateMachine(taskId, taskNotificationExecutor);
        taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>()
        {
            @Override
            public void stateChanged(TaskState newState)
            {
                if (!newState.isDone()) {
                    return;
                }

                // store final task info
                while (true) {
                    TaskHolder taskHolder = taskHolderReference.get();
                    if (taskHolder.isFinished()) {
                        // another concurrent worker already set the final state
                        return;
                    }

                    if (taskHolderReference.compareAndSet(taskHolder, new TaskHolder(createTaskInfo(taskHolder), taskHolder.getIoStats()))) {
                        break;
                    }
                }

                // make sure buffers are cleaned up
                if (newState == TaskState.FAILED || newState == TaskState.ABORTED) {
                    // don't close buffers for a failed query
                    // closed buffers signal to upstream tasks that everything finished cleanly
                    sharedBuffer.fail();
                }
                else {
                    sharedBuffer.destroy();
                }

                try {
                    onDone.apply(SqlTask.this);
                }
                catch (Exception e) {
                    log.warn(e, "Error running task cleanup callback %s", SqlTask.this.taskId);
                }
            }
        });
    }

    private static final class UpdateSystemMemory
            implements SystemMemoryUsageListener
    {
        private final QueryContext queryContext;

        public UpdateSystemMemory(QueryContext queryContext)
        {
            this.queryContext = requireNonNull(queryContext, "queryContext is null");
        }

        @Override
        public void updateSystemMemoryUsage(long deltaMemoryInBytes)
        {
            if (deltaMemoryInBytes > 0) {
                queryContext.reserveSystemMemory(deltaMemoryInBytes);
            }
            else {
                queryContext.freeSystemMemory(-deltaMemoryInBytes);
            }
        }
    }

    public SqlTaskIoStats getIoStats()
    {
        return taskHolderReference.get().getIoStats();
    }

    public TaskId getTaskId()
    {
        return taskStateMachine.getTaskId();
    }

    public String getTaskInstanceId()
    {
        return taskInstanceId;
    }

    public void recordHeartbeat()
    {
        lastHeartbeat.set(DateTime.now());
    }

    public TaskInfo getTaskInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            return createTaskInfo(taskHolderReference.get());
        }
    }

    private TaskInfo createTaskInfo(TaskHolder taskHolder)
    {
        // Always return a new TaskInfo with a larger version number;
        // otherwise a client will not accept the update
        long versionNumber = nextTaskInfoVersion.getAndIncrement();

        TaskState state = taskStateMachine.getState();
        List<ExecutionFailureInfo> failures = ImmutableList.of();
        if (state == TaskState.FAILED) {
            failures = toFailures(taskStateMachine.getFailureCauses());
        }

        TaskStats taskStats;
        Set<PlanNodeId> noMoreSplits;

        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            taskStats = finalTaskInfo.getStats();
            noMoreSplits = finalTaskInfo.getNoMoreSplits();
        }
        else {
            SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
            if (taskExecution != null) {
                taskStats = taskExecution.getTaskContext().getTaskStats();
                noMoreSplits = taskExecution.getNoMoreSplits();
            }
            else {
                // if the task completed without creation, set end time
                DateTime endTime = state.isDone() ? DateTime.now() : null;
                taskStats = new TaskStats(taskStateMachine.getCreatedTime(), endTime);
                noMoreSplits = ImmutableSet.of();
            }
        }

        return new TaskInfo(
                new TaskStatus(taskStateMachine.getTaskId(),
                        taskInstanceId,
                        versionNumber,
                        state,
                        location,
                        failures,
                        taskStats.getQueuedPartitionedDrivers(),
                        taskStats.getRunningPartitionedDrivers(),
                        taskStats.getMemoryReservation()),
                lastHeartbeat.get(),
                sharedBuffer.getInfo(),
                noMoreSplits,
                taskStats,
                needsPlan.get());
    }

    public CompletableFuture<TaskStatus> getTaskStatus(TaskState callersCurrentState)
    {
        requireNonNull(callersCurrentState, "callersCurrentState is null");

        if (callersCurrentState.isDone()) {
            return completedFuture(getTaskInfo().getTaskStatus());
        }

        CompletableFuture<TaskState> futureTaskState = taskStateMachine.getStateChange(callersCurrentState);
        return futureTaskState.thenApply(input -> getTaskInfo().getTaskStatus());
    }

    public CompletableFuture<TaskInfo> getTaskInfo(TaskState callersCurrentState)
    {
        requireNonNull(callersCurrentState, "callersCurrentState is null");

        // If the caller's current state is already done, just return the current
        // state of this task as it will either be done or possibly still running
        // (due to a bug in the caller), since we can not transition from a done
        // state.
        if (callersCurrentState.isDone()) {
            return completedFuture(getTaskInfo());
        }

        CompletableFuture<TaskState> futureTaskState = taskStateMachine.getStateChange(callersCurrentState);
        return futureTaskState.thenApply(input -> getTaskInfo());
    }

    public TaskInfo updateTask(Session session, Optional<PlanFragment> fragment, List<TaskSource> sources, OutputBuffers outputBuffers)
    {
        try {
            // assure the task execution is only created once
            SqlTaskExecution taskExecution;
            synchronized (this) {
                // is task already complete?
                TaskHolder taskHolder = taskHolderReference.get();
                if (taskHolder.isFinished()) {
                    return taskHolder.getFinalTaskInfo();
                }
                taskExecution = taskHolder.getTaskExecution();
                if (taskExecution == null) {
                    checkState(fragment.isPresent(), "fragment must be present");
                    taskExecution = sqlTaskExecutionFactory.create(session, queryContext, taskStateMachine, sharedBuffer, fragment.get(), sources);
                    taskHolderReference.compareAndSet(taskHolder, new TaskHolder(taskExecution));
                    needsPlan.set(false);
                }
            }

            if (taskExecution != null) {
                // addSources checks for task completion, so update the buffers first and the task might complete earlier
                sharedBuffer.setOutputBuffers(outputBuffers);
                taskExecution.addSources(sources);
            }
        }
        catch (Error e) {
            failed(e);
            throw e;
        }
        catch (RuntimeException e) {
            failed(e);
        }

        return getTaskInfo();
    }

    public CompletableFuture<BufferResult> getTaskResults(TaskId outputName, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(outputName, "outputName is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return sharedBuffer.get(outputName, startingSequenceId, maxSize);
    }

    public TaskInfo abortTaskResults(TaskId outputId)
    {
        requireNonNull(outputId, "outputId is null");

        log.debug("Aborting task %s output %s", taskId, outputId);
        sharedBuffer.abort(outputId);

        return getTaskInfo();
    }

    public void failed(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        taskStateMachine.failed(cause);
    }

    public TaskInfo cancel()
    {
        taskStateMachine.cancel();
        return getTaskInfo();
    }

    public TaskInfo abort()
    {
        taskStateMachine.abort();
        return getTaskInfo();
    }

    @Override
    public String toString()
    {
        return taskId.toString();
    }

    private static final class TaskHolder
    {
        private final SqlTaskExecution taskExecution;
        private final TaskInfo finalTaskInfo;
        private final SqlTaskIoStats finalIoStats;

        private TaskHolder()
        {
            this.taskExecution = null;
            this.finalTaskInfo = null;
            this.finalIoStats = null;
        }

        private TaskHolder(SqlTaskExecution taskExecution)
        {
            this.taskExecution = requireNonNull(taskExecution, "taskExecution is null");
            this.finalTaskInfo = null;
            this.finalIoStats = null;
        }

        private TaskHolder(TaskInfo finalTaskInfo, SqlTaskIoStats finalIoStats)
        {
            this.taskExecution = null;
            this.finalTaskInfo = requireNonNull(finalTaskInfo, "finalTaskInfo is null");
            this.finalIoStats = requireNonNull(finalIoStats, "finalIoStats is null");
        }

        public boolean isFinished()
        {
            return finalTaskInfo != null;
        }

        @Nullable
        public SqlTaskExecution getTaskExecution()
        {
            return taskExecution;
        }

        @Nullable
        public TaskInfo getFinalTaskInfo()
        {
            return finalTaskInfo;
        }

        public SqlTaskIoStats getIoStats()
        {
            // if we are finished, return the final IoStats
            if (finalIoStats != null) {
                return finalIoStats;
            }
            // if we haven't started yet, return an empty IoStats
            if (taskExecution == null) {
                return new SqlTaskIoStats();
            }
            // get IoStats from the current task execution
            TaskContext taskContext = taskExecution.getTaskContext();
            return new SqlTaskIoStats(taskContext.getInputDataSize(), taskContext.getInputPositions(), taskContext.getOutputDataSize(), taskContext.getOutputPositions());
        }
    }

    public void addStateChangeListener(StateChangeListener<TaskState> stateChangeListener)
    {
        taskStateMachine.addStateChangeListener(stateChangeListener);
    }
}
