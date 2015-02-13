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
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SqlTask
{
    private static final Logger log = Logger.get(SqlTask.class);

    private final TaskId taskId;
    private final String nodeInstanceId;
    private final URI location;
    private final TaskStateMachine taskStateMachine;
    private final SharedBuffer sharedBuffer;

    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;

    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
    private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskInfo.STARTING_VERSION);

    private final AtomicReference<TaskHolder> taskHolderReference = new AtomicReference<>(new TaskHolder());

    public SqlTask(
            TaskId taskId,
            String nodeInstanceId,
            URI location,
            SqlTaskExecutionFactory sqlTaskExecutionFactory,
            ExecutorService taskNotificationExecutor,
            final Function<SqlTask, ?> onDone,
            DataSize maxBufferSize)
    {
        this.taskId = checkNotNull(taskId, "taskId is null");
        this.nodeInstanceId = checkNotNull(nodeInstanceId, "nodeInstanceId is null");
        this.location = checkNotNull(location, "location is null");
        this.sqlTaskExecutionFactory = checkNotNull(sqlTaskExecutionFactory, "sqlTaskExecutionFactory is null");
        checkNotNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        checkNotNull(onDone, "onDone is null");
        checkNotNull(maxBufferSize, "maxBufferSize is null");

        sharedBuffer = new SharedBuffer(taskId, taskNotificationExecutor, maxBufferSize);
        taskStateMachine = new TaskStateMachine(taskId, taskNotificationExecutor);
        taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>()
        {
            @Override
            public void stateChanged(TaskState taskState)
            {
                if (!taskState.isDone()) {
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
                if (taskState == TaskState.FAILED || taskState == TaskState.ABORTED) {
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

    public SqlTaskIoStats getIoStats()
    {
        return taskHolderReference.get().getIoStats();
    }

    public TaskId getTaskId()
    {
        return taskStateMachine.getTaskId();
    }

    public TaskInfo getTaskInfo()
    {
        lastHeartbeat.set(DateTime.now());

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
                taskStateMachine.getTaskId(),
                Optional.of(nodeInstanceId),
                versionNumber,
                state,
                location,
                lastHeartbeat.get(),
                sharedBuffer.getInfo(),
                noMoreSplits,
                taskStats,
                failures);
    }

    public ListenableFuture<TaskInfo> getTaskInfo(TaskState callersCurrentState)
    {
        checkNotNull(callersCurrentState, "callersCurrentState is null");
        lastHeartbeat.set(DateTime.now());

        // If the caller's current state is already done, just return the current
        // state of this task as it will either be done or possibly still running
        // (due to a bug in the caller), since we can not transition from a done
        // state.
        if (callersCurrentState.isDone()) {
            return Futures.immediateFuture(getTaskInfo());
        }

        ListenableFuture<TaskState> futureTaskState = taskStateMachine.getStateChange(callersCurrentState);
        return Futures.transform(futureTaskState, (TaskState input) -> getTaskInfo());
    }

    public TaskInfo updateTask(Session session, PlanFragment fragment, List<TaskSource> sources, OutputBuffers outputBuffers)
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
                    taskExecution = sqlTaskExecutionFactory.create(session, taskStateMachine, sharedBuffer, fragment, sources);
                    taskHolderReference.compareAndSet(taskHolder, new TaskHolder(taskExecution));
                }
            }

            lastHeartbeat.set(DateTime.now());

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

    public ListenableFuture<BufferResult> getTaskResults(TaskId outputName, long startingSequenceId, DataSize maxSize)
    {
        checkNotNull(outputName, "outputName is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        lastHeartbeat.set(DateTime.now());

        return sharedBuffer.get(outputName, startingSequenceId, maxSize);
    }

    public TaskInfo abortTaskResults(TaskId outputId)
    {
        checkNotNull(outputId, "outputId is null");

        lastHeartbeat.set(DateTime.now());

        log.debug("Aborting task %s output %s", taskId, outputId);
        sharedBuffer.abort(outputId);

        return getTaskInfo();
    }

    public void failed(Throwable cause)
    {
        checkNotNull(cause, "cause is null");

        taskStateMachine.failed(cause);
    }

    public TaskInfo cancel()
    {
        lastHeartbeat.set(DateTime.now());

        taskStateMachine.cancel();
        return getTaskInfo();
    }

    public TaskInfo abort()
    {
        lastHeartbeat.set(DateTime.now());

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
            this.taskExecution = checkNotNull(taskExecution, "taskExecution is null");
            this.finalTaskInfo = null;
            this.finalIoStats = null;
        }

        private TaskHolder(TaskInfo finalTaskInfo, SqlTaskIoStats finalIoStats)
        {
            this.taskExecution = null;
            this.finalTaskInfo = checkNotNull(finalTaskInfo, "finalTaskInfo is null");
            this.finalIoStats = checkNotNull(finalIoStats, "finalIoStats is null");
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
}
