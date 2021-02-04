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

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.Session;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.LazyOutputBuffer;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.PipelineStatus;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskExchangeClientManager;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdater;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.TaskState.ABORTED;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class SqlTask
{
    private static final Logger log = Logger.get(SqlTask.class);

    private final TaskId taskId;
    private final TaskInstanceId taskInstanceId;
    private final URI location;
    private final String nodeId;
    private final TaskStateMachine taskStateMachine;
    private final OutputBuffer outputBuffer;
    private final QueryContext queryContext;

    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;
    private final TaskExchangeClientManager taskExchangeClientManager;

    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
    private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskStatus.STARTING_VERSION);

    private final AtomicReference<TaskHolder> taskHolderReference = new AtomicReference<>(new TaskHolder());
    private final AtomicBoolean needsPlan = new AtomicBoolean(true);

    public static SqlTask createSqlTask(
            TaskId taskId,
            URI location,
            String nodeId,
            QueryContext queryContext,
            SqlTaskExecutionFactory sqlTaskExecutionFactory,
            ExchangeClientSupplier exchangeClientSupplier,
            ExecutorService taskNotificationExecutor,
            Function<SqlTask, ?> onDone,
            DataSize maxBufferSize,
            CounterStat failedTasks)
    {
        SqlTask sqlTask = new SqlTask(
                taskId,
                location,
                nodeId,
                queryContext,
                sqlTaskExecutionFactory,
                exchangeClientSupplier,
                taskNotificationExecutor,
                maxBufferSize);
        sqlTask.initialize(onDone, failedTasks);
        return sqlTask;
    }

    private SqlTask(
            TaskId taskId,
            URI location,
            String nodeId,
            QueryContext queryContext,
            SqlTaskExecutionFactory sqlTaskExecutionFactory,
            ExchangeClientSupplier exchangeClientSupplier,
            ExecutorService taskNotificationExecutor,
            DataSize maxBufferSize)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = new TaskInstanceId(UUID.randomUUID());
        this.location = requireNonNull(location, "location is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.sqlTaskExecutionFactory = requireNonNull(sqlTaskExecutionFactory, "sqlTaskExecutionFactory is null");
        requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        requireNonNull(maxBufferSize, "maxBufferSize is null");

        this.taskExchangeClientManager = new TaskExchangeClientManager(exchangeClientSupplier);
        outputBuffer = new LazyOutputBuffer(
                taskId,
                taskInstanceId.getUuidString(),
                taskNotificationExecutor,
                maxBufferSize,
                // Pass a memory context supplier instead of a memory context to the output buffer,
                // because we haven't created the task context that holds the the memory context yet.
                () -> queryContext.getTaskContextByTaskId(taskId).localSystemMemoryContext());
        taskStateMachine = new TaskStateMachine(taskId, taskNotificationExecutor);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize(Function<SqlTask, ?> onDone, CounterStat failedTasks)
    {
        requireNonNull(onDone, "onDone is null");
        requireNonNull(failedTasks, "failedTasks is null");
        taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>()
        {
            @Override
            public void stateChanged(TaskState newState)
            {
                if (!newState.isDone()) {
                    return;
                }

                // Update failed tasks counter
                if (newState == FAILED) {
                    failedTasks.update(1);
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
                if (newState == FAILED || newState == ABORTED) {
                    // don't close buffers for a failed query
                    // closed buffers signal to upstream tasks that everything finished cleanly
                    outputBuffer.fail();
                }
                else {
                    outputBuffer.destroy();
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

    public boolean isOutputBufferOverutilized()
    {
        return outputBuffer.isOverutilized();
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
        return taskInstanceId.getUuidString();
    }

    public void recordHeartbeat()
    {
        lastHeartbeat.set(DateTime.now());
    }

    public TaskState getTaskState()
    {
        return taskStateMachine.getState();
    }

    public DateTime getTaskCreatedTime()
    {
        return taskStateMachine.getCreatedTime();
    }

    public TaskInfo getTaskInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            return createTaskInfo(taskHolderReference.get());
        }
    }

    public TaskStatus getTaskStatus()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            return createTaskStatus(taskHolderReference.get());
        }
    }

    private TaskStatus createTaskStatus(TaskHolder taskHolder)
    {
        // Always return a new TaskInfo with a larger version number;
        // otherwise a client will not accept the update
        long versionNumber = nextTaskInfoVersion.getAndIncrement();

        TaskState state = taskStateMachine.getState();
        List<ExecutionFailureInfo> failures = ImmutableList.of();
        if (state == FAILED) {
            failures = toFailures(taskStateMachine.getFailureCauses());
        }

        int queuedPartitionedDrivers = 0;
        int runningPartitionedDrivers = 0;
        long physicalWrittenDataSizeInBytes = 0L;
        long userMemoryReservationInBytes = 0L;
        long systemMemoryReservationInBytes = 0L;
        // TODO: add a mechanism to avoid sending the whole completedDriverGroups set over the wire for every task status reply
        Set<Lifespan> completedDriverGroups = ImmutableSet.of();
        long fullGcCount = 0;
        long fullGcTimeInMillis = 0L;
        if (taskHolder.getFinalTaskInfo() != null) {
            TaskStats taskStats = taskHolder.getFinalTaskInfo().getStats();
            queuedPartitionedDrivers = taskStats.getQueuedPartitionedDrivers();
            runningPartitionedDrivers = taskStats.getRunningPartitionedDrivers();
            physicalWrittenDataSizeInBytes = taskStats.getPhysicalWrittenDataSizeInBytes();
            userMemoryReservationInBytes = taskStats.getUserMemoryReservationInBytes();
            systemMemoryReservationInBytes = taskStats.getSystemMemoryReservationInBytes();
            fullGcCount = taskStats.getFullGcCount();
            fullGcTimeInMillis = taskStats.getFullGcTimeInMillis();
        }
        else if (taskHolder.getTaskExecution() != null) {
            long physicalWrittenBytes = 0;
            TaskContext taskContext = taskHolder.getTaskExecution().getTaskContext();
            for (PipelineContext pipelineContext : taskContext.getPipelineContexts()) {
                PipelineStatus pipelineStatus = pipelineContext.getPipelineStatus();
                queuedPartitionedDrivers += pipelineStatus.getQueuedPartitionedDrivers();
                runningPartitionedDrivers += pipelineStatus.getRunningPartitionedDrivers();
                physicalWrittenBytes += pipelineContext.getPhysicalWrittenDataSize();
            }
            physicalWrittenDataSizeInBytes = physicalWrittenBytes;
            userMemoryReservationInBytes = taskContext.getMemoryReservation().toBytes();
            systemMemoryReservationInBytes = taskContext.getSystemMemoryReservation().toBytes();
            completedDriverGroups = taskContext.getCompletedDriverGroups();
            fullGcCount = taskContext.getFullGcCount();
            fullGcTimeInMillis = taskContext.getFullGcTime().toMillis();
        }

        return new TaskStatus(
                taskInstanceId.getUuidLeastSignificantBits(),
                taskInstanceId.getUuidMostSignificantBits(),
                versionNumber,
                state,
                location,
                completedDriverGroups,
                failures,
                queuedPartitionedDrivers,
                runningPartitionedDrivers,
                outputBuffer.getUtilization(),
                isOutputBufferOverutilized(),
                physicalWrittenDataSizeInBytes,
                userMemoryReservationInBytes,
                systemMemoryReservationInBytes,
                queryContext.getPeakNodeTotalMemory(),
                fullGcCount,
                fullGcTimeInMillis);
    }

    private TaskStats getTaskStats(TaskHolder taskHolder)
    {
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            return finalTaskInfo.getStats();
        }
        SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
        if (taskExecution != null) {
            return taskExecution.getTaskContext().getTaskStats();
        }
        // if the task completed without creation, set end time
        DateTime endTime = taskStateMachine.getState().isDone() ? DateTime.now() : null;
        return new TaskStats(taskStateMachine.getCreatedTime(), endTime);
    }

    private MetadataUpdates getMetadataUpdateRequests(TaskHolder taskHolder)
    {
        ConnectorId connectorId = null;
        ImmutableList.Builder<ConnectorMetadataUpdateHandle> connectorMetadataUpdatesBuilder = ImmutableList.builder();

        if (taskHolder.getTaskExecution() != null) {
            TaskMetadataContext taskMetadataContext = taskHolder.getTaskExecution().getTaskContext().getTaskMetadataContext();
            if (!taskMetadataContext.getMetadataUpdaters().isEmpty()) {
                connectorId = taskMetadataContext.getConnectorId();
                for (ConnectorMetadataUpdater metadataUpdater : taskMetadataContext.getMetadataUpdaters()) {
                    connectorMetadataUpdatesBuilder.addAll(metadataUpdater.getPendingMetadataUpdateRequests());
                }
            }
        }

        return new MetadataUpdates(connectorId, connectorMetadataUpdatesBuilder.build());
    }

    private static Set<PlanNodeId> getNoMoreSplits(TaskHolder taskHolder)
    {
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            return finalTaskInfo.getNoMoreSplits();
        }
        SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
        if (taskExecution != null) {
            return taskExecution.getNoMoreSplits();
        }
        return ImmutableSet.of();
    }

    private TaskInfo createTaskInfo(TaskHolder taskHolder)
    {
        TaskStats taskStats = getTaskStats(taskHolder);
        Set<PlanNodeId> noMoreSplits = getNoMoreSplits(taskHolder);
        MetadataUpdates metadataRequests = getMetadataUpdateRequests(taskHolder);

        TaskStatus taskStatus = createTaskStatus(taskHolder);
        return new TaskInfo(
                taskStateMachine.getTaskId(),
                taskStatus,
                lastHeartbeat.get(),
                outputBuffer.getInfo(),
                noMoreSplits,
                taskStats,
                needsPlan.get(),
                metadataRequests);
    }

    public ListenableFuture<TaskStatus> getTaskStatus(TaskState callersCurrentState)
    {
        requireNonNull(callersCurrentState, "callersCurrentState is null");

        if (callersCurrentState.isDone()) {
            return immediateFuture(getTaskStatus());
        }

        ListenableFuture<TaskState> futureTaskState = taskStateMachine.getStateChange(callersCurrentState);
        return Futures.transform(futureTaskState, input -> getTaskStatus(), directExecutor());
    }

    public ListenableFuture<TaskInfo> getTaskInfo(TaskState callersCurrentState)
    {
        requireNonNull(callersCurrentState, "callersCurrentState is null");

        // If the caller's current state is already done, just return the current
        // state of this task as it will either be done or possibly still running
        // (due to a bug in the caller), since we can not transition from a done
        // state.
        if (callersCurrentState.isDone()) {
            return immediateFuture(getTaskInfo());
        }

        ListenableFuture<TaskState> futureTaskState = taskStateMachine.getStateChange(callersCurrentState);
        return Futures.transform(futureTaskState, input -> getTaskInfo(), directExecutor());
    }

    public TaskInfo updateTask(
            Session session,
            Optional<PlanFragment> fragment,
            List<TaskSource> sources,
            OutputBuffers outputBuffers,
            Optional<TableWriteInfo> tableWriteInfo)
    {
        try {
            // The LazyOutput buffer does not support write methods, so the actual
            // output buffer must be established before drivers are created (e.g.
            // a VALUES query).
            outputBuffer.setOutputBuffers(outputBuffers);

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
                    checkState(tableWriteInfo.isPresent(), "tableWriteInfo must be present");
                    taskExecution = sqlTaskExecutionFactory.create(
                            session,
                            queryContext,
                            taskStateMachine,
                            outputBuffer,
                            taskExchangeClientManager,
                            fragment.get(),
                            sources,
                            tableWriteInfo.get());
                    taskHolderReference.compareAndSet(taskHolder, new TaskHolder(taskExecution));
                    needsPlan.set(false);
                }
            }

            if (taskExecution != null) {
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

    public TaskMetadataContext getTaskMetadataContext()
    {
        return taskHolderReference.get().taskExecution.getTaskContext().getTaskMetadataContext();
    }

    public ListenableFuture<BufferResult> getTaskResults(OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return outputBuffer.get(bufferId, startingSequenceId, maxSize);
    }

    public void acknowledgeTaskResults(OutputBufferId bufferId, long sequenceId)
    {
        requireNonNull(bufferId, "bufferId is null");

        outputBuffer.acknowledge(bufferId, sequenceId);
    }

    public TaskInfo abortTaskResults(OutputBufferId bufferId)
    {
        requireNonNull(bufferId, "bufferId is null");

        log.debug("Aborting task %s output %s", taskId, bufferId);
        outputBuffer.abort(bufferId);

        return getTaskInfo();
    }

    public void removeRemoteSource(TaskId sourceTaskId)
    {
        requireNonNull(sourceTaskId, "sourceTaskId is null");

        log.debug("Removing remote source %s from task %s", sourceTaskId, taskId);

        taskExchangeClientManager.getExchangeClients()
                .forEach(exchangeClient -> exchangeClient.removeRemoteSource(sourceTaskId));
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

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<TaskState> stateChangeListener)
    {
        taskStateMachine.addStateChangeListener(stateChangeListener);
    }

    public QueryContext getQueryContext()
    {
        return queryContext;
    }

    public Optional<TaskContext> getTaskContext()
    {
        SqlTaskExecution taskExecution = taskHolderReference.get().getTaskExecution();
        if (taskExecution == null) {
            return Optional.empty();
        }
        return Optional.of(taskExecution.getTaskContext());
    }

    private static class TaskInstanceId
    {
        private final long uuidLeastSignificantBits;
        private final long uuidMostSignificantBits;
        private final String uuidString;

        public TaskInstanceId(UUID uuid)
        {
            requireNonNull(uuid, "uuid is null");
            this.uuidLeastSignificantBits = uuid.getLeastSignificantBits();
            this.uuidMostSignificantBits = uuid.getMostSignificantBits();
            this.uuidString = uuid.toString();
        }

        public long getUuidLeastSignificantBits()
        {
            return uuidLeastSignificantBits;
        }

        public long getUuidMostSignificantBits()
        {
            return uuidMostSignificantBits;
        }

        public String getUuidString()
        {
            return uuidString;
        }
    }
}
