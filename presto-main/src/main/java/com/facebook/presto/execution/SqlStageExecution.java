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

import com.facebook.presto.Session;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.getMaxFailedTaskPercentage;
import static com.facebook.presto.failureDetector.FailureDetector.State.GONE;
import static com.facebook.presto.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_RECOVERY_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.PAGE_TRANSPORT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.PAGE_TRANSPORT_TIMEOUT;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public final class SqlStageExecution
{
    public static final Set<ErrorCode> RECOVERABLE_ERROR_CODES = ImmutableSet.of(
            TOO_MANY_REQUESTS_FAILED.toErrorCode(),
            PAGE_TRANSPORT_ERROR.toErrorCode(),
            PAGE_TRANSPORT_TIMEOUT.toErrorCode(),
            REMOTE_TASK_MISMATCH.toErrorCode(),
            REMOTE_TASK_ERROR.toErrorCode());

    private final Session session;
    private final StageExecutionStateMachine stateMachine;
    private final PlanFragment planFragment;
    private final RemoteTaskFactory remoteTaskFactory;
    private final NodeTaskMap nodeTaskMap;
    private final boolean summarizeTaskInfo;
    private final Executor executor;
    private final FailureDetector failureDetector;
    private final double maxFailedTaskPercentage;

    private final Map<PlanFragmentId, RemoteSourceNode> exchangeSources;

    private final TableWriteInfo tableWriteInfo;

    private final Map<InternalNode, Set<RemoteTask>> tasks = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final AtomicInteger nextTaskId = new AtomicInteger();
    @GuardedBy("this")
    private final Set<TaskId> allTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<TaskId> finishedTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<TaskId> failedTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<TaskId> tasksWithFinalInfo = newConcurrentHashSet();

    private final Set<Lifespan> finishedLifespans = ConcurrentHashMap.newKeySet();
    private final int totalLifespans;

    @GuardedBy("this")
    private final AtomicBoolean splitsScheduled = new AtomicBoolean();

    @GuardedBy("this")
    private final Multimap<PlanNodeId, RemoteTask> sourceTasks = HashMultimap.create();
    @GuardedBy("this")
    private final Set<PlanNodeId> completeSources = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<PlanFragmentId> completeSourceFragments = newConcurrentHashSet();

    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();

    private final ListenerManager<Set<Lifespan>> completedLifespansChangeListeners = new ListenerManager<>();

    @GuardedBy("this")
    private Optional<StageTaskRecoveryCallback> stageTaskRecoveryCallback = Optional.empty();

    public static SqlStageExecution createSqlStageExecution(
            StageExecutionId stageExecutionId,
            PlanFragment fragment,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            boolean summarizeTaskInfo,
            NodeTaskMap nodeTaskMap,
            ExecutorService executor,
            FailureDetector failureDetector,
            SplitSchedulerStats schedulerStats,
            TableWriteInfo tableWriteInfo)
    {
        requireNonNull(stageExecutionId, "stageId is null");
        requireNonNull(fragment, "fragment is null");
        requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        requireNonNull(session, "session is null");
        requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(failureDetector, "failureDetector is null");
        requireNonNull(schedulerStats, "schedulerStats is null");
        requireNonNull(tableWriteInfo, "tableWriteInfo is null");

        SqlStageExecution sqlStageExecution = new SqlStageExecution(
                session,
                new StageExecutionStateMachine(stageExecutionId, executor, schedulerStats, !fragment.getTableScanSchedulingOrder().isEmpty()),
                fragment,
                remoteTaskFactory,
                nodeTaskMap,
                summarizeTaskInfo,
                executor,
                failureDetector,
                getMaxFailedTaskPercentage(session),
                tableWriteInfo);
        sqlStageExecution.initialize();
        return sqlStageExecution;
    }

    private SqlStageExecution(
            Session session,
            StageExecutionStateMachine stateMachine,
            PlanFragment planFragment,
            RemoteTaskFactory remoteTaskFactory,
            NodeTaskMap nodeTaskMap,
            boolean summarizeTaskInfo,
            Executor executor,
            FailureDetector failureDetector,
            double maxFailedTaskPercentage,
            TableWriteInfo tableWriteInfo)
    {
        this.session = requireNonNull(session, "session is null");
        this.stateMachine = stateMachine;
        this.planFragment = requireNonNull(planFragment, "planFragment is null");
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.executor = requireNonNull(executor, "executor is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.tableWriteInfo = requireNonNull(tableWriteInfo);
        this.maxFailedTaskPercentage = maxFailedTaskPercentage;

        ImmutableMap.Builder<PlanFragmentId, RemoteSourceNode> fragmentToExchangeSource = ImmutableMap.builder();
        for (RemoteSourceNode remoteSourceNode : planFragment.getRemoteSourceNodes()) {
            for (PlanFragmentId planFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                fragmentToExchangeSource.put(planFragmentId, remoteSourceNode);
            }
        }
        this.exchangeSources = fragmentToExchangeSource.build();
        this.totalLifespans = planFragment.getStageExecutionDescriptor().getTotalLifespans();
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        stateMachine.addStateChangeListener(newState -> checkAllTaskFinal());
        completedLifespansChangeListeners.addListener(lifespans -> finishedLifespans.addAll(lifespans));
    }

    public StageExecutionId getStageExecutionId()
    {
        return stateMachine.getStageExecutionId();
    }

    public StageExecutionState getState()
    {
        return stateMachine.getState();
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor.
     */
    public void addStateChangeListener(StateChangeListener<StageExecutionState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    /**
     * Add a listener for the final stage info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addFinalStageInfoListener(StateChangeListener<StageExecutionInfo> stateChangeListener)
    {
        stateMachine.addFinalStageInfoListener(stateChangeListener);
    }

    public void addCompletedDriverGroupsChangedListener(Consumer<Set<Lifespan>> newlyCompletedDriverGroupConsumer)
    {
        completedLifespansChangeListeners.addListener(newlyCompletedDriverGroupConsumer);
    }

    public synchronized void registerStageTaskRecoveryCallback(StageTaskRecoveryCallback stageTaskRecoveryCallback)
    {
        checkState(!this.stageTaskRecoveryCallback.isPresent(), "stageTaskRecoveryCallback should be registered only once");
        this.stageTaskRecoveryCallback = Optional.of(requireNonNull(stageTaskRecoveryCallback, "stageTaskRecoveryCallback is null"));
    }

    public PlanFragment getFragment()
    {
        return planFragment;
    }

    public OutputBuffers getOutputBuffers()
    {
        return outputBuffers.get();
    }

    public void beginScheduling()
    {
        stateMachine.transitionToScheduling();
    }

    public synchronized void transitionToFinishedTaskScheduling()
    {
        stateMachine.transitionToFinishedTaskScheduling();
    }

    public synchronized void transitionToSchedulingSplits()
    {
        stateMachine.transitionToSchedulingSplits();
    }

    public synchronized void schedulingComplete()
    {
        if (!stateMachine.transitionToScheduled()) {
            return;
        }

        if (getAllTasks().stream().anyMatch(task -> getState() == StageExecutionState.RUNNING)) {
            stateMachine.transitionToRunning();
        }
        if (finishedTasks.size() == allTasks.size()) {
            stateMachine.transitionToFinished();
        }

        for (PlanNodeId tableScanPlanNodeId : planFragment.getTableScanSchedulingOrder()) {
            schedulingComplete(tableScanPlanNodeId);
        }
    }

    public synchronized void schedulingComplete(PlanNodeId partitionedSource)
    {
        for (RemoteTask task : getAllTasks()) {
            task.noMoreSplits(partitionedSource);
        }
        completeSources.add(partitionedSource);
    }

    public synchronized void cancel()
    {
        stateMachine.transitionToCanceled();
        getAllTasks().forEach(RemoteTask::cancel);
    }

    public synchronized void abort()
    {
        stateMachine.transitionToAborted();
        getAllTasks().forEach(RemoteTask::abort);
    }

    public long getUserMemoryReservation()
    {
        return stateMachine.getUserMemoryReservation();
    }

    public long getTotalMemoryReservation()
    {
        return stateMachine.getTotalMemoryReservation();
    }

    public synchronized Duration getTotalCpuTime()
    {
        long millis = getAllTasks().stream()
                .mapToLong(task -> NANOSECONDS.toMillis(task.getTaskInfo().getStats().getTotalCpuTimeInNanos()))
                .sum();
        return new Duration(millis, TimeUnit.MILLISECONDS);
    }

    public synchronized DataSize getRawInputDataSize()
    {
        if (planFragment.getTableScanSchedulingOrder().isEmpty()) {
            return new DataSize(0, BYTE);
        }
        long datasize = getAllTasks().stream()
                .mapToLong(task -> task.getTaskInfo().getStats().getRawInputDataSizeInBytes())
                .sum();
        return DataSize.succinctBytes(datasize);
    }

    public BasicStageExecutionStats getBasicStageStats()
    {
        return stateMachine.getBasicStageStats(this::getAllTaskInfo);
    }

    public StageExecutionInfo getStageExecutionInfo()
    {
        return stateMachine.getStageExecutionInfo(this::getAllTaskInfo, finishedLifespans.size(), totalLifespans);
    }

    private Iterable<TaskInfo> getAllTaskInfo()
    {
        return getAllTasks().stream()
                .map(RemoteTask::getTaskInfo)
                .collect(toImmutableList());
    }

    public synchronized void addExchangeLocations(PlanFragmentId fragmentId, Set<RemoteTask> sourceTasks, boolean noMoreExchangeLocations)
    {
        requireNonNull(fragmentId, "fragmentId is null");
        requireNonNull(sourceTasks, "sourceTasks is null");

        RemoteSourceNode remoteSource = exchangeSources.get(fragmentId);
        checkArgument(remoteSource != null, "Unknown remote source %s. Known sources are %s", fragmentId, exchangeSources.keySet());

        this.sourceTasks.putAll(remoteSource.getId(), sourceTasks);

        for (RemoteTask task : getAllTasks()) {
            ImmutableMultimap.Builder<PlanNodeId, Split> newSplits = ImmutableMultimap.builder();
            for (RemoteTask sourceTask : sourceTasks) {
                TaskStatus sourceTaskStatus = sourceTask.getTaskStatus();
                newSplits.put(remoteSource.getId(), createRemoteSplitFor(task.getTaskId(), sourceTask.getRemoteTaskLocation(), sourceTask.getTaskId()));
            }
            task.addSplits(newSplits.build());
        }

        if (noMoreExchangeLocations) {
            completeSourceFragments.add(fragmentId);

            // is the source now complete?
            if (completeSourceFragments.containsAll(remoteSource.getSourceFragmentIds())) {
                completeSources.add(remoteSource.getId());
                for (RemoteTask task : getAllTasks()) {
                    task.noMoreSplits(remoteSource.getId());
                }
            }
        }
    }

    public synchronized void setOutputBuffers(OutputBuffers outputBuffers)
    {
        requireNonNull(outputBuffers, "outputBuffers is null");

        while (true) {
            OutputBuffers currentOutputBuffers = this.outputBuffers.get();
            if (currentOutputBuffers != null) {
                if (outputBuffers.getVersion() <= currentOutputBuffers.getVersion()) {
                    return;
                }
                currentOutputBuffers.checkValidTransition(outputBuffers);
            }

            if (this.outputBuffers.compareAndSet(currentOutputBuffers, outputBuffers)) {
                for (RemoteTask task : getAllTasks()) {
                    task.setOutputBuffers(outputBuffers);
                }
                return;
            }
        }
    }

    // do not synchronize
    // this is used for query info building which should be independent of scheduling work
    public boolean hasTasks()
    {
        return !tasks.isEmpty();
    }

    // do not synchronize
    // this is used for query info building which should be independent of scheduling work
    public List<RemoteTask> getAllTasks()
    {
        return tasks.values().stream()
                .flatMap(Set::stream)
                .collect(toImmutableList());
    }

    // We only support removeRemoteSource for single task stage because stages with many tasks introduce coordinator to worker HTTP requests in bursty manner.
    // See https://github.com/prestodb/presto/pull/11065 for a similar issue.
    public void removeRemoteSourceIfSingleTaskStage(TaskId remoteSourceTaskId)
    {
        List<RemoteTask> allTasks = getAllTasks();
        if (allTasks.size() > 1) {
            return;
        }
        getOnlyElement(allTasks).removeRemoteSource(remoteSourceTaskId);
    }

    public synchronized Optional<RemoteTask> scheduleTask(InternalNode node, int partition)
    {
        requireNonNull(node, "node is null");

        if (stateMachine.getState().isDone()) {
            return Optional.empty();
        }
        checkState(!splitsScheduled.get(), "scheduleTask can not be called once splits have been scheduled");
        return Optional.of(scheduleTask(node, new TaskId(stateMachine.getStageExecutionId(), partition), ImmutableMultimap.of()));
    }

    public synchronized Set<RemoteTask> scheduleSplits(InternalNode node, Multimap<PlanNodeId, Split> splits, Multimap<PlanNodeId, Lifespan> noMoreSplitsNotification)
    {
        requireNonNull(node, "node is null");
        requireNonNull(splits, "splits is null");

        if (stateMachine.getState().isDone()) {
            return ImmutableSet.of();
        }
        splitsScheduled.set(true);

        checkArgument(planFragment.getTableScanSchedulingOrder().containsAll(splits.keySet()), "Invalid splits");

        ImmutableSet.Builder<RemoteTask> newTasks = ImmutableSet.builder();
        Collection<RemoteTask> tasks = this.tasks.get(node);
        RemoteTask task;
        if (tasks == null) {
            // The output buffer depends on the task id starting from 0 and being sequential, since each
            // task is assigned a private buffer based on task id.
            TaskId taskId = new TaskId(stateMachine.getStageExecutionId(), nextTaskId.getAndIncrement());
            task = scheduleTask(node, taskId, splits);
            newTasks.add(task);
        }
        else {
            task = tasks.iterator().next();
            task.addSplits(splits);
        }
        if (noMoreSplitsNotification.size() > 1) {
            // The assumption that `noMoreSplitsNotification.size() <= 1` currently holds.
            // If this assumption no longer holds, we should consider calling task.noMoreSplits with multiple entries in one shot.
            // These kind of methods can be expensive since they are grabbing locks and/or sending HTTP requests on change.
            throw new UnsupportedOperationException("This assumption no longer holds: noMoreSplitsNotification.size() < 1");
        }
        for (Entry<PlanNodeId, Lifespan> entry : noMoreSplitsNotification.entries()) {
            task.noMoreSplits(entry.getKey(), entry.getValue());
        }
        return newTasks.build();
    }

    private synchronized RemoteTask scheduleTask(InternalNode node, TaskId taskId, Multimap<PlanNodeId, Split> sourceSplits)
    {
        checkArgument(!allTasks.contains(taskId), "A task with id %s already exists", taskId);

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        initialSplits.putAll(sourceSplits);

        sourceTasks.forEach((planNodeId, task) -> {
            TaskStatus status = task.getTaskStatus();
            if (status.getState() != TaskState.FINISHED) {
                initialSplits.put(planNodeId, createRemoteSplitFor(taskId, task.getRemoteTaskLocation(), task.getTaskId()));
            }
        });

        OutputBuffers outputBuffers = this.outputBuffers.get();
        checkState(outputBuffers != null, "Initial output buffers must be set before a task can be scheduled");

        RemoteTask task = remoteTaskFactory.createRemoteTask(
                session,
                taskId,
                node,
                planFragment,
                initialSplits.build(),
                outputBuffers,
                nodeTaskMap.createPartitionedSplitCountTracker(node, taskId),
                summarizeTaskInfo,
                tableWriteInfo);

        completeSources.forEach(task::noMoreSplits);

        allTasks.add(taskId);
        tasks.computeIfAbsent(node, key -> newConcurrentHashSet()).add(task);
        nodeTaskMap.addTask(node, task);

        task.addStateChangeListener(new StageTaskListener(taskId));
        task.addFinalTaskInfoListener(this::updateFinalTaskInfo);

        if (!stateMachine.getState().isDone()) {
            task.start();
        }
        else {
            // stage finished while we were scheduling this task
            task.abort();
        }

        return task;
    }

    public Set<InternalNode> getScheduledNodes()
    {
        return ImmutableSet.copyOf(tasks.keySet());
    }

    public void recordGetSplitTime(long start)
    {
        stateMachine.recordGetSplitTime(start);
    }

    private static Split createRemoteSplitFor(TaskId taskId, URI remoteSourceTaskLocation, TaskId remoteSourceTaskId)
    {
        // Fetch the results from the buffer assigned to the task based on id
        String splitLocation = remoteSourceTaskLocation.toASCIIString() + "/results/" + taskId.getId();
        return new Split(REMOTE_CONNECTOR_ID, new RemoteTransactionHandle(), new RemoteSplit(new Location(splitLocation), remoteSourceTaskId));
    }

    private void updateTaskStatus(TaskId taskId, TaskStatus taskStatus)
    {
        try {
            StageExecutionState stageExecutionState = getState();
            if (stageExecutionState.isDone()) {
                return;
            }

            TaskState taskState = taskStatus.getState();
            if (taskState == TaskState.FAILED) {
                // no matter if it is possible to recover - the task is failed
                failedTasks.add(taskId);

                RuntimeException failure = taskStatus.getFailures().stream()
                        .findFirst()
                        .map(this::rewriteTransportFailure)
                        .map(ExecutionFailureInfo::toException)
                        .orElse(new PrestoException(GENERIC_INTERNAL_ERROR, "A task failed for an unknown reason"));
                if (isRecoverable(taskStatus.getFailures())) {
                    try {
                        stageTaskRecoveryCallback.get().recover(taskId);
                        finishedTasks.add(taskId);
                    }
                    catch (Throwable t) {
                        // In an ideal world, this exception is not supposed to happen.
                        // However, it could happen, for example, if connector throws exception.
                        // We need to handle the exception in order to fail the query properly, otherwise the failed task will hang in RUNNING/SCHEDULING state.
                        failure.addSuppressed(new PrestoException(GENERIC_RECOVERY_ERROR, format("Encountered error when trying to recover task %s", taskId), t));
                        stateMachine.transitionToFailed(failure);
                    }
                }
                else {
                    stateMachine.transitionToFailed(failure);
                }
            }
            else if (taskState == TaskState.ABORTED) {
                // A task should only be in the aborted state if the STAGE is done (ABORTED or FAILED)
                stateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "A task is in the ABORTED state but stage is " + stageExecutionState));
            }
            else if (taskState == TaskState.FINISHED) {
                finishedTasks.add(taskId);
            }

            // The finishedTasks.add(taskStatus.getTaskId()) must happen before the getState() (see schedulingComplete)
            stageExecutionState = getState();
            if (stageExecutionState == StageExecutionState.SCHEDULED || stageExecutionState == StageExecutionState.RUNNING) {
                if (taskState == TaskState.RUNNING) {
                    stateMachine.transitionToRunning();
                }
                if (finishedTasks.size() == allTasks.size()) {
                    stateMachine.transitionToFinished();
                }
            }
        }
        finally {
            // after updating state, check if all tasks have final status information
            checkAllTaskFinal();
        }
    }

    private boolean isRecoverable(List<ExecutionFailureInfo> failures)
    {
        for (ExecutionFailureInfo failure : failures) {
            if (!RECOVERABLE_ERROR_CODES.contains(failure.getErrorCode())) {
                return false;
            }
        }
        return stageTaskRecoveryCallback.isPresent() &&
                failedTasks.size() < allTasks.size() * maxFailedTaskPercentage;
    }

    private synchronized void updateFinalTaskInfo(TaskInfo finalTaskInfo)
    {
        tasksWithFinalInfo.add(finalTaskInfo.getTaskId());
        checkAllTaskFinal();
    }

    private synchronized void checkAllTaskFinal()
    {
        if (stateMachine.getState().isDone() && tasksWithFinalInfo.containsAll(allTasks)) {
            if (getFragment().getStageExecutionDescriptor().isStageGroupedExecution()) {
                // in case stage is CANCELLED/ABORTED/FAILED, number of finished lifespans can be less than total lifespans
                checkState(finishedLifespans.size() <= totalLifespans, format("Number of finished lifespans (%s) exceeds number of total lifespans (%s)", finishedLifespans.size(), totalLifespans));
            }
            else {
                // ungrouped execution will not update finished lifespans
                checkState(finishedLifespans.isEmpty());
            }

            List<TaskInfo> finalTaskInfos = getAllTasks().stream()
                    .map(RemoteTask::getTaskInfo)
                    .collect(toImmutableList());
            stateMachine.setAllTasksFinal(finalTaskInfos, totalLifespans);
        }
    }

    private ExecutionFailureInfo rewriteTransportFailure(ExecutionFailureInfo executionFailureInfo)
    {
        if (executionFailureInfo.getRemoteHost() == null || failureDetector.getState(executionFailureInfo.getRemoteHost()) != GONE) {
            return executionFailureInfo;
        }

        return new ExecutionFailureInfo(
                executionFailureInfo.getType(),
                executionFailureInfo.getMessage(),
                executionFailureInfo.getCause(),
                executionFailureInfo.getSuppressed(),
                executionFailureInfo.getStack(),
                executionFailureInfo.getErrorLocation(),
                REMOTE_HOST_GONE.toErrorCode(),
                executionFailureInfo.getRemoteHost());
    }

    @Override
    public String toString()
    {
        return stateMachine.toString();
    }

    private class StageTaskListener
            implements StateChangeListener<TaskStatus>
    {
        private long previousUserMemory;
        private long previousSystemMemory;
        private final Set<Lifespan> completedDriverGroups = new HashSet<>();
        private final TaskId taskId;

        public StageTaskListener(TaskId taskId)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
        }

        @Override
        public void stateChanged(TaskStatus taskStatus)
        {
            try {
                updateMemoryUsage(taskStatus);
                updateCompletedDriverGroups(taskStatus);
            }
            finally {
                updateTaskStatus(taskId, taskStatus);
            }
        }

        private synchronized void updateMemoryUsage(TaskStatus taskStatus)
        {
            long currentUserMemory = taskStatus.getMemoryReservationInBytes();
            long currentSystemMemory = taskStatus.getSystemMemoryReservationInBytes();
            long deltaUserMemoryInBytes = currentUserMemory - previousUserMemory;
            long deltaTotalMemoryInBytes = (currentUserMemory + currentSystemMemory) - (previousUserMemory + previousSystemMemory);
            previousUserMemory = currentUserMemory;
            previousSystemMemory = currentSystemMemory;
            stateMachine.updateMemoryUsage(deltaUserMemoryInBytes, deltaTotalMemoryInBytes, taskStatus.getPeakNodeTotalMemoryReservationInBytes());
        }

        private synchronized void updateCompletedDriverGroups(TaskStatus taskStatus)
        {
            // Sets.difference returns a view.
            // Once we add the difference into `completedDriverGroups`, the view will be empty.
            // `completedLifespansChangeListeners.invoke` happens asynchronously.
            // As a result, calling the listeners before updating `completedDriverGroups` doesn't make a difference.
            // That's why a copy must be made here.
            Set<Lifespan> newlyCompletedDriverGroups = ImmutableSet.copyOf(Sets.difference(taskStatus.getCompletedDriverGroups(), this.completedDriverGroups));
            if (newlyCompletedDriverGroups.isEmpty()) {
                return;
            }
            completedLifespansChangeListeners.invoke(newlyCompletedDriverGroups, executor);
            // newlyCompletedDriverGroups is a view.
            // Making changes to completedDriverGroups will change newlyCompletedDriverGroups.
            completedDriverGroups.addAll(newlyCompletedDriverGroups);
        }
    }

    @FunctionalInterface
    public interface StageTaskRecoveryCallback
    {
        void recover(TaskId taskId);
    }

    private static class ListenerManager<T>
    {
        private final List<Consumer<T>> listeners = new ArrayList<>();
        private boolean frozen;

        public synchronized void addListener(Consumer<T> listener)
        {
            checkState(!frozen, "Listeners have been invoked");
            listeners.add(listener);
        }

        public synchronized void invoke(T payload, Executor executor)
        {
            frozen = true;
            for (Consumer<T> listener : listeners) {
                executor.execute(() -> listener.accept(payload));
            }
        }
    }
}
