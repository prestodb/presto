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
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.MoreFutures.firstCompletedFuture;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

@ThreadSafe
public final class SqlStageExecution
{
    private final StageStateMachine stateMachine;
    private final RemoteTaskFactory remoteTaskFactory;
    private final NodeTaskMap nodeTaskMap;
    private final boolean summarizeTaskInfo;

    private final Map<PlanFragmentId, RemoteSourceNode> exchangeSources;

    private final Map<Node, Set<RemoteTask>> tasks = new ConcurrentHashMap<>();
    private final AtomicInteger nextTaskId = new AtomicInteger();
    private final Set<TaskId> allTasks = newConcurrentHashSet();
    private final Set<TaskId> finishedTasks = newConcurrentHashSet();
    private final AtomicBoolean splitsScheduled = new AtomicBoolean();

    private final Multimap<PlanNodeId, URI> exchangeLocations = HashMultimap.create();
    private final Set<PlanNodeId> completeSources = newConcurrentHashSet();
    private final Set<PlanFragmentId> completeSourceFragments = newConcurrentHashSet();

    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();

    public SqlStageExecution(
            StageId stageId,
            URI location,
            PlanFragment fragment,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            boolean summarizeTaskInfo,
            NodeTaskMap nodeTaskMap,
            ExecutorService executor)
    {
        this(new StageStateMachine(
                        requireNonNull(stageId, "stageId is null"),
                        requireNonNull(location, "location is null"),
                        requireNonNull(session, "session is null"),
                        requireNonNull(fragment, "fragment is null"),
                        requireNonNull(executor, "executor is null")),
                remoteTaskFactory,
                nodeTaskMap,
                summarizeTaskInfo);
    }

    public SqlStageExecution(StageStateMachine stateMachine, RemoteTaskFactory remoteTaskFactory, NodeTaskMap nodeTaskMap, boolean summarizeTaskInfo)
    {
        this.stateMachine = stateMachine;
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.summarizeTaskInfo = summarizeTaskInfo;

        ImmutableMap.Builder<PlanFragmentId, RemoteSourceNode> fragmentToExchangeSource = ImmutableMap.builder();
        for (RemoteSourceNode remoteSourceNode : stateMachine.getFragment().getRemoteSourceNodes()) {
            for (PlanFragmentId planFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                fragmentToExchangeSource.put(planFragmentId, remoteSourceNode);
            }
        }
        this.exchangeSources = fragmentToExchangeSource.build();
    }

    public StageId getStageId()
    {
        return stateMachine.getStageId();
    }

    public StageState getState()
    {
        return stateMachine.getState();
    }

    public void addStateChangeListener(StateChangeListener<StageState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener::stateChanged);
    }

    public PlanFragment getFragment()
    {
        return stateMachine.getFragment();
    }

    public void beginScheduling()
    {
        stateMachine.transitionToScheduling();
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

        if (getAllTasks().stream().anyMatch(task -> getState() == StageState.RUNNING)) {
            stateMachine.transitionToRunning();
        }
        if (finishedTasks.containsAll(allTasks)) {
            stateMachine.transitionToFinished();
        }

        for (PlanNodeId partitionedSource : stateMachine.getFragment().getPartitionedSources()) {
            for (RemoteTask task : getAllTasks()) {
                task.noMoreSplits(partitionedSource);
            }
            completeSources.add(partitionedSource);
        }
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

    public long getMemoryReservation()
    {
        return stateMachine.getMemoryReservation();
    }

    public synchronized Duration getTotalCpuTime()
    {
        long millis = getAllTasks().stream()
                .mapToLong(task -> task.getTaskInfo().getStats().getTotalCpuTime().toMillis())
                .sum();
        return new Duration(millis, TimeUnit.MILLISECONDS);
    }

    public StageInfo getStageInfo()
    {
        return stateMachine.getStageInfo(
                () -> getAllTasks().stream()
                        .map(RemoteTask::getTaskInfo)
                        .collect(toImmutableList()),
                ImmutableList::of);
    }

    public synchronized void addExchangeLocations(PlanFragmentId fragmentId, Set<URI> exchangeLocations, boolean noMoreExchangeLocations)
    {
        requireNonNull(fragmentId, "fragmentId is null");
        requireNonNull(exchangeLocations, "exchangeLocations is null");

        RemoteSourceNode remoteSource = exchangeSources.get(fragmentId);
        checkArgument(remoteSource != null, "Unknown remote source %s. Known sources are %s", fragmentId, exchangeSources.keySet());

        this.exchangeLocations.putAll(remoteSource.getId(), exchangeLocations);

        for (RemoteTask task : getAllTasks()) {
            ImmutableMultimap.Builder<PlanNodeId, Split> newSplits = ImmutableMultimap.builder();
            for (URI exchangeLocation : exchangeLocations) {
                newSplits.put(remoteSource.getId(), createRemoteSplitFor(task.getTaskId(), exchangeLocation));
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

    public synchronized CompletableFuture<?> getTaskStateChange()
    {
        List<RemoteTask> allTasks = getAllTasks();
        if (allTasks.isEmpty()) {
            return completedFuture(null);
        }

        List<CompletableFuture<TaskStatus>> stateChangeFutures = allTasks.stream()
                .map(task -> task.getStateChange(task.getTaskStatus()))
                .collect(toImmutableList());

        return firstCompletedFuture(stateChangeFutures, true);
    }

    public synchronized RemoteTask scheduleTask(Node node, int partition)
    {
        requireNonNull(node, "node is null");

        checkState(!splitsScheduled.get(), "scheduleTask can not be called once splits have been scheduled");
        return scheduleTask(node, new TaskId(stateMachine.getStageId(), partition), ImmutableMultimap.of());
    }

    public synchronized Set<RemoteTask> scheduleSplits(Node node, Multimap<PlanNodeId, Split> splits)
    {
        requireNonNull(node, "node is null");
        requireNonNull(splits, "splits is null");

        splitsScheduled.set(true);

        checkArgument(stateMachine.getFragment().getPartitionedSources().containsAll(splits.keySet()), "Invalid splits");

        ImmutableSet.Builder<RemoteTask> newTasks = ImmutableSet.builder();
        Collection<RemoteTask> tasks = this.tasks.get(node);
        if (tasks == null) {
            // The output buffer depends on the task id starting from 0 and being sequential, since each
            // task is assigned a private buffer based on task id.
            TaskId taskId = new TaskId(stateMachine.getStageId(), nextTaskId.getAndIncrement());
            newTasks.add(scheduleTask(node, taskId, splits));
        }
        else {
            RemoteTask task = tasks.iterator().next();
            task.addSplits(splits);
        }
        return newTasks.build();
    }

    private synchronized RemoteTask scheduleTask(Node node, TaskId taskId, Multimap<PlanNodeId, Split> sourceSplits)
    {
        checkArgument(!allTasks.contains(taskId), "A task with id %s already exists", taskId);

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        initialSplits.putAll(sourceSplits);
        for (Entry<PlanNodeId, URI> entry : exchangeLocations.entries()) {
            initialSplits.put(entry.getKey(), createRemoteSplitFor(taskId, entry.getValue()));
        }

        OutputBuffers outputBuffers = this.outputBuffers.get();
        checkState(outputBuffers != null, "Initial output buffers must be set before a task can be scheduled");

        RemoteTask task = remoteTaskFactory.createRemoteTask(
                stateMachine.getSession(),
                taskId,
                node,
                stateMachine.getFragment(),
                initialSplits.build(),
                outputBuffers,
                nodeTaskMap.createPartitionedSplitCountTracker(node, taskId),
                summarizeTaskInfo);

        completeSources.forEach(task::noMoreSplits);

        allTasks.add(taskId);
        tasks.computeIfAbsent(node, key -> newConcurrentHashSet()).add(task);
        nodeTaskMap.addTask(node, task);

        task.addStateChangeListener(new StageTaskListener());

        if (!stateMachine.getState().isDone()) {
            task.start();
        }
        else {
            // stage finished while we were scheduling this task
            task.abort();
        }

        return task;
    }

    public Set<Node> getScheduledNodes()
    {
        return ImmutableSet.copyOf(tasks.keySet());
    }

    public void recordGetSplitTime(long start)
    {
        stateMachine.recordGetSplitTime(start);
    }

    private static Split createRemoteSplitFor(TaskId taskId, URI taskLocation)
    {
        // Fetch the results from the buffer assigned to the task based on id
        URI splitLocation = uriBuilderFrom(taskLocation).appendPath("results").appendPath(String.valueOf(taskId.getId())).build();
        return new Split(REMOTE_CONNECTOR_ID, new RemoteTransactionHandle(), new RemoteSplit(splitLocation));
    }

    @Override
    public String toString()
    {
        return stateMachine.toString();
    }

    private class StageTaskListener
            implements StateChangeListener<TaskStatus>
    {
        private long previousMemory;

        @Override
        public void stateChanged(TaskStatus taskStatus)
        {
            updateMemoryUsage(taskStatus);

            StageState stageState = getState();
            if (stageState.isDone()) {
                return;
            }

            TaskState taskState = taskStatus.getState();
            if (taskState == TaskState.FAILED) {
                RuntimeException failure = taskStatus.getFailures().stream()
                        .findFirst()
                        .map(ExecutionFailureInfo::toException)
                        .orElse(new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "A task failed for an unknown reason"));
                stateMachine.transitionToFailed(failure);
            }
            else if (taskState == TaskState.ABORTED) {
                // A task should only be in the aborted state if the STAGE is done (ABORTED or FAILED)
                stateMachine.transitionToFailed(new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "A task is in the ABORTED state but stage is " + stageState));
            }
            else if (taskState == TaskState.FINISHED) {
                finishedTasks.add(taskStatus.getTaskId());
            }

            if (stageState == StageState.SCHEDULED || stageState == StageState.RUNNING) {
                if (taskState == TaskState.RUNNING) {
                    stateMachine.transitionToRunning();
                }
                if (finishedTasks.containsAll(allTasks)) {
                    stateMachine.transitionToFinished();
                }
            }
        }

        private synchronized void updateMemoryUsage(TaskStatus taskStatus)
        {
            long currentMemory = taskStatus.getMemoryReservation().toBytes();
            long deltaMemoryInBytes = currentMemory - previousMemory;
            previousMemory = currentMemory;
            stateMachine.updateMemoryUsage(deltaMemoryInBytes);
        }
    }
}
