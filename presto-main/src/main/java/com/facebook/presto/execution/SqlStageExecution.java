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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
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

    private final Map<PlanFragmentId, RemoteSourceNode> exchangeSources;

    private final Map<Node, Set<RemoteTask>> tasks = new ConcurrentHashMap<>();
    private final AtomicInteger nextTaskId = new AtomicInteger();
    private final Set<TaskId> allTasks = newConcurrentHashSet();
    private final Set<TaskId> finishedTasks = newConcurrentHashSet();

    private final Multimap<PlanNodeId, URI> exchangeLocations = HashMultimap.create();
    private final Set<PlanNodeId> completeSources = newConcurrentHashSet();
    private final Set<PlanFragmentId> completeSourceFragments = newConcurrentHashSet();

    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>(INITIAL_EMPTY_OUTPUT_BUFFERS);

    public SqlStageExecution(
            StageId stageId,
            URI location,
            PlanFragment fragment,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
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
                nodeTaskMap);
    }

    public SqlStageExecution(StageStateMachine stateMachine, RemoteTaskFactory remoteTaskFactory, NodeTaskMap nodeTaskMap)
    {
        this.stateMachine = stateMachine;
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");

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

        PlanNodeId partitionedSource = stateMachine.getFragment().getPartitionedSource();
        if (partitionedSource != null) {
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

    public synchronized long getMemoryReservation()
    {
        return getAllTasks().stream()
                .mapToLong(task -> task.getTaskInfo().getStats().getMemoryReservation().toBytes())
                .sum();
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
                newSplits.put(remoteSource.getId(), createRemoteSplitFor(task.getTaskInfo().getTaskId(), exchangeLocation));
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
            if (outputBuffers.getVersion() <= currentOutputBuffers.getVersion()) {
                return;
            }

            currentOutputBuffers.checkValidTransition(outputBuffers);

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

        List<CompletableFuture<TaskInfo>> stateChangeFutures = allTasks.stream()
                .map(task -> task.getStateChange(task.getTaskInfo()))
                .collect(toImmutableList());

        return firstCompletedFuture(stateChangeFutures, true);
    }

    public synchronized RemoteTask scheduleTask(Node node, int partition)
    {
        requireNonNull(node, "node is null");

        return scheduleTask(node, partition, null, ImmutableList.<Split>of());
    }

    public synchronized Set<RemoteTask> scheduleSplits(Node node, int partition, Iterable<Split> splits)
    {
        requireNonNull(node, "node is null");
        requireNonNull(splits, "splits is null");

        PlanNodeId partitionedSource = stateMachine.getFragment().getPartitionedSource();
        checkState(partitionedSource != null, "Partitioned source is null");

        ImmutableSet.Builder<RemoteTask> newTasks = ImmutableSet.builder();
        Collection<RemoteTask> tasks = this.tasks.get(node);
        if (tasks == null) {
            newTasks.add(scheduleTask(node, partition, partitionedSource, splits));
        }
        else {
            RemoteTask task = tasks.iterator().next();
            task.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(partitionedSource, splits)
                    .build());
        }
        return newTasks.build();
    }

    private synchronized RemoteTask scheduleTask(Node node, int partition, PlanNodeId sourceId, Iterable<Split> sourceSplits)
    {
        TaskId taskId = new TaskId(stateMachine.getStageId(), String.valueOf(nextTaskId.getAndIncrement()));

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        for (Split sourceSplit : sourceSplits) {
            initialSplits.put(sourceId, sourceSplit);
        }
        for (Entry<PlanNodeId, URI> entry : exchangeLocations.entries()) {
            initialSplits.put(entry.getKey(), createRemoteSplitFor(taskId, entry.getValue()));
        }

        RemoteTask task = remoteTaskFactory.createRemoteTask(
                stateMachine.getSession(),
                taskId,
                node,
                partition,
                stateMachine.getFragment(),
                initialSplits.build(),
                outputBuffers.get(),
                nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));

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
        URI splitLocation = uriBuilderFrom(taskLocation).appendPath("results").appendPath(taskId.toString()).build();
        return new Split("remote", new RemoteTransactionHandle(), new RemoteSplit(splitLocation));
    }

    @Override
    public String toString()
    {
        return stateMachine.toString();
    }

    private class StageTaskListener
            implements StateChangeListener<TaskInfo>
    {
        private long previousMemory;

        @Override
        public void stateChanged(TaskInfo taskInfo)
        {
            updateMemoryUsage(taskInfo);

            StageState stageState = getState();
            if (stageState.isDone()) {
                return;
            }

            TaskState taskState = taskInfo.getState();
            if (taskState == TaskState.FAILED) {
                RuntimeException failure = taskInfo.getFailures().stream()
                        .findFirst()
                        .map(ExecutionFailureInfo::toException)
                        .orElse(new PrestoException(StandardErrorCode.INTERNAL_ERROR, "A task failed for an unknown reason"));
                stateMachine.transitionToFailed(failure);
            }
            else if (taskState == TaskState.ABORTED) {
                // A task should only be in the aborted state if the STAGE is done (ABORTED or FAILED)
                stateMachine.transitionToFailed(new PrestoException(StandardErrorCode.INTERNAL_ERROR, "A task is in the ABORTED state but stage is " + stageState));
            }
            else if (taskState == TaskState.FINISHED) {
                finishedTasks.add(taskInfo.getTaskId());
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

        private synchronized void updateMemoryUsage(TaskInfo taskInfo)
        {
            long currentMemory = taskInfo.getStats().getMemoryReservation().toBytes();
            long deltaMemoryInBytes = currentMemory - previousMemory;
            previousMemory = currentMemory;
            stateMachine.updateMemoryUsage(deltaMemoryInBytes);
        }
    }
}
