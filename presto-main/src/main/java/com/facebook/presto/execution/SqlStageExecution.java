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

import com.facebook.presto.HashPagePartitionFunction;
import com.facebook.presto.OutputBuffers;
import com.facebook.presto.PagePartitionFunction;
import com.facebook.presto.PartitionedPagePartitionFunction;
import com.facebook.presto.Session;
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.execution.NodeScheduler.NodeSelector;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.airlift.concurrent.SetThreadName;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

@ThreadSafe
public final class SqlStageExecution
{
    private final PlanFragment fragment;
    private final Set<PlanNodeId> allSources;
    private final Map<PlanFragmentId, SqlStageExecution> subStages;

    private final Multimap<Node, TaskId> localNodeTaskMap = HashMultimap.create();
    private final ConcurrentMap<TaskId, RemoteTask> tasks = new ConcurrentHashMap<>();

    private final Optional<SplitSource> dataSource;
    private final RemoteTaskFactory remoteTaskFactory;
    private final int splitBatchSize;

    private final int initialHashPartitions;

    private final StageStateMachine stateMachine;

    private final Set<PlanNodeId> completeSources = newConcurrentHashSet();

    @GuardedBy("this")
    private OutputBuffers currentOutputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS;
    @GuardedBy("this")
    private OutputBuffers nextOutputBuffers;

    private final ExecutorService executor;

    private final NodeSelector nodeSelector;
    private final NodeTaskMap nodeTaskMap;

    // Note: atomic is needed to assure thread safety between constructor and scheduler thread
    private final AtomicReference<Multimap<PlanNodeId, URI>> exchangeLocations = new AtomicReference<>(ImmutableMultimap.<PlanNodeId, URI>of());

    public SqlStageExecution(QueryId queryId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int splitBatchSize,
            int initialHashPartitions,
            ExecutorService executor,
            NodeTaskMap nodeTaskMap,
            OutputBuffers nextOutputBuffers)
    {
        this(
                queryId,
                new AtomicInteger(),
                locationFactory,
                plan,
                nodeScheduler,
                remoteTaskFactory,
                session,
                splitBatchSize,
                initialHashPartitions,
                executor,
                nodeTaskMap);

        // add a single output buffer
        this.nextOutputBuffers = nextOutputBuffers;
    }

    private SqlStageExecution(
            QueryId queryId,
            AtomicInteger nextStageId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int splitBatchSize,
            int initialHashPartitions,
            ExecutorService executor,
            NodeTaskMap nodeTaskMap)
    {
        checkNotNull(queryId, "queryId is null");
        checkNotNull(nextStageId, "nextStageId is null");
        checkNotNull(locationFactory, "locationFactory is null");
        checkNotNull(plan, "plan is null");
        checkNotNull(nodeScheduler, "nodeScheduler is null");
        checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        checkNotNull(session, "session is null");
        checkArgument(initialHashPartitions > 0, "initialHashPartitions must be greater than 0");
        checkNotNull(executor, "executor is null");
        checkNotNull(nodeTaskMap, "nodeTaskMap is null");

        StageId stageId = new StageId(queryId, String.valueOf(nextStageId.getAndIncrement()));
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            this.fragment = plan.getFragment();
            this.dataSource = plan.getDataSource();
            this.remoteTaskFactory = remoteTaskFactory;
            this.splitBatchSize = splitBatchSize;
            this.initialHashPartitions = initialHashPartitions;
            this.executor = executor;

            this.allSources = Stream.concat(
                    Stream.of(plan.getFragment().getPartitionedSource()),
                    plan.getFragment().getRemoteSourceNodes().stream()
                            .map(RemoteSourceNode::getId))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

            ImmutableMap.Builder<PlanFragmentId, SqlStageExecution> subStages = ImmutableMap.builder();
            for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
                PlanFragmentId subStageFragmentId = subStagePlan.getFragment().getId();
                SqlStageExecution subStage = new SqlStageExecution(
                        queryId,
                        nextStageId,
                        locationFactory,
                        subStagePlan,
                        nodeScheduler,
                        remoteTaskFactory,
                        session,
                        splitBatchSize,
                        initialHashPartitions,
                        executor,
                        nodeTaskMap);

                subStages.put(subStageFragmentId, subStage);
            }
            this.subStages = subStages.build();

            String dataSourceName = dataSource.isPresent() ? dataSource.get().getDataSourceName() : null;
            this.nodeSelector = nodeScheduler.createNodeSelector(dataSourceName);
            this.nodeTaskMap = nodeTaskMap;
            this.stateMachine = new StageStateMachine(stageId, locationFactory.createStageLocation(stageId), session, plan.getFragment(), executor);
        }
    }

    public void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            if (stageId.equals(stateMachine.getStageId())) {
                cancel();
            }
            else {
                for (SqlStageExecution subStage : subStages.values()) {
                    subStage.cancelStage(stageId);
                }
            }
        }
    }

    public StageState getState()
    {
        return stateMachine.getState();
    }

    public long getTotalMemoryReservation()
    {
        long memory = 0;
        for (RemoteTask task : tasks.values()) {
            memory += task.getTaskInfo().getStats().getMemoryReservation().toBytes();
        }
        for (SqlStageExecution subStage : subStages.values()) {
            memory += subStage.getTotalMemoryReservation();
        }
        return memory;
    }

    public StageInfo getStageInfo()
    {
        return stateMachine.getStageInfo(
                () -> tasks.values().stream()
                        .map(RemoteTask::getTaskInfo)
                        .collect(toImmutableList()),
                () -> subStages.values().stream()
                        .map(SqlStageExecution::getStageInfo)
                        .collect(toImmutableList()));
    }

    public Collection<SqlStageExecution> getSubStages()
    {
        return subStages.values();
    }

    private synchronized void parentTasksAdded(List<TaskId> parentTasks, boolean noMoreParentNodes)
    {
        checkNotNull(parentTasks, "parentTasks is null");

        // get the current buffers
        OutputBuffers startingOutputBuffers = nextOutputBuffers != null ? nextOutputBuffers : currentOutputBuffers;

        // add new buffers
        OutputBuffers newOutputBuffers;
        if (fragment.getOutputPartitioning() == OutputPartitioning.NONE) {
            ImmutableMap.Builder<TaskId, PagePartitionFunction> newBuffers = ImmutableMap.builder();
            for (TaskId taskId : parentTasks) {
                newBuffers.put(taskId, new UnpartitionedPagePartitionFunction());
            }
            newOutputBuffers = startingOutputBuffers.withBuffers(newBuffers.build());

            // no more flag
            if (noMoreParentNodes) {
                newOutputBuffers = newOutputBuffers.withNoMoreBufferIds();
            }
        }
        else if (fragment.getOutputPartitioning() == OutputPartitioning.HASH) {
            checkArgument(noMoreParentNodes, "Hash partitioned output requires all parent nodes be added in a single call");

            ImmutableMap.Builder<TaskId, PagePartitionFunction> buffers = ImmutableMap.builder();
            for (int nodeIndex = 0; nodeIndex < parentTasks.size(); nodeIndex++) {
                TaskId taskId = parentTasks.get(nodeIndex);
                buffers.put(taskId, new HashPagePartitionFunction(nodeIndex, parentTasks.size(), getPartitioningChannels(fragment).get(), getHashChannel(fragment), fragment.getTypes()));
            }

            newOutputBuffers = startingOutputBuffers
                    .withBuffers(buffers.build())
                    .withNoMoreBufferIds();
        }
        else if (fragment.getOutputPartitioning() == OutputPartitioning.ROUND_ROBIN) {
            checkArgument(noMoreParentNodes, "Round-robin partitioned output requires all parent nodes be added in a single call");

            ImmutableMap.Builder<TaskId, PagePartitionFunction> buffers = ImmutableMap.builder();
            for (int nodeIndex = 0; nodeIndex < parentTasks.size(); nodeIndex++) {
                TaskId taskId = parentTasks.get(nodeIndex);
                buffers.put(taskId, new PartitionedPagePartitionFunction(nodeIndex, parentTasks.size()));
            }

            newOutputBuffers = startingOutputBuffers
                    .withBuffers(buffers.build())
                    .withNoMoreBufferIds();
        }
        else {
            throw new UnsupportedOperationException("Unsupported output partitioning " + fragment.getOutputPartitioning());
        }

        // only notify scheduler and tasks if the buffers changed
        if (newOutputBuffers.getVersion() != startingOutputBuffers.getVersion()) {
            this.nextOutputBuffers = newOutputBuffers;
            this.notifyAll();
        }
    }

    private synchronized OutputBuffers getCurrentOutputBuffers()
    {
        return currentOutputBuffers;
    }

    private synchronized OutputBuffers updateToNextOutputBuffers()
    {
        if (nextOutputBuffers == null) {
            return currentOutputBuffers;
        }

        currentOutputBuffers = nextOutputBuffers;
        nextOutputBuffers = null;
        this.notifyAll();
        return currentOutputBuffers;
    }

    public void addStateChangeListener(StateChangeListener<StageState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener::stateChanged);
    }

    private Multimap<PlanNodeId, URI> getNewExchangeLocations()
    {
        Multimap<PlanNodeId, URI> exchangeLocations = this.exchangeLocations.get();

        ImmutableMultimap.Builder<PlanNodeId, URI> newExchangeLocations = ImmutableMultimap.builder();
        for (RemoteSourceNode remoteSourceNode : fragment.getRemoteSourceNodes()) {
            for (PlanFragmentId planFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                SqlStageExecution subStage = subStages.get(planFragmentId);
                checkState(subStage != null, "Unknown sub stage %s, known stages %s", planFragmentId, subStages.keySet());

                // add new task locations
                for (URI taskLocation : subStage.getTaskLocations()) {
                    if (!exchangeLocations.containsEntry(remoteSourceNode.getId(), taskLocation)) {
                        newExchangeLocations.putAll(remoteSourceNode.getId(), taskLocation);
                    }
                }
            }
        }
        return newExchangeLocations.build();
    }

    private synchronized List<URI> getTaskLocations()
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stateMachine.getStageId())) {
            ImmutableList.Builder<URI> locations = ImmutableList.builder();
            for (RemoteTask task : tasks.values()) {
                locations.add(task.getTaskInfo().getSelf());
            }
            return locations.build();
        }
    }

    @VisibleForTesting
    public List<RemoteTask> getAllTasks()
    {
        return ImmutableList.copyOf(tasks.values());
    }

    @VisibleForTesting
    public List<RemoteTask> getTasks(Node node)
    {
        return FluentIterable.from(localNodeTaskMap.get(node)).transform(Functions.forMap(tasks)).toList();
    }

    public Future<?> start()
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stateMachine.getStageId())) {
            return scheduleStartTasks();
        }
    }

    private Future<?> scheduleStartTasks()
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stateMachine.getStageId())) {
            // start sub-stages (starts bottom-up)
            subStages.values().forEach(SqlStageExecution::scheduleStartTasks);
            return executor.submit(this::startTasks);
        }
    }

    private void startTasks()
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stateMachine.getStageId())) {
            try {
                checkState(!Thread.holdsLock(this), "Can not start while holding a lock on this");

                // transition to scheduling
                if (!stateMachine.transitionToScheduling()) {
                    // stage has already been started, has been canceled or has no tasks due to partition pruning
                    return;
                }

                // schedule tasks
                if (fragment.getDistribution() == PlanDistribution.SINGLE) {
                    scheduleFixedNodeCount(1);
                }
                else if (fragment.getDistribution() == PlanDistribution.FIXED) {
                    scheduleFixedNodeCount(initialHashPartitions);
                }
                else if (fragment.getDistribution() == PlanDistribution.SOURCE) {
                    scheduleSourcePartitionedNodes();
                }
                else if (fragment.getDistribution() == PlanDistribution.COORDINATOR_ONLY) {
                    scheduleOnCurrentNode();
                }
                else {
                    throw new IllegalStateException("Unsupported partitioning: " + fragment.getDistribution());
                }

                stateMachine.transitionToScheduled();

                // add the missing exchanges output buffers
                updateNewExchangesAndBuffers(true);
            }
            catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }

                if (stateMachine.transitionToFailed(e)) {
                    throw Throwables.propagate(e);
                }

                // stage is already finished, so only throw if this is an error
                Throwables.propagateIfInstanceOf(e, Error.class);
            }
            finally {
                doUpdateState();
            }
        }
    }

    private void scheduleFixedNodeCount(int nodeCount)
    {
        // create tasks on "nodeCount" random nodes
        List<Node> nodes = nodeSelector.selectRandomNodes(nodeCount);
        checkCondition(!nodes.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
        ImmutableList.Builder<TaskId> tasks = ImmutableList.builder();
        for (int taskId = 0; taskId < nodes.size(); taskId++) {
            Node node = nodes.get(taskId);
            RemoteTask task = scheduleTask(taskId, node);
            tasks.add(task.getTaskInfo().getTaskId());
        }

        // tell sub stages about all nodes and that there will not be more nodes
        for (SqlStageExecution subStage : subStages.values()) {
            subStage.parentTasksAdded(tasks.build(), true);
        }
    }

    private void scheduleOnCurrentNode()
    {
        // create task on current node
        Node node = nodeSelector.selectCurrentNode();
        RemoteTask task = scheduleTask(0, node);

        // tell sub stages about all nodes and that there will not be more nodes
        for (SqlStageExecution subStage : subStages.values()) {
            subStage.parentTasksAdded(ImmutableList.of(task.getTaskInfo().getTaskId()), true);
        }
    }

    private void scheduleSourcePartitionedNodes()
            throws InterruptedException
    {
        AtomicInteger nextTaskId = new AtomicInteger(0);

        try (SplitSource splitSource = this.dataSource.get()) {
            while (!splitSource.isFinished()) {
                // if query has been canceled, exit cleanly; query will never run regardless
                if (getState().isDone()) {
                    break;
                }

                long start = System.nanoTime();
                Set<Split> pendingSplits = ImmutableSet.copyOf(getFutureValue(splitSource.getNextBatch(splitBatchSize)));
                stateMachine.recordGetSplitTime(start);

                while (!pendingSplits.isEmpty() && !getState().isDone()) {
                    Multimap<Node, Split> splitAssignment = nodeSelector.computeAssignments(pendingSplits, tasks.values());
                    pendingSplits = ImmutableSet.copyOf(Sets.difference(pendingSplits, ImmutableSet.copyOf(splitAssignment.values())));

                    assignSplits(nextTaskId, splitAssignment);

                    if (!pendingSplits.isEmpty()) {
                        waitForFreeNode(nextTaskId);
                    }
                }
            }
        }

        for (RemoteTask task : tasks.values()) {
            task.noMoreSplits(fragment.getPartitionedSource());
        }
        completeSources.add(fragment.getPartitionedSource());

        // tell sub stages there will be no more output buffers
        setNoMoreStageNodes();
    }

    private void assignSplits(AtomicInteger nextTaskId, Multimap<Node, Split> splitAssignment)
    {
        for (Entry<Node, Collection<Split>> taskSplits : splitAssignment.asMap().entrySet()) {
            long scheduleSplitStart = System.nanoTime();
            Node node = taskSplits.getKey();

            TaskId taskId = Iterables.getOnlyElement(localNodeTaskMap.get(node), null);
            RemoteTask task = taskId != null ? tasks.get(taskId) : null;
            if (task == null) {
                RemoteTask remoteTask = scheduleTask(nextTaskId.getAndIncrement(), node, fragment.getPartitionedSource(), taskSplits.getValue());

                // tell the sub stages to create a buffer for this task
                addStageNode(remoteTask.getTaskInfo().getTaskId());

                stateMachine.recordScheduleTaskTime(scheduleSplitStart);
            }
            else {
                task.addSplits(fragment.getPartitionedSource(), taskSplits.getValue());
                stateMachine.recordAddSplit(scheduleSplitStart);
            }
        }
    }

    private void waitForFreeNode(AtomicInteger nextTaskId)
    {
        // if we have sub stages...
        if (!subStages.isEmpty()) {
            // before we block, we need to create all possible output buffers on the sub stages, or they can deadlock
            // waiting for the "noMoreBuffers" call
            nodeSelector.lockDownNodes();
            for (Node node : Sets.difference(new HashSet<>(nodeSelector.allNodes()), localNodeTaskMap.keySet())) {
                RemoteTask remoteTask = scheduleTask(nextTaskId.getAndIncrement(), node);

                // tell the sub stages to create a buffer for this task
                addStageNode(remoteTask.getTaskInfo().getTaskId());
            }
            // tell sub stages there will be no more output buffers
            setNoMoreStageNodes();
        }

        synchronized (this) {
            // otherwise wait for some tasks to complete
            try {
                // todo this adds latency: replace this wait with an event listener
                TimeUnit.MILLISECONDS.timedWait(this, 100);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
        updateNewExchangesAndBuffers(false);
    }

    private void addStageNode(TaskId task)
    {
        for (SqlStageExecution subStage : subStages.values()) {
            subStage.parentTasksAdded(ImmutableList.of(task), false);
        }
    }

    private void setNoMoreStageNodes()
    {
        for (SqlStageExecution subStage : subStages.values()) {
            subStage.parentTasksAdded(ImmutableList.<TaskId>of(), true);
        }
    }

    private RemoteTask scheduleTask(int id, Node node)
    {
        return scheduleTask(id, node, null, ImmutableList.<Split>of());
    }

    private RemoteTask scheduleTask(int id, Node node, PlanNodeId sourceId, Iterable<? extends Split> sourceSplits)
    {
        // before scheduling a new task update all existing tasks with new exchanges and output buffers
        addNewExchangesAndBuffers();

        TaskId taskId = new TaskId(stateMachine.getStageId(), String.valueOf(id));

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        for (Split sourceSplit : sourceSplits) {
            initialSplits.put(sourceId, sourceSplit);
        }
        for (Entry<PlanNodeId, URI> entry : exchangeLocations.get().entries()) {
            initialSplits.put(entry.getKey(), createRemoteSplitFor(taskId, entry.getValue()));
        }

        RemoteTask task = remoteTaskFactory.createRemoteTask(stateMachine.getSession(),
                taskId,
                node,
                fragment,
                initialSplits.build(),
                getCurrentOutputBuffers());

        task.addStateChangeListener(taskInfo -> doUpdateState());

        // create and update task
        task.start();

        // record this task
        tasks.put(task.getTaskInfo().getTaskId(), task);
        localNodeTaskMap.put(node, task.getTaskInfo().getTaskId());
        nodeTaskMap.addTask(node, task);

        // check whether the stage finished while we were scheduling this task
        if (stateMachine.getState().isDone()) {
            task.cancel();
        }

        // update in case task finished before listener was registered
        doUpdateState();

        return task;
    }

    private void updateNewExchangesAndBuffers(boolean waitUntilFinished)
    {
        checkState(!Thread.holdsLock(this), "Can not add exchanges or buffers to tasks while holding a lock on this");

        while (!getState().isDone()) {
            boolean finished = addNewExchangesAndBuffers();

            if (finished || !waitUntilFinished) {
                return;
            }

            synchronized (this) {
                // wait for a state change
                //
                // NOTE this must be a wait with a timeout since there is no notification
                // for new exchanges from the child stages
                try {
                    TimeUnit.MILLISECONDS.timedWait(this, 100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    private boolean addNewExchangesAndBuffers()
    {
        // get new exchanges and update exchange state
        Set<PlanNodeId> completeSources = updateCompleteSources();
        boolean allSourceComplete = completeSources.containsAll(allSources);
        Multimap<PlanNodeId, URI> newExchangeLocations = getNewExchangeLocations();
        exchangeLocations.set(ImmutableMultimap.<PlanNodeId, URI>builder()
                .putAll(exchangeLocations.get())
                .putAll(newExchangeLocations)
                .build());

        // get new output buffer and update output buffer state
        OutputBuffers outputBuffers = updateToNextOutputBuffers();

        // finished state must be decided before update to avoid race conditions
        boolean finished = allSourceComplete && outputBuffers.isNoMoreBufferIds();

        // update tasks
        try (SetThreadName ignored = new SetThreadName("SqlStageExecution-%s", stateMachine.getStageId())) {
            for (RemoteTask task : tasks.values()) {
                for (Entry<PlanNodeId, URI> entry : newExchangeLocations.entries()) {
                    Split remoteSplit = createRemoteSplitFor(task.getTaskInfo().getTaskId(), entry.getValue());
                    task.addSplits(entry.getKey(), ImmutableList.of(remoteSplit));
                }
                task.setOutputBuffers(outputBuffers);
                completeSources.forEach(task::noMoreSplits);
            }
        }

        return finished;
    }

    private Set<PlanNodeId> updateCompleteSources()
    {
        for (RemoteSourceNode remoteSourceNode : fragment.getRemoteSourceNodes()) {
            if (!completeSources.contains(remoteSourceNode.getId())) {
                boolean exchangeFinished = remoteSourceNode.getSourceFragmentIds().stream()
                        .allMatch(this::isExchangeFinished);
                if (exchangeFinished) {
                    completeSources.add(remoteSourceNode.getId());
                }
            }
        }
        return completeSources;
    }

    private boolean isExchangeFinished(PlanFragmentId planFragmentId)
    {
        SqlStageExecution subStage = subStages.get(planFragmentId);
        switch (subStage.getState()) {
            case SCHEDULED:
            case RUNNING:
            case FINISHED:
            case CANCELED:
                return true;
            // DO NOT complete a FAILED or ABORTED stage.  This will cause the
            // stage above to finish normally, which will result in a query
            // completing successfully when it should fail..
        }
        return false;
    }

    @SuppressWarnings("NakedNotify")
    private void doUpdateState()
    {
        checkState(!Thread.holdsLock(this), "Can not doUpdateState while holding a lock on this");

        try (SetThreadName ignored = new SetThreadName("Stage-%s", stateMachine.getStageId())) {
            synchronized (this) {
                // wake up worker thread waiting for state changes
                this.notifyAll();

                StageState initialState = getState();
                if (initialState.isDone()) {
                    return;
                }

                List<TaskInfo> taskInfos = tasks.values().stream()
                        .map(RemoteTask::getTaskInfo)
                        .collect(toImmutableList());

                List<TaskState> taskStates = taskInfos.stream()
                        .map(TaskInfo::getState)
                        .collect(toImmutableList());

                if (any(taskStates, equalTo(TaskState.FAILED))) {
                    RuntimeException failure = taskInfos.stream()
                            .map(taskInfo -> Iterables.getFirst(taskInfo.getFailures(), null))
                            .filter(Objects::nonNull)
                            .findFirst()
                            .map(ExecutionFailureInfo::toException)
                            .orElse(new PrestoException(StandardErrorCode.INTERNAL_ERROR, "A task failed for an unknown reason"));
                    stateMachine.transitionToFailed(failure);
                }
                else if (taskStates.stream().anyMatch(TaskState.ABORTED::equals)) {
                    // A task should only be in the aborted state if the STAGE is done (ABORTED or FAILED)
                    stateMachine.transitionToFailed(new PrestoException(StandardErrorCode.INTERNAL_ERROR, "A task is in the ABORTED state but stage is " + initialState));
                }
                else if (initialState != StageState.PLANNED && initialState != StageState.SCHEDULING) {
                    // all tasks are now scheduled, so we can check the finished state
                    if (taskStates.stream().allMatch(TaskState::isDone)) {
                        stateMachine.transitionToFinished();
                    }
                    else if (taskStates.stream().anyMatch(TaskState.RUNNING::equals)) {
                        stateMachine.transitionToRunning();
                    }
                }
            }

            // if this stage is now finished, cancel all work
            if (getState().isDone()) {
                cancel();
            }
        }
    }

    public void cancel()
    {
        checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        try (SetThreadName ignored = new SetThreadName("Stage-%s", stateMachine.getStageId())) {
            // check if the stage already completed naturally
            doUpdateState();

            stateMachine.transitionToCanceled();

            // cancel all tasks
            tasks.values().forEach(RemoteTask::cancel);

            // propagate cancel to sub-stages
            subStages.values().forEach(SqlStageExecution::cancel);
        }
    }

    public void abort()
    {
        checkState(!Thread.holdsLock(this), "Can not abort while holding a lock on this");

        try (SetThreadName ignored = new SetThreadName("Stage-%s", stateMachine.getStageId())) {
            // transition to aborted state, only if not already finished
            doUpdateState();

            stateMachine.transitionToAborted();

            // abort all tasks
            tasks.values().forEach(RemoteTask::abort);

            // propagate abort to sub-stages
            subStages.values().forEach(SqlStageExecution::abort);
        }
    }

    private static Split createRemoteSplitFor(TaskId taskId, URI taskLocation)
    {
        URI splitLocation = uriBuilderFrom(taskLocation).appendPath("results").appendPath(taskId.toString()).build();
        return new Split("remote", new RemoteSplit(splitLocation));
    }

    @Override
    public String toString()
    {
        return stateMachine.toString();
    }

    private static Optional<Integer> getHashChannel(PlanFragment fragment)
    {
        return fragment.getHash().map(symbol -> fragment.getOutputLayout().indexOf(symbol));
    }

    private static Optional<List<Integer>> getPartitioningChannels(PlanFragment fragment)
    {
        checkState(fragment.getOutputPartitioning() == OutputPartitioning.HASH, "fragment is not hash partitioned");
        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        return fragment.getPartitionBy().map(
                t -> t.stream()
                        .map(symbol -> fragment.getOutputLayout().indexOf(symbol))
                        .collect(toImmutableList()));
    }
}
