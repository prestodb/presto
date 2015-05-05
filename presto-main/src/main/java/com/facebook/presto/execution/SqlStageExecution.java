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
import com.facebook.presto.Session;
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.execution.NodeScheduler.NodeSelector;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.TaskStats;
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
import io.airlift.log.Logger;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Failures.toFailures;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public final class SqlStageExecution
        implements StageExecutionNode
{
    private static final Logger log = Logger.get(SqlStageExecution.class);

    // NOTE: DO NOT call methods on the parent while holding a lock on the child.  Locks
    // are always acquired top down in the tree, so calling a method on the parent while
    // holding a lock on the 'this' could cause a deadlock.
    // This is only here to aid in debugging
    @Nullable
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final StageExecutionNode parent;
    private final StageId stageId;
    private final URI location;
    private final PlanFragment fragment;
    private final Set<PlanNodeId> allSources;
    private final Map<PlanFragmentId, StageExecutionNode> subStages;

    private final Multimap<Node, TaskId> localNodeTaskMap = HashMultimap.create();
    private final ConcurrentMap<TaskId, RemoteTask> tasks = new ConcurrentHashMap<>();

    private final Optional<SplitSource> dataSource;
    private final RemoteTaskFactory remoteTaskFactory;
    private final Session session; // only used for remote task factory
    private final int splitBatchSize;

    private final int initialHashPartitions;

    private final StateMachine<StageState> stageState;

    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    private final Set<PlanNodeId> completeSources = newConcurrentHashSet();

    @GuardedBy("this")
    private OutputBuffers currentOutputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS;
    @GuardedBy("this")
    private OutputBuffers nextOutputBuffers;

    private final ExecutorService executor;

    private final AtomicReference<DateTime> schedulingComplete = new AtomicReference<>();

    private final Distribution getSplitDistribution = new Distribution();
    private final Distribution scheduleTaskDistribution = new Distribution();
    private final Distribution addSplitDistribution = new Distribution();

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
        this(null,
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

    private SqlStageExecution(@Nullable StageExecutionNode parent,
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

        this.stageId = new StageId(queryId, String.valueOf(nextStageId.getAndIncrement()));
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            this.parent = parent;
            this.location = locationFactory.createStageLocation(stageId);
            this.fragment = plan.getFragment();
            this.dataSource = plan.getDataSource();
            this.remoteTaskFactory = remoteTaskFactory;
            this.session = session;
            this.splitBatchSize = splitBatchSize;
            this.initialHashPartitions = initialHashPartitions;
            this.executor = executor;

            this.allSources = Stream.concat(
                    Stream.of(fragment.getPartitionedSource()),
                    fragment.getRemoteSourceNodes().stream()
                            .map(RemoteSourceNode::getId))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

            ImmutableMap.Builder<PlanFragmentId, StageExecutionNode> subStages = ImmutableMap.builder();
            for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
                PlanFragmentId subStageFragmentId = subStagePlan.getFragment().getId();
                StageExecutionNode subStage = new SqlStageExecution(this,
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

                subStage.addStateChangeListener(stageState -> doUpdateState());

                subStages.put(subStageFragmentId, subStage);
            }
            this.subStages = subStages.build();

            String dataSourceName = dataSource.isPresent() ? dataSource.get().getDataSourceName() : null;
            this.nodeSelector = nodeScheduler.createNodeSelector(dataSourceName);
            this.nodeTaskMap = nodeTaskMap;
            stageState = new StateMachine<>("stage " + stageId, this.executor, StageState.PLANNED);
            stageState.addStateChangeListener(state -> log.debug("Stage %s is %s", stageId, state));
        }
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            if (stageId.equals(this.stageId)) {
                cancel();
            }
            else {
                for (StageExecutionNode subStage : subStages.values()) {
                    subStage.cancelStage(stageId);
                }
            }
        }
    }

    @Override
    @VisibleForTesting
    public StageState getState()
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            return stageState.get();
        }
    }

    @Override
    public long getTotalMemoryReservation()
    {
        long memory = 0;
        for (RemoteTask task : tasks.values()) {
            memory += task.getTaskInfo().getStats().getMemoryReservation().toBytes();
        }
        for (StageExecutionNode subStage : subStages.values()) {
            memory += subStage.getTotalMemoryReservation();
        }
        return memory;
    }

    @Override
    public StageInfo getStageInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            // stage state must be captured first in order to provide a
            // consistent view of the stage For example, building this
            // information, the stage could finish, and the task states would
            // never be visible.
            StageState state = stageState.get();

            List<TaskInfo> taskInfos = tasks.values().stream()
                    .map(RemoteTask::getTaskInfo)
                    .collect(toImmutableList());

            List<StageInfo> subStageInfos = subStages.values().stream()
                    .map(StageExecutionNode::getStageInfo)
                    .collect(toImmutableList());

            int totalTasks = taskInfos.size();
            int runningTasks = 0;
            int completedTasks = 0;

            int totalDrivers = 0;
            int queuedDrivers = 0;
            int runningDrivers = 0;
            int completedDrivers = 0;

            long totalMemoryReservation = 0;

            long totalScheduledTime = 0;
            long totalCpuTime = 0;
            long totalUserTime = 0;
            long totalBlockedTime = 0;

            long rawInputDataSize = 0;
            long rawInputPositions = 0;

            long processedInputDataSize = 0;
            long processedInputPositions = 0;

            long outputDataSize = 0;
            long outputPositions = 0;

            boolean fullyBlocked = true;
            Set<BlockedReason> blockedReasons = new HashSet<>();

            for (TaskInfo taskInfo : taskInfos) {
                if (taskInfo.getState().isDone()) {
                    completedTasks++;
                }
                else {
                    runningTasks++;
                }

                TaskStats taskStats = taskInfo.getStats();

                totalDrivers += taskStats.getTotalDrivers();
                queuedDrivers += taskStats.getQueuedDrivers();
                runningDrivers += taskStats.getRunningDrivers();
                completedDrivers += taskStats.getCompletedDrivers();

                totalMemoryReservation += taskStats.getMemoryReservation().toBytes();

                totalScheduledTime += taskStats.getTotalScheduledTime().roundTo(NANOSECONDS);
                totalCpuTime += taskStats.getTotalCpuTime().roundTo(NANOSECONDS);
                totalUserTime += taskStats.getTotalUserTime().roundTo(NANOSECONDS);
                totalBlockedTime += taskStats.getTotalBlockedTime().roundTo(NANOSECONDS);
                if (!taskInfo.getState().isDone()) {
                    fullyBlocked &= taskStats.isFullyBlocked();
                    blockedReasons.addAll(taskStats.getBlockedReasons());
                }

                rawInputDataSize += taskStats.getRawInputDataSize().toBytes();
                rawInputPositions += taskStats.getRawInputPositions();

                processedInputDataSize += taskStats.getProcessedInputDataSize().toBytes();
                processedInputPositions += taskStats.getProcessedInputPositions();

                outputDataSize += taskStats.getOutputDataSize().toBytes();
                outputPositions += taskStats.getOutputPositions();
            }

            StageStats stageStats = new StageStats(
                    schedulingComplete.get(),
                    getSplitDistribution.snapshot(),
                    scheduleTaskDistribution.snapshot(),
                    addSplitDistribution.snapshot(),

                    totalTasks,
                    runningTasks,
                    completedTasks,

                    totalDrivers,
                    queuedDrivers,
                    runningDrivers,
                    completedDrivers,

                    new DataSize(totalMemoryReservation, BYTE).convertToMostSuccinctDataSize(),
                    new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    fullyBlocked && runningTasks > 0,
                    blockedReasons,

                    new DataSize(rawInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                    rawInputPositions,
                    new DataSize(processedInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                    processedInputPositions,
                    new DataSize(outputDataSize, BYTE).convertToMostSuccinctDataSize(),
                    outputPositions);

            return new StageInfo(stageId,
                    state,
                    location,
                    fragment,
                    fragment.getTypes(),
                    stageStats,
                    taskInfos,
                    subStageInfos,
                    toFailures(failureCauses));
        }
    }

    @Override
    public synchronized void parentTasksAdded(List<TaskId> parentTasks, boolean noMoreParentNodes)
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
                buffers.put(taskId, new HashPagePartitionFunction(nodeIndex, parentTasks.size(), getPartitioningChannels(fragment), getHashChannel(fragment), fragment.getTypes()));
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

    public synchronized OutputBuffers getCurrentOutputBuffers()
    {
        return currentOutputBuffers;
    }

    public synchronized OutputBuffers updateToNextOutputBuffers()
    {
        if (nextOutputBuffers == null) {
            return currentOutputBuffers;
        }

        currentOutputBuffers = nextOutputBuffers;
        nextOutputBuffers = null;
        this.notifyAll();
        return currentOutputBuffers;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<StageState> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            stageState.addStateChangeListener(stateChangeListener::stateChanged);
        }
    }

    private Multimap<PlanNodeId, URI> getNewExchangeLocations()
    {
        Multimap<PlanNodeId, URI> exchangeLocations = this.exchangeLocations.get();

        ImmutableMultimap.Builder<PlanNodeId, URI> newExchangeLocations = ImmutableMultimap.builder();
        for (RemoteSourceNode remoteSourceNode : fragment.getRemoteSourceNodes()) {
            for (PlanFragmentId planFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                StageExecutionNode subStage = subStages.get(planFragmentId);
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

    @Override
    @VisibleForTesting
    public synchronized List<URI> getTaskLocations()
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
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
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            return scheduleStartTasks();
        }
    }

    @Override
    @VisibleForTesting
    public Future<?> scheduleStartTasks()
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            // start sub-stages (starts bottom-up)
            subStages.values().forEach(StageExecutionNode::scheduleStartTasks);
            return executor.submit(this::startTasks);
        }
    }

    private void startTasks()
    {
        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            try {
                checkState(!Thread.holdsLock(this), "Can not start while holding a lock on this");

                // transition to scheduling
                synchronized (this) {
                    if (!stageState.compareAndSet(StageState.PLANNED, StageState.SCHEDULING)) {
                        // stage has already been started, has been canceled or has no tasks due to partition pruning
                        return;
                    }
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

                schedulingComplete.set(DateTime.now());
                stageState.set(StageState.SCHEDULED);

                // add the missing exchanges output buffers
                updateNewExchangesAndBuffers(true);
            }
            catch (Throwable e) {
                // some exceptions can occur when the query finishes early
                if (!getState().isDone()) {
                    synchronized (this) {
                        failureCauses.add(e);
                        stageState.set(StageState.FAILED);
                    }
                    log.error(e, "Error while starting stage %s", stageId);
                    abort();
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    throw Throwables.propagate(e);
                }
                Throwables.propagateIfInstanceOf(e, Error.class);
                log.debug(e, "Error while starting stage in done query %s", stageId);
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
        for (StageExecutionNode subStage : subStages.values()) {
            subStage.parentTasksAdded(tasks.build(), true);
        }
    }

    private void scheduleOnCurrentNode()
    {
        // create task on current node
        Node node = nodeSelector.selectCurrentNode();
        RemoteTask task = scheduleTask(0, node);

        // tell sub stages about all nodes and that there will not be more nodes
        for (StageExecutionNode subStage : subStages.values()) {
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
                getSplitDistribution.add(System.nanoTime() - start);

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

                scheduleTaskDistribution.add(System.nanoTime() - scheduleSplitStart);
            }
            else {
                task.addSplits(fragment.getPartitionedSource(), taskSplits.getValue());
                addSplitDistribution.add(System.nanoTime() - scheduleSplitStart);
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
        for (StageExecutionNode subStage : subStages.values()) {
            subStage.parentTasksAdded(ImmutableList.of(task), false);
        }
    }

    private void setNoMoreStageNodes()
    {
        for (StageExecutionNode subStage : subStages.values()) {
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

        TaskId taskId = new TaskId(stageId, String.valueOf(id));

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        for (Split sourceSplit : sourceSplits) {
            initialSplits.put(sourceId, sourceSplit);
        }
        for (Entry<PlanNodeId, URI> entry : exchangeLocations.get().entries()) {
            initialSplits.put(entry.getKey(), createRemoteSplitFor(taskId, entry.getValue()));
        }

        RemoteTask task = remoteTaskFactory.createRemoteTask(session,
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
        for (RemoteTask task : tasks.values()) {
            for (Entry<PlanNodeId, URI> entry : newExchangeLocations.entries()) {
                Split remoteSplit = createRemoteSplitFor(task.getTaskInfo().getTaskId(), entry.getValue());
                task.addSplits(entry.getKey(), ImmutableList.of(remoteSplit));
            }
            task.setOutputBuffers(outputBuffers);
            completeSources.forEach(task::noMoreSplits);
        }

        return finished;
    }

    private Set<PlanNodeId> updateCompleteSources()
    {
        for (RemoteSourceNode remoteSourceNode : fragment.getRemoteSourceNodes()) {
            if (!completeSources.contains(remoteSourceNode.getId())) {
                boolean exchangeFinished = true;
                for (PlanFragmentId planFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                    StageExecutionNode subStage = subStages.get(planFragmentId);
                    switch (subStage.getState()) {
                        case PLANNED:
                        case SCHEDULING:
                            exchangeFinished = false;
                            break;
                    }
                }
                if (exchangeFinished) {
                    completeSources.add(remoteSourceNode.getId());
                }
            }
        }
        return completeSources;
    }

    @VisibleForTesting
    @SuppressWarnings("NakedNotify")
    public void doUpdateState()
    {
        checkState(!Thread.holdsLock(this), "Can not doUpdateState while holding a lock on this");

        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            synchronized (this) {
                // wake up worker thread waiting for state changes
                this.notifyAll();

                StageState currentState = stageState.get();
                if (currentState.isDone()) {
                    return;
                }

                List<StageState> subStageStates = ImmutableList.copyOf(transform(subStages.values(), StageExecutionNode::getState));
                if (subStageStates.stream().anyMatch(StageState::isFailure)) {
                    stageState.set(StageState.ABORTED);
                }
                else {
                    List<TaskState> taskStates = ImmutableList.copyOf(transform(transform(tasks.values(), RemoteTask::getTaskInfo), TaskInfo::getState));
                    if (any(taskStates, equalTo(TaskState.FAILED))) {
                        stageState.set(StageState.FAILED);
                    }
                    else if (any(taskStates, equalTo(TaskState.ABORTED))) {
                        // A task should only be in the aborted state if the STAGE is done (ABORTED or FAILED)
                        stageState.set(StageState.FAILED);
                        failureCauses.add(new PrestoException(StandardErrorCode.INTERNAL_ERROR, "A task is in the ABORTED state but stage is " + currentState));
                    }
                    else if (currentState != StageState.PLANNED && currentState != StageState.SCHEDULING) {
                        // all tasks are now scheduled, so we can check the finished state
                        if (all(taskStates, TaskState::isDone)) {
                            stageState.set(StageState.FINISHED);
                        }
                        else if (any(taskStates, equalTo(TaskState.RUNNING))) {
                            stageState.set(StageState.RUNNING);
                        }
                    }
                }
            }

            // finish tasks and stages if stage is complete
            StageState stageState = this.stageState.get();
            if (stageState == StageState.ABORTED) {
                abort();
            }
            else if (stageState.isDone()) {
                cancel();
            }
        }
    }

    @Override
    public void cancel()
    {
        checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            // check if the stage already completed naturally
            doUpdateState();
            synchronized (this) {
                if (!stageState.get().isDone()) {
                    log.debug("Cancelling stage %s", stageId);
                    stageState.set(StageState.CANCELED);
                }
            }

            // cancel all tasks
            tasks.values().forEach(RemoteTask::cancel);

            // propagate cancel to sub-stages
            subStages.values().forEach(StageExecutionNode::cancel);
        }
    }

    @Override
    public void abort()
    {
        checkState(!Thread.holdsLock(this), "Can not abort while holding a lock on this");

        try (SetThreadName ignored = new SetThreadName("Stage-%s", stageId)) {
            // transition to aborted state, only if not already finished
            doUpdateState();
            synchronized (this) {
                if (!stageState.get().isDone()) {
                    log.debug("Aborting stage %s", stageId);
                    stageState.set(StageState.ABORTED);
                }
            }

            // abort all tasks
            tasks.values().forEach(RemoteTask::abort);

            // propagate abort to sub-stages
            subStages.values().forEach(StageExecutionNode::abort);
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
        return toStringHelper(this)
                .add("stageId", stageId)
                .add("location", location)
                .add("stageState", stageState.get())
                .toString();
    }

    private static Optional<Integer> getHashChannel(PlanFragment fragment)
    {
        return fragment.getHash().map(symbol -> fragment.getOutputLayout().indexOf(symbol));
    }

    private static List<Integer> getPartitioningChannels(PlanFragment fragment)
    {
        checkState(fragment.getOutputPartitioning() == OutputPartitioning.HASH, "fragment is not hash partitioned");
        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        return fragment.getPartitionBy().stream()
                .map(symbol -> fragment.getOutputLayout().indexOf(symbol))
                .collect(toImmutableList());
    }
}

/*
 * Since the execution is a tree of SqlStateExecutions, each stage can directly access
 * the private fields and methods of stages up and down the tree.  To prevent accidental
 * errors, each stage reference parents and children using this interface so direct
 * access is not possible.
 */
interface StageExecutionNode
{
    StageInfo getStageInfo();

    long getTotalMemoryReservation();

    StageState getState();

    Future<?> scheduleStartTasks();

    void parentTasksAdded(List<TaskId> parentTasks, boolean noMoreParentNodes);

    Iterable<? extends URI> getTaskLocations();

    void addStateChangeListener(StateChangeListener<StageState> stateChangeListener);

    void cancelStage(StageId stageId);

    void cancel();

    void abort();
}
