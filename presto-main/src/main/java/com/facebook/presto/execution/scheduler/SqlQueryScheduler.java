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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.execution.BasicStageStats;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.getConcurrentLifespansPerNode;
import static com.facebook.presto.SystemSessionProperties.getWriterMinSize;
import static com.facebook.presto.connector.ConnectorId.isInternalSystemConnector;
import static com.facebook.presto.execution.BasicStageStats.aggregateBasicStageStats;
import static com.facebook.presto.execution.StageState.ABORTED;
import static com.facebook.presto.execution.StageState.CANCELED;
import static com.facebook.presto.execution.StageState.FAILED;
import static com.facebook.presto.execution.StageState.FINISHED;
import static com.facebook.presto.execution.StageState.RUNNING;
import static com.facebook.presto.execution.StageState.SCHEDULED;
import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class SqlQueryScheduler
{
    private final QueryStateMachine queryStateMachine;
    private final ExecutionPolicy executionPolicy;
    private final Map<StageId, SqlStageExecution> stages;
    private final ExecutorService executor;
    private final StageId rootStageId;
    private final Map<StageId, StageScheduler> stageSchedulers;
    private final Map<StageId, StageLinkage> stageLinkages;
    private final SplitSchedulerStats schedulerStats;
    private final boolean summarizeTaskInfo;
    private final AtomicBoolean started = new AtomicBoolean();

    public SqlQueryScheduler(QueryStateMachine queryStateMachine,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            boolean summarizeTaskInfo,
            int splitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            OutputBuffers rootOutputBuffers,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.executionPolicy = requireNonNull(executionPolicy, "schedulerPolicyFactory is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.summarizeTaskInfo = summarizeTaskInfo;

        // todo come up with a better way to build this, or eliminate this map
        ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers = ImmutableMap.builder();
        ImmutableMap.Builder<StageId, StageLinkage> stageLinkages = ImmutableMap.builder();

        // Only fetch a distribution once per query to assure all stages see the same machine assignments
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();

        OutputBufferId rootBufferId = Iterables.getOnlyElement(rootOutputBuffers.getBuffers().keySet());
        List<SqlStageExecution> stages = createStages(
                (fragmentId, tasks, noMoreExchangeLocations) -> updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations),
                new AtomicInteger(),
                locationFactory,
                plan.withBucketToPartition(Optional.of(new int[1])),
                nodeScheduler,
                remoteTaskFactory,
                session,
                splitBatchSize,
                partitioningHandle -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle)),
                nodePartitioningManager,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                nodeTaskMap,
                stageSchedulers,
                stageLinkages);

        SqlStageExecution rootStage = stages.get(0);
        rootStage.setOutputBuffers(rootOutputBuffers);
        this.rootStageId = rootStage.getStageId();

        this.stages = stages.stream()
                .collect(toImmutableMap(SqlStageExecution::getStageId, identity()));

        this.stageSchedulers = stageSchedulers.build();
        this.stageLinkages = stageLinkages.build();

        this.executor = queryExecutor;

        rootStage.addStateChangeListener(state -> {
            if (state == FINISHED) {
                queryStateMachine.transitionToFinishing();
            }
            else if (state == CANCELED) {
                // output stage was canceled
                queryStateMachine.transitionToCanceled();
            }
        });

        for (SqlStageExecution stage : stages) {
            stage.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }
                if (state == FAILED) {
                    queryStateMachine.transitionToFailed(stage.getStageInfo().getFailureCause().toException());
                }
                else if (state == ABORTED) {
                    // this should never happen, since abort can only be triggered in query clean up after the query is finished
                    queryStateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                }
                else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                    // if the stage has at least one task, we are running
                    if (stage.hasTasks()) {
                        queryStateMachine.transitionToRunning();
                    }
                }
            });
        }

        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()));
            }
        });
        for (SqlStageExecution stage : stages) {
            stage.addFinalStatusListener(status -> queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo())));
        }
    }

    private static void updateQueryOutputLocations(QueryStateMachine queryStateMachine, OutputBufferId rootBufferId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations)
    {
        Set<URI> bufferLocations = tasks.stream()
                .map(task -> task.getTaskStatus().getSelf())
                .map(location -> uriBuilderFrom(location).appendPath("results").appendPath(rootBufferId.toString()).build())
                .collect(toImmutableSet());
        queryStateMachine.updateOutputLocations(bufferLocations, noMoreExchangeLocations);
    }

    private List<SqlStageExecution> createStages(
            ExchangeLocationsConsumer parent,
            AtomicInteger nextStageId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int splitBatchSize,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            NodePartitioningManager nodePartitioningManager,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers,
            ImmutableMap.Builder<StageId, StageLinkage> stageLinkages)
    {
        ImmutableList.Builder<SqlStageExecution> stages = ImmutableList.builder();

        StageId stageId = new StageId(queryStateMachine.getQueryId(), nextStageId.getAndIncrement());
        SqlStageExecution stage = new SqlStageExecution(
                stageId,
                locationFactory.createStageLocation(stageId),
                plan.getFragment(),
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                nodeTaskMap,
                queryExecutor,
                failureDetector,
                schedulerStats);

        stages.add(stage);

        Optional<int[]> bucketToPartition;
        PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
            // nodes are selected dynamically based on the constraints of the splits and the system load
            Entry<PlanNodeId, SplitSource> entry = Iterables.getOnlyElement(plan.getSplitSources().entrySet());
            PlanNodeId planNodeId = entry.getKey();
            SplitSource splitSource = entry.getValue();
            ConnectorId connectorId = splitSource.getConnectorId();
            if (isInternalSystemConnector(connectorId)) {
                connectorId = null;
            }
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(connectorId);
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stage::getAllTasks);

            checkArgument(!plan.getFragment().getStageExecutionStrategy().isAnyScanGroupedExecution());
            stageSchedulers.put(stageId, newSourcePartitionedSchedulerAsStageScheduler(stage, planNodeId, splitSource, placementPolicy, splitBatchSize));
            bucketToPartition = Optional.of(new int[1]);
        }
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            bucketToPartition = Optional.of(new int[1]);
        }
        else {
            // nodes are pre determined by the nodePartitionMap
            NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
            long nodeCount = nodePartitionMap.getPartitionToNode().values().stream().distinct().count();
            OptionalInt concurrentLifespansPerTask = getConcurrentLifespansPerNode(session);

            Map<PlanNodeId, SplitSource> splitSources = plan.getSplitSources();
            if (!splitSources.isEmpty()) {
                List<PlanNodeId> schedulingOrder = plan.getFragment().getPartitionedSources();
                List<ConnectorPartitionHandle> connectorPartitionHandles;
                if (plan.getFragment().getStageExecutionStrategy().isAnyScanGroupedExecution()) {
                    connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                    checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                    checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                }
                else {
                    connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                }
                stageSchedulers.put(stageId, new FixedSourcePartitionedScheduler(
                        stage,
                        splitSources,
                        plan.getFragment().getStageExecutionStrategy(),
                        schedulingOrder,
                        nodePartitionMap,
                        splitBatchSize,
                        concurrentLifespansPerTask.isPresent() ? OptionalInt.of(toIntExact(concurrentLifespansPerTask.getAsInt() * nodeCount)) : OptionalInt.empty(),
                        nodeScheduler.createNodeSelector(null),
                        connectorPartitionHandles));
                bucketToPartition = Optional.of(nodePartitionMap.getBucketToPartition());
            }
            else {
                Map<Integer, Node> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                stageSchedulers.put(stageId, new FixedCountScheduler(stage, partitionToNode));
                bucketToPartition = Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }

        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
            List<SqlStageExecution> subTree = createStages(
                    stage::addExchangeLocations,
                    nextStageId,
                    locationFactory,
                    subStagePlan.withBucketToPartition(bucketToPartition),
                    nodeScheduler,
                    remoteTaskFactory,
                    session,
                    splitBatchSize,
                    partitioningCache,
                    nodePartitioningManager,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    stageSchedulers,
                    stageLinkages);
            stages.addAll(subTree);

            SqlStageExecution childStage = subTree.get(0);
            childStagesBuilder.add(childStage);
        }
        Set<SqlStageExecution> childStages = childStagesBuilder.build();
        stage.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                childStages.forEach(SqlStageExecution::cancel);
            }
        });

        stageLinkages.put(stageId, new StageLinkage(plan.getFragment().getId(), parent, childStages));

        if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStages.stream()
                    .map(SqlStageExecution::getAllTasks)
                    .flatMap(Collection::stream)
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Supplier<Collection<TaskStatus>> writerTasksProvider = () -> stage.getAllTasks().stream()
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                    stage,
                    sourceTasksProvider,
                    writerTasksProvider,
                    nodeScheduler.createNodeSelector(null),
                    schedulerExecutor,
                    getWriterMinSize(session));
            whenAllStages(childStages, StageState::isDone)
                    .addListener(scheduler::finish, directExecutor());
            stageSchedulers.put(stageId, scheduler);
        }

        return stages.build();
    }

    public BasicStageStats getBasicStageStats()
    {
        List<BasicStageStats> stageStats = stages.values().stream()
                .map(SqlStageExecution::getBasicStageStats)
                .collect(toImmutableList());

        return aggregateBasicStageStats(stageStats);
    }

    public StageInfo getStageInfo()
    {
        Map<StageId, StageInfo> stageInfos = stages.values().stream()
                .map(SqlStageExecution::getStageInfo)
                .collect(toImmutableMap(StageInfo::getStageId, identity()));

        return buildStageInfo(rootStageId, stageInfos);
    }

    private StageInfo buildStageInfo(StageId stageId, Map<StageId, StageInfo> stageInfos)
    {
        StageInfo parent = stageInfos.get(stageId);
        checkArgument(parent != null, "No stageInfo for %s", parent);
        List<StageInfo> childStages = stageLinkages.get(stageId).getChildStageIds().stream()
                .map(childStageId -> buildStageInfo(childStageId, stageInfos))
                .collect(toImmutableList());
        if (childStages.isEmpty()) {
            return parent;
        }
        return new StageInfo(
                parent.getStageId(),
                parent.getState(),
                parent.getSelf(),
                parent.getPlan(),
                parent.getTypes(),
                parent.getStageStats(),
                parent.getTasks(),
                childStages,
                parent.getFailureCause());
    }

    public long getUserMemoryReservation()
    {
        return stages.values().stream()
                .mapToLong(SqlStageExecution::getUserMemoryReservation)
                .sum();
    }

    public long getTotalMemoryReservation()
    {
        return stages.values().stream()
                .mapToLong(SqlStageExecution::getTotalMemoryReservation)
                .sum();
    }

    public Duration getTotalCpuTime()
    {
        long millis = stages.values().stream()
                .mapToLong(stage -> stage.getTotalCpuTime().toMillis())
                .sum();
        return new Duration(millis, MILLISECONDS);
    }

    public void start()
    {
        if (started.compareAndSet(false, true)) {
            executor.submit(this::schedule);
        }
    }

    private void schedule()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            Set<StageId> completedStages = new HashSet<>();
            ExecutionSchedule executionSchedule = executionPolicy.createExecutionSchedule(stages.values());
            while (!executionSchedule.isFinished()) {
                List<ListenableFuture<?>> blockedStages = new ArrayList<>();
                for (SqlStageExecution stage : executionSchedule.getStagesToSchedule()) {
                    stage.beginScheduling();

                    // perform some scheduling work
                    ScheduleResult result = stageSchedulers.get(stage.getStageId())
                            .schedule();

                    // modify parent and children based on the results of the scheduling
                    if (result.isFinished()) {
                        stage.schedulingComplete();
                    }
                    else if (!result.getBlocked().isDone()) {
                        blockedStages.add(result.getBlocked());
                    }
                    stageLinkages.get(stage.getStageId())
                            .processScheduleResults(stage.getState(), result.getNewTasks());
                    schedulerStats.getSplitsScheduledPerIteration().add(result.getSplitsScheduled());
                    if (result.getBlockedReason().isPresent()) {
                        switch (result.getBlockedReason().get()) {
                            case WRITER_SCALING:
                                // no-op
                                break;
                            case WAITING_FOR_SOURCE:
                                schedulerStats.getWaitingForSource().update(1);
                                break;
                            case SPLIT_QUEUES_FULL:
                                schedulerStats.getSplitQueuesFull().update(1);
                                break;
                            case MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE:
                            case NO_ACTIVE_DRIVER_GROUP:
                                break;
                            default:
                                throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                        }
                    }
                }

                // make sure to update stage linkage at least once per loop to catch async state changes (e.g., partial cancel)
                for (SqlStageExecution stage : stages.values()) {
                    if (!completedStages.contains(stage.getStageId()) && stage.getState().isDone()) {
                        stageLinkages.get(stage.getStageId())
                                .processScheduleResults(stage.getState(), ImmutableSet.of());
                        completedStages.add(stage.getStageId());
                    }
                }

                // wait for a state change and then schedule again
                if (!blockedStages.isEmpty()) {
                    try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                        tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                    }
                    for (ListenableFuture<?> blockedStage : blockedStages) {
                        blockedStage.cancel(true);
                    }
                }
            }

            for (SqlStageExecution stage : stages.values()) {
                StageState state = stage.getState();
                if (state != SCHEDULED && state != RUNNING && !state.isDone()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage %s is in state %s", stage.getStageId(), state));
                }
            }
        }
        catch (Throwable t) {
            queryStateMachine.transitionToFailed(t);
            throw t;
        }
        finally {
            RuntimeException closeError = new RuntimeException();
            for (StageScheduler scheduler : stageSchedulers.values()) {
                try {
                    scheduler.close();
                }
                catch (Throwable t) {
                    queryStateMachine.transitionToFailed(t);
                    // Self-suppression not permitted
                    if (closeError != t) {
                        closeError.addSuppressed(t);
                    }
                }
            }
            if (closeError.getSuppressed().length > 0) {
                throw closeError;
            }
        }
    }

    public void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            SqlStageExecution sqlStageExecution = stages.get(stageId);
            SqlStageExecution stage = requireNonNull(sqlStageExecution, () -> format("Stage %s does not exist", stageId));
            stage.cancel();
        }
    }

    public void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            stages.values().forEach(SqlStageExecution::abort);
        }
    }

    private static ListenableFuture<?> whenAllStages(Collection<SqlStageExecution> stages, Predicate<StageState> predicate)
    {
        checkArgument(!stages.isEmpty(), "stages is empty");
        Set<StageId> stageIds = newConcurrentHashSet(stages.stream()
                .map(SqlStageExecution::getStageId)
                .collect(toSet()));
        SettableFuture<?> future = SettableFuture.create();

        for (SqlStageExecution stage : stages) {
            stage.addStateChangeListener(state -> {
                if (predicate.test(state) && stageIds.remove(stage.getStageId()) && stageIds.isEmpty()) {
                    future.set(null);
                }
            });
        }

        return future;
    }

    private interface ExchangeLocationsConsumer
    {
        void addExchangeLocations(PlanFragmentId fragmentId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations);
    }

    private static class StageLinkage
    {
        private final PlanFragmentId currentStageFragmentId;
        private final ExchangeLocationsConsumer parent;
        private final Set<OutputBufferManager> childOutputBufferManagers;
        private final Set<StageId> childStageIds;

        public StageLinkage(PlanFragmentId fragmentId, ExchangeLocationsConsumer parent, Set<SqlStageExecution> children)
        {
            this.currentStageFragmentId = fragmentId;
            this.parent = parent;
            this.childOutputBufferManagers = children.stream()
                    .map(childStage -> {
                        PartitioningHandle partitioningHandle = childStage.getFragment().getPartitioningScheme().getPartitioning().getHandle();
                        if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
                            return new BroadcastOutputBufferManager(childStage::setOutputBuffers);
                        }
                        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                            return new ScaledOutputBufferManager(childStage::setOutputBuffers);
                        }
                        else {
                            int partitionCount = Ints.max(childStage.getFragment().getPartitioningScheme().getBucketToPartition().get()) + 1;
                            return new PartitionedOutputBufferManager(partitioningHandle, partitionCount, childStage::setOutputBuffers);
                        }
                    })
                    .collect(toImmutableSet());

            this.childStageIds = children.stream()
                    .map(SqlStageExecution::getStageId)
                    .collect(toImmutableSet());
        }

        public Set<StageId> getChildStageIds()
        {
            return childStageIds;
        }

        public void processScheduleResults(StageState newState, Set<RemoteTask> newTasks)
        {
            boolean noMoreTasks = false;
            switch (newState) {
                case PLANNED:
                case SCHEDULING:
                    // workers are still being added to the query
                    break;
                case SCHEDULING_SPLITS:
                case SCHEDULED:
                case RUNNING:
                case FINISHED:
                case CANCELED:
                    // no more workers will be added to the query
                    noMoreTasks = true;
                case ABORTED:
                case FAILED:
                    // DO NOT complete a FAILED or ABORTED stage.  This will cause the
                    // stage above to finish normally, which will result in a query
                    // completing successfully when it should fail..
                    break;
            }

            // Add an exchange location to the parent stage for each new task
            parent.addExchangeLocations(currentStageFragmentId, newTasks, noMoreTasks);

            if (!childOutputBufferManagers.isEmpty()) {
                // Add an output buffer to the child stages for each new task
                List<OutputBufferId> newOutputBuffers = newTasks.stream()
                        .map(task -> new OutputBufferId(task.getTaskId().getId()))
                        .collect(toImmutableList());
                for (OutputBufferManager child : childOutputBufferManagers) {
                    child.addOutputBuffers(newOutputBuffers, noMoreTasks);
                }
            }
        }
    }
}
