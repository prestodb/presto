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

import com.facebook.presto.Session;
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
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.getConcurrentLifespansPerNode;
import static com.facebook.presto.SystemSessionProperties.getMaxConcurrentMaterializations;
import static com.facebook.presto.SystemSessionProperties.getMaxTasksPerStage;
import static com.facebook.presto.SystemSessionProperties.getWriterMinSize;
import static com.facebook.presto.execution.BasicStageStats.aggregateBasicStageStats;
import static com.facebook.presto.execution.SqlStageExecution.createSqlStageExecution;
import static com.facebook.presto.execution.StageState.ABORTED;
import static com.facebook.presto.execution.StageState.CANCELED;
import static com.facebook.presto.execution.StageState.FAILED;
import static com.facebook.presto.execution.StageState.FINISHED;
import static com.facebook.presto.execution.StageState.PLANNED;
import static com.facebook.presto.execution.StageState.RUNNING;
import static com.facebook.presto.execution.StageState.SCHEDULED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createDiscardingOutputBuffers;
import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static com.facebook.presto.spi.ConnectorId.isInternalSystemConnector;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.collect.Streams.stream;
import static com.google.common.graph.Traverser.forTree;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
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
    private final SubPlan plan;
    private final StreamingPlanSection sectionedPlan;
    private final Map<StageId, SqlStageExecution> stages;
    private final ExecutorService executor;
    private final StageId rootStageId;
    private final Map<StageId, StageScheduler> stageSchedulers;
    private final Map<StageId, StageLinkage> stageLinkages;
    private final SplitSchedulerStats schedulerStats;
    private final boolean summarizeTaskInfo;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean scheduling = new AtomicBoolean();
    private final int maxConcurrentMaterializations;

    public static SqlQueryScheduler createSqlQueryScheduler(
            QueryStateMachine queryStateMachine,
            LocationFactory locationFactory,
            SubPlan plan,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
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
        SqlQueryScheduler sqlQueryScheduler = new SqlQueryScheduler(
                queryStateMachine,
                locationFactory,
                plan,
                nodePartitioningManager,
                nodeScheduler,
                remoteTaskFactory,
                splitSourceFactory,
                session,
                summarizeTaskInfo,
                splitBatchSize,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                rootOutputBuffers,
                nodeTaskMap,
                executionPolicy,
                schedulerStats);
        sqlQueryScheduler.initialize();
        return sqlQueryScheduler;
    }

    private SqlQueryScheduler(
            QueryStateMachine queryStateMachine,
            LocationFactory locationFactory,
            SubPlan plan,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
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
        this.plan = requireNonNull(plan, "plan is null");
        this.executionPolicy = requireNonNull(executionPolicy, "schedulerPolicyFactory is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.summarizeTaskInfo = summarizeTaskInfo;

        // todo come up with a better way to build this, or eliminate this map
        ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers = ImmutableMap.builder();
        ImmutableMap.Builder<StageId, StageLinkage> stageLinkages = ImmutableMap.builder();

        OutputBufferId rootBufferId = getOnlyElement(rootOutputBuffers.getBuffers().keySet());
        sectionedPlan = extractStreamingSections(plan);
        List<SqlStageExecution> stages = createStages(
                (fragmentId, tasks, noMoreExchangeLocations) -> updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations),
                locationFactory,
                sectionedPlan,
                Optional.of(new int[1]),
                rootOutputBuffers,
                nodeScheduler,
                remoteTaskFactory,
                splitSourceFactory,
                session,
                splitBatchSize,
                nodePartitioningManager,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                nodeTaskMap,
                stageSchedulers,
                stageLinkages);

        this.rootStageId = stages.get(0).getStageId();

        this.stages = stages.stream()
                .collect(toImmutableMap(SqlStageExecution::getStageId, identity()));

        this.stageSchedulers = stageSchedulers.build();
        this.stageLinkages = stageLinkages.build();

        this.executor = queryExecutor;
        this.maxConcurrentMaterializations = getMaxConcurrentMaterializations(session);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        SqlStageExecution rootStage = stages.get(rootStageId);
        rootStage.addStateChangeListener(state -> {
            if (state == FINISHED) {
                queryStateMachine.transitionToFinishing();
            }
            else if (state == CANCELED) {
                // output stage was canceled
                queryStateMachine.transitionToCanceled();
            }
        });

        for (SqlStageExecution stage : stages.values()) {
            stage.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }
                if (state == FAILED) {
                    queryStateMachine.transitionToFailed(stage.getStageInfo().getFailureCause().get().toException());
                }
                else if (state == ABORTED) {
                    // this should never happen, since abort can only be triggered in query clean up after the query is finished
                    queryStateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                }
                else if (state == FINISHED) {
                    // checks if there's any new sections available for execution and starts the scheduling if any
                    startScheduling();
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
                queryStateMachine.updateQueryInfo(Optional.of(getStageInfo()));
            }
        });
        for (SqlStageExecution stage : stages.values()) {
            stage.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.of(getStageInfo())));
        }
    }

    private static void updateQueryOutputLocations(QueryStateMachine queryStateMachine, OutputBufferId rootBufferId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations)
    {
        Map<URI, TaskId> bufferLocations = tasks.stream()
                .collect(toImmutableMap(
                        task -> getBufferLocation(task, rootBufferId),
                        RemoteTask::getTaskId));
        queryStateMachine.updateOutputLocations(bufferLocations, noMoreExchangeLocations);
    }

    private static URI getBufferLocation(RemoteTask remoteTask, OutputBufferId rootBufferId)
    {
        URI location = remoteTask.getTaskStatus().getSelf();
        return uriBuilderFrom(location).appendPath("results").appendPath(rootBufferId.toString()).build();
    }

    private List<SqlStageExecution> createStages(
            ExchangeLocationsConsumer locationsConsumer,
            LocationFactory locationFactory,
            StreamingPlanSection section,
            Optional<int[]> bucketToPartition,
            OutputBuffers outputBuffers,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            int splitBatchSize,
            NodePartitioningManager nodePartitioningManager,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers,
            ImmutableMap.Builder<StageId, StageLinkage> stageLinkages)
    {
        ImmutableList.Builder<SqlStageExecution> stages = ImmutableList.builder();

        // Only fetch a distribution once per section to ensure all stages see the same machine assignments
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();
        List<SqlStageExecution> sectionStages = createStreamingLinkedStages(
                locationsConsumer,
                locationFactory,
                section.getPlan().withBucketToPartition(bucketToPartition),
                nodeScheduler,
                remoteTaskFactory,
                splitSourceFactory,
                session,
                splitBatchSize,
                partitioningHandle -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle)),
                nodePartitioningManager,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                nodeTaskMap,
                stageSchedulers,
                stageLinkages,
                Optional.empty());
        sectionStages.get(0).setOutputBuffers(outputBuffers);
        stages.addAll(sectionStages);

        for (StreamingPlanSection childSection : section.getChildren()) {
            stages.addAll(createStages(
                    discardingLocationConsumer(),
                    locationFactory,
                    childSection,
                    Optional.empty(),
                    createDiscardingOutputBuffers(),
                    nodeScheduler,
                    remoteTaskFactory,
                    splitSourceFactory,
                    session,
                    splitBatchSize,
                    nodePartitioningManager,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    stageSchedulers,
                    stageLinkages));
        }

        return stages.build();
    }

    private List<SqlStageExecution> createStreamingLinkedStages(
            ExchangeLocationsConsumer parent,
            LocationFactory locationFactory,
            StreamingSubPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            int splitBatchSize,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            NodePartitioningManager nodePartitioningManager,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers,
            ImmutableMap.Builder<StageId, StageLinkage> stageLinkages,
            Optional<SqlStageExecution> parentStageExecution)
    {
        ImmutableList.Builder<SqlStageExecution> stages = ImmutableList.builder();

        PlanFragmentId fragmentId = plan.getFragment().getId();
        StageId stageId = getStageId(fragmentId);
        SqlStageExecution stage = createSqlStageExecution(
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
            // TODO: defer opening split sources when stage scheduling starts
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(plan.getFragment(), session);
            // nodes are selected dynamically based on the constraints of the splits and the system load
            Entry<PlanNodeId, SplitSource> entry = getOnlyElement(splitSources.entrySet());
            PlanNodeId planNodeId = entry.getKey();
            SplitSource splitSource = entry.getValue();
            ConnectorId connectorId = splitSource.getConnectorId();
            if (isInternalSystemConnector(connectorId)) {
                connectorId = null;
            }
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(connectorId);
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stage::getAllTasks);

            checkArgument(!plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution());
            stageSchedulers.put(stageId, newSourcePartitionedSchedulerAsStageScheduler(stage, planNodeId, splitSource, placementPolicy, splitBatchSize));
            bucketToPartition = Optional.of(new int[1]);
        }
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            bucketToPartition = Optional.of(new int[1]);
        }
        else {
            // TODO: defer opening split sources when stage scheduling starts
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(plan.getFragment(), session);
            if (!splitSources.isEmpty()) {
                // contains local source
                List<PlanNodeId> schedulingOrder = plan.getFragment().getTableScanSchedulingOrder();
                ConnectorId connectorId = partitioningHandle.getConnectorId().orElseThrow(IllegalStateException::new);
                List<ConnectorPartitionHandle> connectorPartitionHandles;
                boolean groupedExecutionForStage = plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution();
                if (groupedExecutionForStage) {
                    connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                    checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                }
                else {
                    connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                }

                BucketNodeMap bucketNodeMap;
                List<InternalNode> stageNodeList;
                if (plan.getFragment().getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                    // no non-replicated remote source
                    boolean dynamicLifespanSchedule = plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule();
                    bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule);

                    // verify execution is consistent with planner's decision on dynamic lifespan schedule
                    verify(bucketNodeMap.isDynamic() == dynamicLifespanSchedule);

                    if (!bucketNodeMap.isDynamic()) {
                        stageNodeList = ((FixedBucketNodeMap) bucketNodeMap).getBucketToNode().stream()
                                .distinct()
                                .collect(toImmutableList());
                    }
                    else {
                        stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(connectorId).selectRandomNodes(getMaxTasksPerStage(session)));
                    }
                    bucketToPartition = Optional.empty();
                }
                else {
                    // cannot use dynamic lifespan schedule
                    verify(!plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule());

                    // remote source requires nodePartitionMap
                    NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                    if (groupedExecutionForStage) {
                        checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                    }
                    stageNodeList = nodePartitionMap.getPartitionToNode();
                    bucketNodeMap = nodePartitionMap.asBucketNodeMap();
                    bucketToPartition = Optional.of(nodePartitionMap.getBucketToPartition());
                }

                FixedSourcePartitionedScheduler stageScheduler = new FixedSourcePartitionedScheduler(
                        stage,
                        splitSources,
                        plan.getFragment().getStageExecutionDescriptor(),
                        schedulingOrder,
                        stageNodeList,
                        bucketNodeMap,
                        splitBatchSize,
                        getConcurrentLifespansPerNode(session),
                        nodeScheduler.createNodeSelector(connectorId),
                        connectorPartitionHandles);
                stageSchedulers.put(stageId, stageScheduler);
                if (plan.getFragment().getStageExecutionDescriptor().isRecoverableGroupedExecution()) {
                    stage.registerStageTaskRecoveryCallback(taskId -> {
                        checkArgument(taskId.getStageId().equals(stageId), "The task did not execute this stage");
                        checkArgument(parentStageExecution.isPresent(), "Parent stage execution must exist");
                        checkArgument(parentStageExecution.get().getAllTasks().size() == 1, "Parent stage should only have one task for recoverable grouped execution");

                        parentStageExecution.get().removeRemoteSourceIfSingleTaskStage(taskId);
                        stageScheduler.recover(taskId);
                    });
                }
            }
            else {
                // all sources are remote
                NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                stageSchedulers.put(stageId, new FixedCountScheduler(stage, partitionToNode));
                bucketToPartition = Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }

        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        for (StreamingSubPlan stagePlan : plan.getChildren()) {
            List<SqlStageExecution> subTree = createStreamingLinkedStages(
                    stage::addExchangeLocations,
                    locationFactory,
                    stagePlan.withBucketToPartition(bucketToPartition),
                    nodeScheduler,
                    remoteTaskFactory,
                    splitSourceFactory,
                    session,
                    splitBatchSize,
                    partitioningCache,
                    nodePartitioningManager,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    stageSchedulers,
                    stageLinkages,
                    Optional.of(stage));
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

        stageLinkages.put(stageId, new StageLinkage(fragmentId, parent, childStages));

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

        return buildStageInfo(plan, stageInfos);
    }

    private StageInfo buildStageInfo(SubPlan subPlan, Map<StageId, StageInfo> stageInfos)
    {
        StageInfo stageInfo = stageInfos.get(getStageId(subPlan.getFragment().getId()));
        checkArgument(stageInfo != null, "No stageInfo for %s", stageInfo);
        if (subPlan.getChildren().isEmpty()) {
            return stageInfo;
        }
        return new StageInfo(
                stageInfo.getStageId(),
                stageInfo.getState(),
                stageInfo.getSelf(),
                stageInfo.getPlan(),
                stageInfo.getTypes(),
                stageInfo.getStageStats(),
                stageInfo.getTasks(),
                subPlan.getChildren().stream()
                        .map(plan -> buildStageInfo(plan, stageInfos))
                        .collect(toImmutableList()),
                stageInfo.getFailureCause());
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
            startScheduling();
        }
    }

    private void startScheduling()
    {
        requireNonNull(stages);
        // still scheduling the previous batch of stages
        if (scheduling.get()) {
            return;
        }
        executor.submit(this::schedule);
    }

    private void schedule()
    {
        if (!scheduling.compareAndSet(false, true)) {
            // still scheduling the previous batch of stages
            return;
        }

        List<SqlStageExecution> scheduledStages = new ArrayList<>();

        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            Set<StageId> completedStages = new HashSet<>();

            List<ExecutionSchedule> sectionExecutionSchedules = new LinkedList<>();

            while (!Thread.currentThread().isInterrupted()) {
                // remove finished section
                sectionExecutionSchedules.removeIf(ExecutionSchedule::isFinished);

                // try to pull more section that are ready to be run
                List<StreamingPlanSection> sectionsReadyForExecution = getSectionsReadyForExecution();

                // all finished
                if (sectionsReadyForExecution.isEmpty() && sectionExecutionSchedules.isEmpty()) {
                    break;
                }

                List<List<SqlStageExecution>> sectionStageExecutions = getStageExecutions(sectionsReadyForExecution);
                sectionStageExecutions.forEach(scheduledStages::addAll);
                sectionStageExecutions.stream()
                        .map(executionPolicy::createExecutionSchedule)
                        .forEach(sectionExecutionSchedules::add);

                while (sectionExecutionSchedules.stream().noneMatch(ExecutionSchedule::isFinished)) {
                    List<ListenableFuture<?>> blockedStages = new ArrayList<>();

                    List<SqlStageExecution> stagesToSchedule = sectionExecutionSchedules.stream()
                            .flatMap(schedule -> schedule.getStagesToSchedule().stream())
                            .collect(toImmutableList());

                    for (SqlStageExecution stage : stagesToSchedule) {
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
                                    schedulerStats.getMixedSplitQueuesFullAndWaitingForSource().update(1);
                                    break;
                                case NO_ACTIVE_DRIVER_GROUP:
                                    schedulerStats.getNoActiveDriverGroup().update(1);
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                            }
                        }
                    }

                    // make sure to update stage linkage at least once per loop to catch async state changes (e.g., partial cancel)
                    boolean stageFinishedExecution = false;
                    for (SqlStageExecution stage : scheduledStages) {
                        if (!completedStages.contains(stage.getStageId()) && stage.getState().isDone()) {
                            stageLinkages.get(stage.getStageId())
                                    .processScheduleResults(stage.getState(), ImmutableSet.of());
                            completedStages.add(stage.getStageId());
                            stageFinishedExecution = true;
                        }
                    }

                    // if any stage has just finished execution try to pull more sections for scheduling
                    if (stageFinishedExecution) {
                        break;
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
            }

            for (SqlStageExecution stage : scheduledStages) {
                StageState state = stage.getState();
                if (state != SCHEDULED && state != RUNNING && !state.isDone()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage %s is in state %s", stage.getStageId(), state));
                }
            }

            scheduling.set(false);

            if (!getSectionsReadyForExecution().isEmpty()) {
                startScheduling();
            }
        }
        catch (Throwable t) {
            scheduling.set(false);
            queryStateMachine.transitionToFailed(t);
            throw t;
        }
        finally {
            RuntimeException closeError = new RuntimeException();
            for (SqlStageExecution stage : scheduledStages) {
                try {
                    stageSchedulers.get(stage.getStageId()).close();
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

    private List<StreamingPlanSection> getSectionsReadyForExecution()
    {
        long runningPlanSections =
                stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                        .map(section -> getStageExecution(section.getPlan().getFragment().getId()).getState())
                        .filter(state -> !state.isDone() && state != PLANNED)
                        .count();
        return stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                // get all sections ready for execution
                .filter(this::isReadyForExecution)
                .limit(maxConcurrentMaterializations - runningPlanSections)
                .collect(toImmutableList());
    }

    private boolean isReadyForExecution(StreamingPlanSection section)
    {
        SqlStageExecution stage = getStageExecution(section.getPlan().getFragment().getId());
        if (stage.getState() != PLANNED) {
            // already scheduled
            return false;
        }
        for (StreamingPlanSection child : section.getChildren()) {
            SqlStageExecution childRootStage = getStageExecution(child.getPlan().getFragment().getId());
            if (childRootStage.getState() != FINISHED) {
                return false;
            }
        }
        return true;
    }

    private List<List<SqlStageExecution>> getStageExecutions(List<StreamingPlanSection> sections)
    {
        return sections.stream()
                .map(section -> stream(forTree(StreamingSubPlan::getChildren).depthFirstPreOrder(section.getPlan())).collect(toImmutableList()))
                .map(plans -> plans.stream()
                        .map(StreamingSubPlan::getFragment)
                        .map(PlanFragment::getId)
                        .map(this::getStageExecution)
                        .collect(toImmutableList()))
                .collect(toImmutableList());
    }

    private SqlStageExecution getStageExecution(PlanFragmentId planFragmentId)
    {
        return stages.get(getStageId(planFragmentId));
    }

    private StageId getStageId(PlanFragmentId fragmentId)
    {
        return new StageId(queryStateMachine.getQueryId(), fragmentId.getId());
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

    public static StreamingPlanSection extractStreamingSections(SubPlan subPlan)
    {
        ImmutableList.Builder<SubPlan> materializedExchangeChildren = ImmutableList.builder();
        StreamingSubPlan streamingSection = extractStreamingSection(subPlan, materializedExchangeChildren);
        return new StreamingPlanSection(
                streamingSection,
                materializedExchangeChildren.build().stream()
                        .map(SqlQueryScheduler::extractStreamingSections)
                        .collect(toImmutableList()));
    }

    private static StreamingSubPlan extractStreamingSection(SubPlan subPlan, ImmutableList.Builder<SubPlan> materializedExchangeChildren)
    {
        ImmutableList.Builder<StreamingSubPlan> streamingSources = ImmutableList.builder();
        Set<PlanFragmentId> streamingFragmentIds = subPlan.getFragment().getRemoteSourceNodes().stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .collect(toImmutableSet());
        for (SubPlan child : subPlan.getChildren()) {
            if (streamingFragmentIds.contains(child.getFragment().getId())) {
                streamingSources.add(extractStreamingSection(child, materializedExchangeChildren));
            }
            else {
                materializedExchangeChildren.add(child);
            }
        }
        return new StreamingSubPlan(subPlan.getFragment(), streamingSources.build());
    }

    private interface ExchangeLocationsConsumer
    {
        void addExchangeLocations(PlanFragmentId fragmentId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations);
    }

    private static ExchangeLocationsConsumer discardingLocationConsumer()
    {
        return (fragmentId, tasks, noMoreExchangeLocations) -> {};
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
                case FINISHED_TASK_SCHEDULING:
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

    private static class StreamingPlanSection
    {
        private final StreamingSubPlan plan;
        // materialized exchange children
        private final List<StreamingPlanSection> children;

        public StreamingPlanSection(StreamingSubPlan plan, List<StreamingPlanSection> children)
        {
            this.plan = requireNonNull(plan, "plan is null");
            this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
        }

        public StreamingSubPlan getPlan()
        {
            return plan;
        }

        public List<StreamingPlanSection> getChildren()
        {
            return children;
        }
    }

    /**
     * StreamingSubPlan is similar to SubPlan but only contains streaming children
     */
    private static class StreamingSubPlan
    {
        private final PlanFragment fragment;
        // streaming children
        private final List<StreamingSubPlan> children;

        public StreamingSubPlan(PlanFragment fragment, List<StreamingSubPlan> children)
        {
            this.fragment = requireNonNull(fragment, "fragment is null");
            this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
        }

        public PlanFragment getFragment()
        {
            return fragment;
        }

        public List<StreamingSubPlan> getChildren()
        {
            return children;
        }

        public StreamingSubPlan withBucketToPartition(Optional<int[]> bucketToPartition)
        {
            return new StreamingSubPlan(fragment.withBucketToPartition(bucketToPartition), children);
        }
    }
}
