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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.execution.ForQueryExecution;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.server.remotetask.HttpRemoteTask;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmenterUtils;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.getConcurrentLifespansPerNode;
import static com.facebook.presto.SystemSessionProperties.getMaxTasksPerStage;
import static com.facebook.presto.SystemSessionProperties.getWriterMinSize;
import static com.facebook.presto.SystemSessionProperties.isOptimizedScaleWriterProducerBuffer;
import static com.facebook.presto.execution.SqlStageExecution.createSqlStageExecution;
import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerWithSplitRetryAsStageScheduler;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.spi.ConnectorId.isInternalSystemConnector;
import static com.facebook.presto.spi.NodePoolType.INTERMEDIATE;
import static com.facebook.presto.spi.NodePoolType.LEAF;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class SectionExecutionFactory
{
    private static final Logger log = Logger.get(SectionExecutionFactory.class);
    private static final String RUNTIME_STATS_RETRIED_SPLITS_PREFIX = "retried-splits-from-node-";
    public static final int SPLIT_RETRY_BATCH_SIZE = 100;
    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeTaskMap nodeTaskMap;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final FailureDetector failureDetector;
    private final SplitSchedulerStats schedulerStats;
    private final NodeScheduler nodeScheduler;
    private final int splitBatchSize;
    private final boolean isEnableWorkerIsolation;
    private final InternalNodeManager nodeManager;
    private final boolean isEnableGracefulShutdown;
    private final boolean isRetryOfFailedSplitsEnabled;

    @Inject
    public SectionExecutionFactory(
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            NodeTaskMap nodeTaskMap,
            @ForQueryExecution ExecutorService executor,
            @ForScheduler ScheduledExecutorService scheduledExecutor,
            FailureDetector failureDetector,
            SplitSchedulerStats schedulerStats,
            NodeScheduler nodeScheduler,
            QueryManagerConfig queryManagerConfig,
            InternalNodeManager nodeManager)
    {
        this(
                metadata,
                nodePartitioningManager,
                nodeTaskMap,
                executor,
                scheduledExecutor,
                failureDetector,
                schedulerStats,
                nodeScheduler,
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getScheduleSplitBatchSize(),
                queryManagerConfig.isEnableWorkerIsolation(),
                queryManagerConfig.isEnableGracefulShutdown(),
                queryManagerConfig.isEnableRetryForFailedSplits(),
                nodeManager);
    }

    public SectionExecutionFactory(
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            NodeTaskMap nodeTaskMap,
            ExecutorService executor,
            ScheduledExecutorService scheduledExecutor,
            FailureDetector failureDetector,
            SplitSchedulerStats schedulerStats,
            NodeScheduler nodeScheduler,
            int splitBatchSize,
            boolean isEnableWorkerIsolation,
            boolean isEnableGracefulShutdown,
            boolean isRetryOfFailedSplitsEnabled,
            InternalNodeManager nodeManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.splitBatchSize = splitBatchSize;
        this.isEnableGracefulShutdown = isEnableGracefulShutdown;
        this.isRetryOfFailedSplitsEnabled = isRetryOfFailedSplitsEnabled;
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.isEnableWorkerIsolation = isEnableWorkerIsolation;
        if (isEnableGracefulShutdown || isRetryOfFailedSplitsEnabled) {
            checkState(isEnableWorkerIsolation, "Worker isolation needs to be enabled for enabling retry of failed leaf splits");
        }

        if (isRetryOfFailedSplitsEnabled) {
            checkState(isEnableGracefulShutdown, "Graceful shutdown needs to be enabled for enabling retry of failed leaf splits");
        }
    }

    /**
     * returns a List of SectionExecutions in a postorder representation of the tree
     */
    public SectionExecution createSectionExecutions(
            Session session,
            StreamingPlanSection section,
            ExchangeLocationsConsumer locationsConsumer,
            Optional<int[]> bucketToPartition,
            OutputBuffers outputBuffers,
            boolean summarizeTaskInfo,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            int attemptId)
    {
        // Only fetch a distribution once per section to ensure all stages see the same machine assignments
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();
        TableWriteInfo tableWriteInfo = createTableWriteInfo(section.getPlan(), metadata, session);
        List<StageExecutionAndScheduler> sectionStages = createStreamingLinkedStageExecutions(
                session,
                locationsConsumer,
                section.getPlan().withBucketToPartition(bucketToPartition),
                partitioningHandle -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle, getNodePoolSelectionPredicate(section.getPlan(), session, handle))),
                tableWriteInfo,
                Optional.empty(),
                summarizeTaskInfo,
                remoteTaskFactory,
                splitSourceFactory,
                attemptId);
        StageExecutionAndScheduler rootStage = getLast(sectionStages);
        rootStage.getStageExecution().setOutputBuffers(outputBuffers);
        return new SectionExecution(rootStage, sectionStages);
    }

    /**
     * returns a List of StageExecutionAndSchedulers in a postorder representation of the tree
     */
    private List<StageExecutionAndScheduler> createStreamingLinkedStageExecutions(
            Session session,
            ExchangeLocationsConsumer parent,
            StreamingSubPlan plan,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            TableWriteInfo tableWriteInfo,
            Optional<SqlStageExecution> parentStageExecution,
            boolean summarizeTaskInfo,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            int attemptId)
    {
        ImmutableList.Builder<StageExecutionAndScheduler> stageExecutionAndSchedulers = ImmutableList.builder();

        PlanFragmentId fragmentId = plan.getFragment().getId();
        StageId stageId = new StageId(session.getQueryId(), fragmentId.getId());
        SqlStageExecution stageExecution = createSqlStageExecution(
                new StageExecutionId(stageId, attemptId),
                plan.getFragment(),
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                nodeTaskMap,
                executor,
                failureDetector,
                schedulerStats,
                tableWriteInfo,
                isEnableGracefulShutdown,
                isRetryOfFailedSplitsEnabled);

        PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
        List<RemoteSourceNode> remoteSourceNodes = plan.getFragment().getRemoteSourceNodes();
        Optional<int[]> bucketToPartition = getBucketToPartition(partitioningHandle, partitioningCache, plan.getFragment().getRoot(), remoteSourceNodes);

        // create child stages
        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        for (StreamingSubPlan stagePlan : plan.getChildren()) {
            List<StageExecutionAndScheduler> subTree = createStreamingLinkedStageExecutions(
                    session,
                    stageExecution::addExchangeLocations,
                    stagePlan.withBucketToPartition(bucketToPartition),
                    partitioningCache,
                    tableWriteInfo,
                    Optional.of(stageExecution),
                    summarizeTaskInfo,
                    remoteTaskFactory,
                    splitSourceFactory,
                    attemptId);
            stageExecutionAndSchedulers.addAll(subTree);
            childStagesBuilder.add(getLast(subTree).getStageExecution());
        }
        Set<SqlStageExecution> childStageExecutions = childStagesBuilder.build();
        stageExecution.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                childStageExecutions.forEach(SqlStageExecution::cancel);
            }
        });

        StageLinkage stageLinkage = new StageLinkage(fragmentId, parent, childStageExecutions);
        StageScheduler stageScheduler = createStageScheduler(
                splitSourceFactory,
                session,
                plan,
                partitioningCache,
                parentStageExecution,
                stageId,
                stageExecution,
                partitioningHandle,
                tableWriteInfo,
                childStageExecutions);
        log.info("stageScheduler for the query %s and plan fragment %s = %s", session.getQueryId(), plan.getFragment().getId(), stageScheduler.getClass().getSimpleName());
        stageExecutionAndSchedulers.add(new StageExecutionAndScheduler(
                stageExecution,
                stageLinkage,
                stageScheduler));

        return stageExecutionAndSchedulers.build();
    }

    private StageScheduler createStageScheduler(
            SplitSourceFactory splitSourceFactory,
            Session session,
            StreamingSubPlan plan,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            Optional<SqlStageExecution> parentStageExecution,
            StageId stageId,
            SqlStageExecution stageExecution,
            PartitioningHandle partitioningHandle,
            TableWriteInfo tableWriteInfo,
            Set<SqlStageExecution> childStageExecutions)
    {
        Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(plan.getFragment(), session, tableWriteInfo);
        int maxTasksPerStage = getMaxTasksPerStage(session);
        Optional<Predicate<Node>> nodePredicate = getNodePoolSelectionPredicate(plan, session, partitioningHandle);
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
            log.info("partitioningHandle is SOURCE_DISTRIBUTION for the query %s and plan %s", session.getQueryId(), plan.getFragment().getId());
            // nodes are selected dynamically based on the constraints of the splits and the system load
            Map.Entry<PlanNodeId, SplitSource> entry = getOnlyElement(splitSources.entrySet());
            PlanNodeId planNodeId = entry.getKey();
            SplitSource splitSource = entry.getValue();
            ConnectorId connectorId = splitSource.getConnectorId();
            if (isInternalSystemConnector(connectorId)) {
                connectorId = null;
            }
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, connectorId, maxTasksPerStage, nodePredicate);
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stageExecution::getAllTasks);

            if (plan.getFragment().isLeaf() && (isEnableGracefulShutdown || isRetryOfFailedSplitsEnabled)) {
                stageExecution.registerTaskGracefulShutdownCallback((failedTaskId) -> {
                    Set<String> activeNodeIDs = nodeManager.getNodes(ACTIVE).stream().map(InternalNode::getNodeIdentifier).collect(toImmutableSet());
                    HttpRemoteTask taskToRecover = stageExecution.getAllTasks().stream()
                            .filter(task -> task.getTaskId().equals(failedTaskId))
                            .filter(task -> task instanceof HttpRemoteTask)
                            .map(task -> (HttpRemoteTask) task)
                            .collect(onlyElement());
                    String failingNodeID = taskToRecover.getNodeId();

                    log.warn("Going to recover task - %s, failed on node = %s", failedTaskId, failingNodeID);

                    List<HttpRemoteTask> activeRemoteTasks = stageExecution.getAllTasks().stream()
                            .filter(task -> !task.getTaskId().equals(failedTaskId))
                            .filter(task -> !task.getNodeId().equals(failingNodeID))
                            .filter(task -> activeNodeIDs.contains(task.getNodeId()))
                            .filter(task -> task instanceof HttpRemoteTask)
                            .map(task -> (HttpRemoteTask) task)
                            .filter(task -> task.getTaskStatus().getState() == TaskState.RUNNING)
                            .collect(toList());

                    if (activeRemoteTasks.isEmpty()) {
                        throw new PrestoException(REMOTE_TASK_ERROR, String.format("Running out of the eligible remote tasks to recover task %s", failedTaskId));
                    }

                    Collections.shuffle(activeRemoteTasks);

                    synchronized (stageExecution) {
                        Collection<ScheduledSplit> allUnprocessedSplits = taskToRecover.getAllUnprocessedSplits(planNodeId);
                        Iterator<List<ScheduledSplit>> splits = Iterables.partition(allUnprocessedSplits,
                                SPLIT_RETRY_BATCH_SIZE).iterator();
                        log.info("Need to retry %s number of splits for the failed task %s", allUnprocessedSplits.size(), failedTaskId);

                        while (splits.hasNext()) {
                            for (int i = 0; i < activeRemoteTasks.size() && splits.hasNext(); i++) {
                                HttpRemoteTask httpRemoteTask = activeRemoteTasks.get(i);
                                List<ScheduledSplit> scheduledSplit = splits.next();
                                List<Long> splitIds = scheduledSplit.stream().map(ScheduledSplit::getSequenceId).collect(toImmutableList());

                                log.warn("Going to retry splits %s of failed task %s on active task %s", splitIds, failedTaskId, httpRemoteTask.getTaskId());

                                Multimap<PlanNodeId, Split> splitsToAdd = HashMultimap.create();
                                splitsToAdd.putAll(planNodeId, scheduledSplit.stream().map(ScheduledSplit::getSplit).collect(toImmutableList()));

                                //FIXME metric to get the retried split information, add time element to it (how long its taking for coordinator to detect the failure)
                                RuntimeStats splitRetryStats = new RuntimeStats();
                                //node and task we are retrying from and destination node and task we are retrying to
                                String retryMetricName = new StringBuilder(RUNTIME_STATS_RETRIED_SPLITS_PREFIX)
                                        .append(failingNodeID).append("-task:" + getTaskIdentifier(failedTaskId))
                                        .append("->")
                                        .append(httpRemoteTask.getNodeId()).append("-task:" + getTaskIdentifier(httpRemoteTask.getTaskId()))
                                        .toString();
                                //track how many splits we are retrying from the source task
                                splitRetryStats.addMetricValue(retryMetricName, RuntimeUnit.NONE, scheduledSplit.size());
                                session.getRuntimeStats().update(splitRetryStats);
                                boolean taskIsNotDone = httpRemoteTask.addSplits(splitsToAdd);
                                if (!taskIsNotDone) {
                                    throw new RuntimeException(String.format("Error adding split %s for retry to task %s", splitIds, httpRemoteTask.getTaskId()));
                                }
                            }
                        }
                    }
                });
            }

            checkArgument(!plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution());
            if ((isEnableGracefulShutdown || isRetryOfFailedSplitsEnabled) && plan.getFragment().isLeaf() && !PlanFragment.containLocalExchange(plan.getFragment().getRoot())) {
                return newSourcePartitionedSchedulerWithSplitRetryAsStageScheduler(stageExecution, planNodeId, splitSource, placementPolicy, splitBatchSize);
            }
            else {
                return newSourcePartitionedSchedulerAsStageScheduler(stageExecution, planNodeId, splitSource, placementPolicy, splitBatchSize);
            }
        }
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            log.info("partitioningHandle is SCALED_WRITER_DISTRIBUTION for the query %s and plan %s", session.getQueryId(), plan.getFragment());
            Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStageExecutions.stream()
                    .map(SqlStageExecution::getAllTasks)
                    .flatMap(Collection::stream)
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Supplier<Collection<TaskStatus>> writerTasksProvider = () -> stageExecution.getAllTasks().stream()
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Optional<Integer> taskNumberIfScaledWriter = PlanFragmenterUtils.getTableWriterTasks(plan.getFragment().getRoot());

            ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                    stageExecution,
                    sourceTasksProvider,
                    writerTasksProvider,
                    nodeScheduler.createNodeSelector(session, null, nodePredicate),
                    scheduledExecutor,
                    getWriterMinSize(session),
                    isOptimizedScaleWriterProducerBuffer(session),
                    taskNumberIfScaledWriter);
            whenAllStages(childStageExecutions, StageExecutionState::isDone)
                    .addListener(scheduler::finish, directExecutor());
            return scheduler;
        }
        else {
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
                    bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule, nodePredicate);

                    // verify execution is consistent with planner's decision on dynamic lifespan schedule
                    verify(bucketNodeMap.isDynamic() == dynamicLifespanSchedule);

                    if (bucketNodeMap.hasInitialMap()) {
                        stageNodeList = bucketNodeMap.getBucketToNode().get().stream()
                                .distinct()
                                .collect(toImmutableList());
                    }
                    else {
                        stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(session, connectorId, nodePredicate).selectRandomNodes(maxTasksPerStage));
                    }
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
                }

                FixedSourcePartitionedScheduler fixedSourcePartitionedScheduler = new FixedSourcePartitionedScheduler(
                        stageExecution,
                        splitSources,
                        plan.getFragment().getStageExecutionDescriptor(),
                        schedulingOrder,
                        stageNodeList,
                        bucketNodeMap,
                        splitBatchSize,
                        getConcurrentLifespansPerNode(session),
                        nodeScheduler.createNodeSelector(session, connectorId, nodePredicate),
                        connectorPartitionHandles);
                if (plan.getFragment().getStageExecutionDescriptor().isRecoverableGroupedExecution()) {
                    stageExecution.registerStageTaskRecoveryCallback(taskId -> {
                        checkArgument(taskId.getStageExecutionId().getStageId().equals(stageId), "The task did not execute this stage");
                        checkArgument(parentStageExecution.isPresent(), "Parent stage execution must exist");
                        checkArgument(parentStageExecution.get().getAllTasks().size() == 1, "Parent stage should only have one task for recoverable grouped execution");

                        parentStageExecution.get().removeRemoteSourceIfSingleTaskStage(taskId);
                        fixedSourcePartitionedScheduler.recover(taskId);
                    });
                }
                return fixedSourcePartitionedScheduler;
            }

            else {
                // all sources are remote
                NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                return new FixedCountScheduler(stageExecution, partitionToNode);
            }
        }
    }

    private String getTaskIdentifier(com.facebook.presto.execution.TaskId failedTask)
    {
        return new StringBuilder()
                .append("S")
                .append(failedTask.getStageExecutionId().getStageId().getId())
                .append(".")
                .append(failedTask.getStageExecutionId().getId())
                .append(".")
                .append(failedTask.getId())
                .append(".")
                .append(failedTask.getAttemptNumber())
                .toString();
    }

    private Optional<Predicate<Node>> getNodePoolSelectionPredicate(StreamingSubPlan plan, Session session, PartitioningHandle partitioningHandle)
    {
        if (!isEnableWorkerIsolation) {
            //skipping node pool based selection for grouped execution
            return Optional.empty();
        }

        boolean hasLocalExchangeAtRoot = PlanFragment.containLocalExchange(plan.getFragment().getRoot());
        boolean isLeafFragment = isLeafSourceFragment(plan, partitioningHandle);
        boolean isGroupedExecution = plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution();
        NodePoolType workerPoolType = !isLeafFragment || hasLocalExchangeAtRoot || isGroupedExecution ? INTERMEDIATE : LEAF;
        //avoid splitting node for jmx connector
        if (isLeafFragment && plan.getFragment().getRoot() instanceof TableScanNode && ((TableScanNode) plan.getFragment().getRoot()).getTable().getConnectorId().getCatalogName().equals("jmx")) {
            return Optional.empty();
        }

        return Optional.of(node -> node.getPoolType().equals(workerPoolType));
    }

    private boolean isLeafSourceFragment(StreamingSubPlan plan, PartitioningHandle partitioningHandle)
    {
        boolean isTpch = partitioningHandle.getConnectorId().isPresent() && partitioningHandle.getConnectorId().get().getCatalogName().equals("tpch");
        return plan.getFragment().isLeaf() && (partitioningHandle.equals(SOURCE_DISTRIBUTION) || isTpch);
    }

    private static Optional<int[]> getBucketToPartition(
            PartitioningHandle partitioningHandle,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            PlanNode fragmentRoot,
            List<RemoteSourceNode> remoteSourceNodes)
    {
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            return Optional.of(new int[1]);
        }
        else if (PlanNodeSearcher.searchFrom(fragmentRoot).where(node -> node instanceof TableScanNode).findFirst().isPresent()) {
            if (remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                return Optional.empty();
            }
            else {
                // remote source requires nodePartitionMap
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                return Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }
        else {
            NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
            List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
            // todo this should asynchronously wait a standard timeout period before failing
            checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
            return Optional.of(nodePartitionMap.getBucketToPartition());
        }
    }

    private static ListenableFuture<?> whenAllStages(Collection<SqlStageExecution> stageExecutions, Predicate<StageExecutionState> predicate)
    {
        checkArgument(!stageExecutions.isEmpty(), "stageExecutions is empty");
        Set<StageExecutionId> stageIds = newConcurrentHashSet(stageExecutions.stream()
                .map(SqlStageExecution::getStageExecutionId)
                .collect(toSet()));
        SettableFuture<?> future = SettableFuture.create();

        for (SqlStageExecution stage : stageExecutions) {
            stage.addStateChangeListener(state -> {
                if (predicate.test(state) && stageIds.remove(stage.getStageExecutionId()) && stageIds.isEmpty()) {
                    future.set(null);
                }
            });
        }

        return future;
    }
}
