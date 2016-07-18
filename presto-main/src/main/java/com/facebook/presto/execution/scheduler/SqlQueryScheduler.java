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
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.facebook.presto.connector.ConnectorManager.INFORMATION_SCHEMA_CONNECTOR_PREFIX;
import static com.facebook.presto.connector.ConnectorManager.SYSTEM_TABLES_CONNECTOR_PREFIX;
import static com.facebook.presto.execution.StageState.ABORTED;
import static com.facebook.presto.execution.StageState.CANCELED;
import static com.facebook.presto.execution.StageState.FAILED;
import static com.facebook.presto.execution.StageState.FINISHED;
import static com.facebook.presto.execution.StageState.RUNNING;
import static com.facebook.presto.execution.StageState.SCHEDULED;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.firstCompletedFuture;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SqlQueryScheduler
{
    private final QueryStateMachine queryStateMachine;
    private final ExecutionPolicy executionPolicy;
    private final Map<StageId, SqlStageExecution> stages;
    private final ExecutorService executor;
    private final StageId rootStageId;
    private final Map<StageId, StageScheduler> stageSchedulers;
    private final Map<StageId, StageLinkage> stageLinkages;
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
            ExecutorService executor,
            OutputBuffers rootOutputBuffers,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.executionPolicy = requireNonNull(executionPolicy, "schedulerPolicyFactory is null");
        this.summarizeTaskInfo = summarizeTaskInfo;

        // todo come up with a better way to build this, or eliminate this map
        ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers = ImmutableMap.builder();
        ImmutableMap.Builder<StageId, StageLinkage> stageLinkages = ImmutableMap.builder();

        // Only fetch a distribution once per query to assure all stages see the same machine assignments
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();

        List<SqlStageExecution> stages = createStages(
                Optional.empty(),
                new AtomicInteger(),
                locationFactory,
                plan.withBucketToPartition(Optional.of(new int[1])),
                nodeScheduler,
                remoteTaskFactory,
                session,
                splitBatchSize,
                partitioningHandle -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle)),
                executor,
                nodeTaskMap,
                stageSchedulers,
                stageLinkages);

        SqlStageExecution rootStage = stages.get(0);
        rootStage.setOutputBuffers(rootOutputBuffers);
        this.rootStageId = rootStage.getStageId();

        this.stages = stages.stream()
                .collect(toImmutableMap(SqlStageExecution::getStageId));

        this.stageSchedulers = stageSchedulers.build();
        this.stageLinkages = stageLinkages.build();

        this.executor = executor;

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
    }

    private List<SqlStageExecution> createStages(
            Optional<SqlStageExecution> parent,
            AtomicInteger nextStageId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int splitBatchSize,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            ExecutorService executor,
            NodeTaskMap nodeTaskMap,
            ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers,
            ImmutableMap.Builder<StageId, StageLinkage> stageLinkages)
    {
        ImmutableList.Builder<SqlStageExecution> stages = ImmutableList.builder();

        StageId stageId = new StageId(queryStateMachine.getQueryId(), String.valueOf(nextStageId.getAndIncrement()));
        SqlStageExecution stage = new SqlStageExecution(
                stageId,
                locationFactory.createStageLocation(stageId),
                plan.getFragment(),
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                nodeTaskMap,
                executor);

        stages.add(stage);

        Optional<int[]> bucketToPartition;
        PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
            // nodes are selected dynamically based on the constraints of the splits and the system load
            Entry<PlanNodeId, SplitSource> entry = Iterables.getOnlyElement(plan.getSplitSources().entrySet());
            String dataSourceName = entry.getValue().getDataSourceName();
            if (dataSourceName.startsWith(SYSTEM_TABLES_CONNECTOR_PREFIX) || dataSourceName.startsWith(INFORMATION_SCHEMA_CONNECTOR_PREFIX)) {
                dataSourceName = null;
            }
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(dataSourceName);
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stage::getAllTasks);
            stageSchedulers.put(stageId, new SourcePartitionedScheduler(stage, entry.getKey(), entry.getValue(), placementPolicy, splitBatchSize));
            bucketToPartition = Optional.of(new int[1]);
        }
        else {
            // nodes are pre determined by the nodePartitionMap
            NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());

            Map<PlanNodeId, SplitSource> splitSources = plan.getSplitSources();
            if (!splitSources.isEmpty()) {
                stageSchedulers.put(stageId, new FixedSourcePartitionedScheduler(
                        stage,
                        splitSources,
                        plan.getFragment().getPartitionedSources(),
                        nodePartitionMap,
                        splitBatchSize,
                        nodeScheduler.createNodeSelector(null)));
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
                    Optional.of(stage),
                    nextStageId,
                    locationFactory,
                    subStagePlan.withBucketToPartition(bucketToPartition),
                    nodeScheduler,
                    remoteTaskFactory,
                    session,
                    splitBatchSize,
                    partitioningCache,
                    executor,
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
                childStages.stream().forEach(SqlStageExecution::cancel);
            }
        });

        stageLinkages.put(stageId, new StageLinkage(plan.getFragment().getId(), parent, childStages));

        return stages.build();
    }

    public StageInfo getStageInfo()
    {
        Map<StageId, StageInfo> stageInfos = stages.values().stream()
                .map(SqlStageExecution::getStageInfo)
                .collect(toImmutableMap(StageInfo::getStageId));

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

    public long getTotalMemoryReservation()
    {
        return stages.values().stream()
                .mapToLong(SqlStageExecution::getMemoryReservation)
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
                List<CompletableFuture<?>> blockedStages = new ArrayList<>();
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
                    tryGetFutureValue(firstCompletedFuture(blockedStages), 100, MILLISECONDS);
                    for (CompletableFuture<?> blockedStage : blockedStages) {
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
            throw Throwables.propagate(t);
        }
        finally {
            RuntimeException closeError = new RuntimeException();
            for (StageScheduler scheduler : stageSchedulers.values()) {
                try {
                    scheduler.close();
                }
                catch (Throwable t) {
                    queryStateMachine.transitionToFailed(t);
                    closeError.addSuppressed(t);
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
            stages.values().stream()
                    .forEach(SqlStageExecution::abort);
        }
    }

    private static class StageLinkage
    {
        private final PlanFragmentId currentStageFragmentId;
        private final Optional<SqlStageExecution> parent;
        private final Set<OutputBufferManager> childOutputBufferManagers;
        private final Set<StageId> childStageIds;

        public StageLinkage(PlanFragmentId fragmentId, Optional<SqlStageExecution> parent, Set<SqlStageExecution> children)
        {
            this.currentStageFragmentId = fragmentId;
            this.parent = parent;
            this.childOutputBufferManagers = children.stream()
                    .map(childStage -> {
                        if (childStage.getFragment().getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION)) {
                            return new BroadcastOutputBufferManager(childStage::setOutputBuffers);
                        }
                        else {
                            int partitionCount = Ints.max(childStage.getFragment().getPartitioningScheme().getBucketToPartition().get()) + 1;
                            return new PartitionedOutputBufferManager(partitionCount, childStage::setOutputBuffers);
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

            if (parent.isPresent()) {
                // Add an exchange location to the parent stage for each new task
                Set<URI> newExchangeLocations = newTasks.stream()
                        .map(task -> task.getTaskStatus().getSelf())
                        .collect(toImmutableSet());
                parent.get().addExchangeLocations(currentStageFragmentId, newExchangeLocations, noMoreTasks);
            }

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
