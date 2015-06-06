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
import com.facebook.presto.Session;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.NodeScheduler;
import com.facebook.presto.execution.NodeScheduler.NodeSelector;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.SqlStageExecution.ExchangeLocation;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.concurrent.SetThreadName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.execution.StageState.ABORTED;
import static com.facebook.presto.execution.StageState.CANCELED;
import static com.facebook.presto.execution.StageState.FAILED;
import static com.facebook.presto.execution.StageState.FINISHED;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
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
    private final AtomicBoolean started = new AtomicBoolean();

    public SqlQueryScheduler(QueryStateMachine queryStateMachine,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int splitBatchSize,
            int initialHashPartitions,
            ExecutorService executor,
            OutputBuffers rootOutputBuffers,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.executionPolicy = requireNonNull(executionPolicy, "schedulerPolicyFactory is null");

        // todo come up with a better way to build this, or eliminate this map
        ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers = ImmutableMap.builder();
        ImmutableMap.Builder<StageId, StageLinkage> stageLinkages = ImmutableMap.builder();

        List<SqlStageExecution> stages = createStages(
                Optional.empty(),
                new AtomicInteger(),
                locationFactory,
                plan,
                nodeScheduler,
                remoteTaskFactory,
                session,
                splitBatchSize,
                initialHashPartitions,
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
                queryStateMachine.transitionToFinished();
            }
            else if (state == CANCELED) {
                // output stage was canceled
                queryStateMachine.transitionToFailed(new PrestoException(USER_CANCELED, "Query was canceled"));
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
                    queryStateMachine.transitionToFailed(new PrestoException(INTERNAL_ERROR, "Query stage was aborted"));
                }
                else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                    // if the stage has at least one task, we are running
                    if (!stage.getAllTasks().isEmpty()) {
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
            int initialHashPartitions,
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
                nodeTaskMap,
                executor);

        stages.add(stage);

        if (plan.getFragment().getDistribution() == PlanDistribution.SINGLE) {
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(null);
            stageSchedulers.put(stageId, new FixedCountScheduler(stage, nodeSelector, 1));
        }
        else if (plan.getFragment().getDistribution() == PlanDistribution.FIXED) {
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(null);
            stageSchedulers.put(stageId, new FixedCountScheduler(stage, nodeSelector, initialHashPartitions));
        }
        else if (plan.getFragment().getDistribution() == PlanDistribution.COORDINATOR_ONLY) {
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(null);
            stageSchedulers.put(stageId, new CurrentNodeScheduler(stage, nodeSelector));
        }
        else if (plan.getFragment().getDistribution() == PlanDistribution.SOURCE) {
            SplitSource splitSource = plan.getDataSource().get();
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(splitSource.getDataSourceName());
            stageSchedulers.put(stageId, new SourcePartitionedScheduler(stage, splitSource, new SplitPlacementPolicy(nodeSelector, stage::getAllTasks), splitBatchSize));
        }
        else {
            throw new IllegalStateException("Unsupported partitioning: " + plan.getFragment().getDistribution());
        }

        ImmutableSet.Builder<SqlStageExecution> childStages = ImmutableSet.builder();
        for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
            List<SqlStageExecution> subTree = createStages(
                    Optional.of(stage),
                    nextStageId,
                    locationFactory,
                    subStagePlan,
                    nodeScheduler,
                    remoteTaskFactory,
                    session,
                    splitBatchSize,
                    initialHashPartitions,
                    executor,
                    nodeTaskMap,
                    stageSchedulers,
                    stageLinkages);
            stages.addAll(subTree);

            SqlStageExecution childStage = subTree.get(0);
            childStages.add(childStage);
        }
        stageLinkages.put(stageId, new StageLinkage(plan.getFragment().getId(), parent, childStages.build()));

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

    public void start()
    {
        if (started.compareAndSet(false, true)) {
            executor.submit(this::schedule);
        }
    }

    private void schedule()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
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

                // wait for a state change and then schedule again
                if (!blockedStages.isEmpty()) {
                    tryGetFutureValue(firstCompletedFuture(blockedStages), 100, MILLISECONDS);
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
                        switch (childStage.getFragment().getOutputPartitioning()) {
                            case NONE:
                                return new BroadcastOutputBufferManager(childStage::setOutputBuffers);
                            case HASH:
                                return new PartitionedOutputBufferManager(childStage::setOutputBuffers, new HashPartitionFunctionGenerator(childStage.getFragment()));
                            case ROUND_ROBIN:
                                return new PartitionedOutputBufferManager(childStage::setOutputBuffers, new RoundRobinPartitionFunctionGenerator());
                            default:
                                throw new UnsupportedOperationException("Unsupported output partitioning " + childStage.getFragment().getOutputPartitioning());
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
            for (RemoteTask remoteTask : newTasks) {
                if (parent.isPresent()) {
                    // when a task is created, add an exchange location to the parent stage
                    parent.get().addExchangeLocation(new ExchangeLocation(currentStageFragmentId, remoteTask.getTaskInfo().getSelf()));
                }
                // when a task is created, add an output buffer to the child stages
                childOutputBufferManagers.forEach(child -> child.addOutputBuffer(remoteTask.getTaskInfo().getTaskId()));
            }

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
                    if (parent.isPresent()) {
                        parent.get().noMoreExchangeLocationsFor(currentStageFragmentId);
                    }
                    childOutputBufferManagers.forEach(OutputBufferManager::noMoreOutputBuffers);
                case ABORTED:
                case FAILED:
                    // DO NOT complete a FAILED or ABORTED stage.  This will cause the
                    // stage above to finish normally, which will result in a query
                    // completing successfully when it should fail..
                    break;
            }
        }
    }
}
