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
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.SqlStageExecution.ExchangeLocation;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.concurrent.SetThreadName;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SqlQueryScheduler
{
    private final QueryStateMachine queryStateMachine;
    private final Map<StageId, SqlStageExecution> stages;
    private final ExecutorService executor;
    private final StageId rootStageId;
    private final Map<StageId, Set<StageId>> stageChildren;

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
            NodeTaskMap nodeTaskMap)
    {
        this.queryStateMachine = queryStateMachine;

        // todo come up with a better way to build this, or eliminate this map
        ImmutableMap.Builder<StageId, Set<StageId>> stageChildren = ImmutableMap.builder();

        List<SqlStageExecution> stages = createStages(
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
                stageChildren);

        SqlStageExecution rootStage = stages.get(0);
        rootStage.setOutputBuffers(rootOutputBuffers);
        this.rootStageId = rootStage.getStageId();

        this.stages = stages.stream()
                .collect(toImmutableMap(SqlStageExecution::getStageId));
        this.stageChildren = stageChildren.build();

        this.executor = executor;

        rootStage.addStateChangeListener(state -> {
            if (state == StageState.FINISHED) {
                queryStateMachine.transitionToFinished();
            }
            else if (state == StageState.CANCELED) {
                // output stage was canceled
                queryStateMachine.transitionToFailed(new PrestoException(USER_CANCELED, "Query was canceled"));
            }
        });

        for (SqlStageExecution stage : stages) {
            stage.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }
                if (state == StageState.FAILED) {
                    queryStateMachine.transitionToFailed(stage.getStageInfo().getFailureCause().toException());
                }
                else if (state == StageState.ABORTED) {
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
            ImmutableMap.Builder<StageId, Set<StageId>> stageChildren)
    {
        ImmutableList.Builder<SqlStageExecution> stages = ImmutableList.builder();

        StageId stageId = new StageId(queryStateMachine.getQueryId(), String.valueOf(nextStageId.getAndIncrement()));
        SqlStageExecution stage = new SqlStageExecution(
                stageId,
                locationFactory.createStageLocation(stageId),
                plan.getFragment(),
                plan.getDataSource(),
                nodeScheduler,
                remoteTaskFactory,
                session,
                splitBatchSize,
                initialHashPartitions,
                nodeTaskMap,
                executor);

        stages.add(stage);

        ImmutableSet.Builder<StageId> childStages = ImmutableSet.builder();
        for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
            List<SqlStageExecution> subTree = createStages(
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
                    stageChildren);
            stages.addAll(subTree);

            SqlStageExecution childStage = subTree.get(0);
            linkParentChildStages(stage, childStage);
            childStages.add(childStage.getStageId());
        }
        stageChildren.put(stageId, childStages.build());

        return stages.build();
    }

    private static void linkParentChildStages(SqlStageExecution parentStage, SqlStageExecution childStage)
    {
        // when parent stage creates a task, add an output buffer to the child stage
        OutputBufferManager outputBufferManager;
        switch (childStage.getFragment().getOutputPartitioning()) {
            case NONE:
                outputBufferManager = new BroadcastOutputBufferManager(childStage::setOutputBuffers);
                break;
            case HASH:
                outputBufferManager = new PartitionedOutputBufferManager(childStage::setOutputBuffers, new HashPartitionFunctionGenerator(childStage.getFragment()));
                break;
            case ROUND_ROBIN:
                outputBufferManager = new PartitionedOutputBufferManager(childStage::setOutputBuffers, new RoundRobinPartitionFunctionGenerator());
                break;
            default:
                throw new UnsupportedOperationException("Unsupported output partitioning " + childStage.getFragment().getOutputPartitioning());
        }
        parentStage.addTaskCreationListener(remoteTask -> outputBufferManager.addOutputBuffer(remoteTask.getTaskInfo().getTaskId()));

        // when parent stage finishes scheduling, set no more output buffers on the child stage
        parentStage.addStateChangeListener(stageState -> {
            if (stageState != StageState.PLANNED && stageState != StageState.SCHEDULING) {
                outputBufferManager.noMoreOutputBuffers();
            }
        });

        // when child stage creates a task, add an exchange location to the parent stage
        PlanFragmentId childStageFragmentId = childStage.getFragment().getId();
        childStage.addTaskCreationListener(remoteTask -> parentStage.addExchangeLocation(new ExchangeLocation(childStageFragmentId, remoteTask.getTaskInfo().getSelf())));

        // when the child stage finishes scheduling, set no more exchange locations on the parent stage
        childStage.addStateChangeListener(state -> {
            switch (state) {
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
                    parentStage.noMoreExchangeLocationsFor(childStageFragmentId);
                case ABORTED:
                case FAILED:
                    // DO NOT complete a FAILED or ABORTED stage.  This will cause the
                    // stage above to finish normally, which will result in a query
                    // completing successfully when it should fail..
                    break;
            }
        });
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
        List<StageInfo> childStages = stageChildren.get(stageId).stream()
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
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            stages.values().stream()
                    .forEach(stage -> executor.submit(stage::startTasks));
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
}
