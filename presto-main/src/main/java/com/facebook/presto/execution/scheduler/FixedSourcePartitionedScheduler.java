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

import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FixedSourcePartitionedScheduler
        implements StageScheduler
{
    private final SqlStageExecution stage;
    private final NodePartitionMap partitioning;
    private final Queue<SourcePartitionedScheduler> sourcePartitionedSchedulers;
    private boolean scheduledTasks;

    public FixedSourcePartitionedScheduler(
            SqlStageExecution stage,
            Map<PlanNodeId, SplitSource> splitSources,
            List<PlanNodeId> schedulingOrder,
            NodePartitionMap partitioning,
            int splitBatchSize,
            NodeSelector nodeSelector)
    {
        requireNonNull(stage, "stage is null");
        requireNonNull(splitSources, "splitSources is null");
        requireNonNull(partitioning, "partitioning is null");

        this.stage = stage;
        this.partitioning = partitioning;

        checkArgument(splitSources.keySet().equals(ImmutableSet.copyOf(schedulingOrder)));

        FixedSplitPlacementPolicy splitPlacementPolicy = new FixedSplitPlacementPolicy(nodeSelector, partitioning, stage::getAllTasks);
        sourcePartitionedSchedulers = new ArrayDeque<>(schedulingOrder.stream()
                .map(sourceId -> new SourcePartitionedScheduler(stage, sourceId, splitSources.get(sourceId), splitPlacementPolicy, splitBatchSize))
                .collect(Collectors.toList()));
    }

    @Override
    public ScheduleResult schedule()
    {
        // schedule a task on every node in the distribution
        List<RemoteTask> newTasks = ImmutableList.of();
        if (!scheduledTasks) {
            newTasks = partitioning.getPartitionToNode().entrySet().stream()
                    .map(entry -> stage.scheduleTask(entry.getValue(), entry.getKey()))
                    .collect(toImmutableList());
            scheduledTasks = true;
        }

        CompletableFuture<?> blocked = CompletableFuture.completedFuture(null);
        while (!sourcePartitionedSchedulers.isEmpty()) {
            ScheduleResult schedule = sourcePartitionedSchedulers.peek().schedule();
            blocked = schedule.getBlocked();
            // if the source is not done scheduling, stop scheduling for now
            if (!blocked.isDone() || !schedule.isFinished()) {
                break;
            }
            sourcePartitionedSchedulers.remove();
        }
        return new ScheduleResult(sourcePartitionedSchedulers.isEmpty(), newTasks, blocked);
    }

    @Override
    public void close()
    {
        sourcePartitionedSchedulers.isEmpty();
    }

    private static class FixedSplitPlacementPolicy
            implements SplitPlacementPolicy
    {
        private final NodeSelector nodeSelector;
        private final NodePartitionMap partitioning;
        private final Supplier<? extends List<RemoteTask>> remoteTasks;

        public FixedSplitPlacementPolicy(NodeSelector nodeSelector,
                NodePartitionMap partitioning,
                Supplier<? extends List<RemoteTask>> remoteTasks)
        {
            this.nodeSelector = nodeSelector;
            this.partitioning = partitioning;
            this.remoteTasks = remoteTasks;
        }

        @Override
        public Multimap<Node, Split> computeAssignments(Set<Split> splits)
        {
            return nodeSelector.computeAssignments(splits, remoteTasks.get(), partitioning);
        }

        @Override
        public void lockDownNodes()
        {
        }

        @Override
        public List<Node> allNodes()
        {
            return ImmutableList.copyOf(partitioning.getPartitionToNode().values());
        }
    }
}
