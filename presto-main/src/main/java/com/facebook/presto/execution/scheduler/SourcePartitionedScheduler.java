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
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static java.util.Objects.requireNonNull;

public class SourcePartitionedScheduler
        implements StageScheduler
{
    private final SqlStageExecution stage;
    private final SplitSource splitSource;
    private final SplitPlacementPolicy splitPlacementPolicy;
    private final int splitBatchSize;
    private final PlanNodeId partitionedNode;

    private CompletableFuture<List<Split>> batchFuture;
    private Set<Split> pendingSplits = ImmutableSet.of();

    public SourcePartitionedScheduler(
            SqlStageExecution stage,
            PlanNodeId partitionedNode,
            SplitSource splitSource,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize)
    {
        this.stage = requireNonNull(stage, "stage is null");
        this.splitSource = requireNonNull(splitSource, "splitSource is null");
        this.splitPlacementPolicy = requireNonNull(splitPlacementPolicy, "splitPlacementPolicy is null");

        checkArgument(splitBatchSize > 0, "splitBatchSize must be at least one");
        this.splitBatchSize = splitBatchSize;

        this.partitionedNode = partitionedNode;
    }

    @Override
    public synchronized ScheduleResult schedule()
    {
        // Acquire a future for the next state change before doing calculations.
        //
        // This code may need to return a future when the workers are full, and
        // it is critical that this future is notified of any changes that occur
        // during this calculation (to avoid starvation).
        CompletableFuture<?> taskStateChange = stage.getTaskStateChange();

        // try to get the next batch if necessary
        if (pendingSplits.isEmpty()) {
            if (batchFuture == null) {
                if (splitSource.isFinished()) {
                    // no more splits
                    splitSource.close();
                    return new ScheduleResult(true, ImmutableSet.of(), CompletableFuture.completedFuture(null));
                }

                long start = System.nanoTime();
                batchFuture = splitSource.getNextBatch(splitBatchSize);
                batchFuture.thenRun(() -> stage.recordGetSplitTime(start));
            }

            if (!batchFuture.isDone()) {
                // wrap batch future in unmodifiable future so cancellation is not propagated
                CompletableFuture<List<Split>> blocked = unmodifiableFuture(batchFuture);
                return new ScheduleResult(false, ImmutableSet.of(), blocked);
            }
            pendingSplits = ImmutableSet.copyOf(getFutureValue(batchFuture));
            batchFuture = null;
        }

        // assign the splits
        Multimap<Node, Split> splitAssignment = splitPlacementPolicy.computeAssignments(pendingSplits);
        Set<RemoteTask> newTasks = assignSplits(splitAssignment);

        // remove assigned splits
        pendingSplits = ImmutableSet.copyOf(Sets.difference(pendingSplits, ImmutableSet.copyOf(splitAssignment.values())));

        // if not all splits were consumed, return a partial result
        if (!pendingSplits.isEmpty()) {
            newTasks = ImmutableSet.<RemoteTask>builder()
                    .addAll(newTasks)
                    .addAll(finalizeTaskCreationIfNecessary())
                    .build();

            return new ScheduleResult(false, newTasks, taskStateChange);
        }

        // all splits assigned - check if the source is finished
        boolean finished = splitSource.isFinished();
        if (finished) {
            splitSource.close();
        }
        return new ScheduleResult(finished, newTasks, CompletableFuture.completedFuture(null));
    }

    @Override
    public void close()
    {
        splitSource.close();
    }

    private Set<RemoteTask> assignSplits(Multimap<Node, Split> splitAssignment)
    {
        ImmutableSet.Builder<RemoteTask> newTasks = ImmutableSet.builder();
        for (Entry<Node, Collection<Split>> taskSplits : splitAssignment.asMap().entrySet()) {
            // source partitioned tasks can only receive broadcast data; otherwise it would have a different distribution
            newTasks.addAll(stage.scheduleSplits(taskSplits.getKey(), ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(partitionedNode, taskSplits.getValue())
                    .build()));
        }
        return newTasks.build();
    }

    private Set<RemoteTask> finalizeTaskCreationIfNecessary()
    {
        // only lock down tasks if there is a sub stage that could block waiting for this stage to create all tasks
        if (stage.getFragment().isLeaf()) {
            return ImmutableSet.of();
        }

        splitPlacementPolicy.lockDownNodes();

        Set<Node> scheduledNodes = stage.getScheduledNodes();
        Set<RemoteTask> newTasks = splitPlacementPolicy.allNodes().stream()
                .filter(node -> !scheduledNodes.contains(node))
                .flatMap(node -> stage.scheduleSplits(node, ImmutableMultimap.of()).stream())
                .collect(toImmutableSet());

        // notify listeners that we have scheduled all tasks so they can set no more buffers or exchange splits
        stage.transitionToSchedulingSplits();

        return newTasks;
    }
}
