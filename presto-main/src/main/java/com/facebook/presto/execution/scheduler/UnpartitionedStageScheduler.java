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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.split.SplitSource.SplitBatch;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.execution.scheduler.StageScheduleResult.BlockedReason.SPLIT_QUEUES_FULL;
import static com.facebook.presto.execution.scheduler.StageScheduleResult.BlockedReason.WAITING_FOR_SOURCE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class UnpartitionedStageScheduler
        implements StageScheduler
{
    private enum State
    {
        /**
         * No splits have been added to pendingSplits set.
         * TOOD: INITIALIZED and SPLITS_ADDED doesn't look useful... there are just 3 states:
         * - WORKING
         * - SPLITS_DISCOVERY_FINISHED (i.e. NO_MORE_SPLITS)
         * - FINISHED (i.e. SPLITS_DISCOVERY_FINISHED and placement finished)
         */
        INITIALIZED,

        /**
         * At least one split has been added to pendingSplits set.
         */
        SPLITS_ADDED,

        /**
         * All splits from underlying SplitSource have been discovered.
         * No more splits will be added to the pendingSplits set.
         * However, some splits might be waiting to be assigned to tasks yet.
         */
        NO_MORE_SPLITS,

        /**
         * All splits have been assigned to tasks.
         */
        FINISHED
    }

    private final SqlStageExecution stage;
    private final SplitSource splitSource;
    private final SplitPlacementPolicy splitPlacementPolicy;
    private final int splitBatchSize;
    private final PlanNodeId partitionedNode;

    private ListenableFuture<SplitBatch> nextSplitBatchFuture;
    private Set<Split> pendingSplits = ImmutableSet.of();
    private State state = State.INITIALIZED;

    public UnpartitionedStageScheduler(
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
    public synchronized StageScheduleResult schedule()
    {
        // Step 1: make sure there are some splits to be scheduled
        //         try to fetch the next batch of splits if pendingSplits is empty.
        if (pendingSplits.isEmpty() && state != State.NO_MORE_SPLITS && state != State.FINISHED) {
            if (nextSplitBatchFuture == null) {
                nextSplitBatchFuture = splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), splitBatchSize);

                long start = System.nanoTime();
                addSuccessCallback(nextSplitBatchFuture, () -> stage.recordGetSplitTime(start));
            }

            if (nextSplitBatchFuture.isDone()) {
                SplitBatch nextSplits = getFutureValue(nextSplitBatchFuture);
                verify(!nextSplits.getSplits().isEmpty());

                nextSplitBatchFuture = null;
                pendingSplits = new HashSet<>(nextSplits.getSplits());
                if (nextSplits.isLastBatch()) {
                    state = State.NO_MORE_SPLITS;
                }
            }
            else {
                // there are no splits for schedule, and we are blocked on fetching the next batch of splits
                return StageScheduleResult.blocked(
                        false,
                        ImmutableSet.of(),
                        nonCancellationPropagating(nextSplitBatchFuture), // cancellation is not propagated
                        WAITING_FOR_SOURCE,
                        0);
            }
        }

        // Step 2: calculate placements for splits, and assign splits to tasks
        // TODO: Note the SplitPlacementPolicy here is always DynamicSplitPlacementPolicy

        // TODO: should we check whether the previous future is finished before calling it again???

        checkState(!pendingSplits.isEmpty());
        if (state == State.INITIALIZED) {
            state = State.SPLITS_ADDED;
        }

        // calculate placements for splits
        SplitPlacementResult splitPlacementResult = splitPlacementPolicy.computeAssignments(pendingSplits);
        Multimap<InternalNode, Split> splitAssignment = splitPlacementResult.getAssignments();
        Set<RemoteTask> newTasks = assignSplits(splitAssignment);

        // remove assigned splits
        pendingSplits = ImmutableSet.copyOf(Sets.difference(pendingSplits, ImmutableSet.copyOf(splitAssignment.values())));

        // there are splits not assigned, this means some workers' split queues are full
        if (!pendingSplits.isEmpty()) {
            newTasks = ImmutableSet.<RemoteTask>builder()
                    .addAll(newTasks)
                    // In a broadcast join, output buffers of the tasks in build source stage have to
                    // hold onto all data produced before probe side task scheduling finishes,
                    // even if the data is acknowledged by all known consumers. This is because
                    // new consumers may be added until the probe side task scheduling finishes.
                    //
                    // As a result, the following line is necessary to prevent deadlock
                    // due to neither build nor probe can make any progress.
                    // The build side blocks due to a full output buffer.
                    // In the meantime the probe side split cannot be consumed since
                    // builder side hash table construction has not finished.
                    .addAll(finalizeTaskCreationIfNecessary())
                    .build();
            return StageScheduleResult.blocked(false, newTasks, splitPlacementResult.getBlocked(), SPLIT_QUEUES_FULL, splitAssignment.values().size());
        }

        // all discovered splits assigned
        if (state == State.NO_MORE_SPLITS) {
            state = State.FINISHED;
            splitSource.close();
        }
        return StageScheduleResult.nonBlocked(state == State.FINISHED, newTasks, splitAssignment.values().size());
    }

    @Override
    public void close()
    {
        splitSource.close();
    }

    private Set<RemoteTask> assignSplits(Multimap<InternalNode, Split> splitAssignment)
    {
        ImmutableSet.Builder<RemoteTask> newTasks = ImmutableSet.builder();
        for (Entry<InternalNode, Collection<Split>> taskSplits : splitAssignment.asMap().entrySet()) {
            newTasks.addAll(stage.scheduleSplits(
                    taskSplits.getKey(),
                    ImmutableMultimap.<PlanNodeId, Split>builder()
                            .putAll(partitionedNode, taskSplits.getValue())
                            .build(),
                    // unpartitioned stage will not send noMoreSplit signal at lifespan granularity,
                    // it will send noMoreSplit signal for the whole stage once scheduling finished (in SqlStageExecution#schedulingComplete)
                    ImmutableMultimap.of()));
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

        Set<InternalNode> scheduledNodes = stage.getScheduledNodes();
        Set<RemoteTask> newTasks = splitPlacementPolicy.allNodes().stream()
                .filter(node -> !scheduledNodes.contains(node))
                .flatMap(node -> stage.scheduleSplits(node, ImmutableMultimap.of(), ImmutableMultimap.of()).stream())
                .collect(toImmutableSet());

        // notify listeners that we have scheduled all tasks so they can set no more buffers or exchange splits
        stage.transitionToSchedulingSplits();

        return newTasks;
    }
}
