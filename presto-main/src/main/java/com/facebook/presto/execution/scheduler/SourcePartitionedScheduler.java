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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class SourcePartitionedScheduler
        implements StageScheduler
{
    private final SqlStageExecution stage;
    private final SplitSource splitSource;
    private final SplitPlacementPolicy splitPlacementPolicy;
    private final int splitBatchSize;

    private CompletableFuture<List<Split>> batchFuture;
    private Set<Split> pendingSplits = ImmutableSet.of();

    private final Set<String> scheduledNodes = new HashSet<>();

    public SourcePartitionedScheduler(
            SqlStageExecution stage,
            SplitSource splitSource,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize)
    {
        this.stage = requireNonNull(stage, "stage is null");
        this.splitSource = requireNonNull(splitSource, "splitSource is null");
        this.splitPlacementPolicy = requireNonNull(splitPlacementPolicy, "splitPlacementPolicy is null");

        checkArgument(splitBatchSize > 0, "splitBatchSize must be at least one");
        this.splitBatchSize = splitBatchSize;
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

                batchFuture = splitSource.getNextBatch(splitBatchSize);
                long start = System.nanoTime();
                batchFuture.thenRun(() -> stage.recordGetSplitTime(start));
            }

            if (!batchFuture.isDone()) {
                return new ScheduleResult(false, ImmutableSet.of(), batchFuture);
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
            newTasks.addAll(stage.scheduleSplits(taskSplits.getKey(), taskSplits.getValue()));
            scheduledNodes.add(taskSplits.getKey().getNodeIdentifier());
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

        Set<RemoteTask> newTasks = splitPlacementPolicy.allNodes().stream()
                .filter(node -> !scheduledNodes.contains(node.getNodeIdentifier()))
                .map(stage::scheduleTask)
                .collect(toImmutableSet());

        // notify listeners that we have scheduled all tasks so they can set no more buffers or exchange splits
        stage.transitionToSchedulingSplits();

        return newTasks;
    }
}
