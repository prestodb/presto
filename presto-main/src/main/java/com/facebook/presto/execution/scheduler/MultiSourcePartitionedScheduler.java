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
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.SplitSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsSourceScheduler;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class MultiSourcePartitionedScheduler
        implements StageScheduler
{
    private static final Logger log = Logger.get(MultiSourcePartitionedScheduler.class);

    private final SqlStageExecution stageExecution;
    private final Queue<SourceScheduler> sourceSchedulers;

    public MultiSourcePartitionedScheduler(
            SqlStageExecution stageExecution,
            Map<PlanNodeId, SplitSource> splitSources,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize,
            StageExecutionDescriptor stageExecutionDescriptor)
    {
        requireNonNull(splitSources, "splitSources is null");
        checkArgument(splitSources.size() > 1, "It is expected that there will be more than one split sources");

        ImmutableList.Builder<SourceScheduler> sourceSchedulers = ImmutableList.builder();
        for (PlanNodeId planNodeId : splitSources.keySet()) {
            SplitSource splitSource = splitSources.get(planNodeId);
            boolean groupedExecutionForScanNode = stageExecutionDescriptor.isScanGroupedExecution(planNodeId);

            SourceScheduler sourceScheduler = newSourcePartitionedSchedulerAsSourceScheduler(
                    stageExecution,
                    planNodeId,
                    splitSource,
                    splitPlacementPolicy,
                    splitBatchSize,
                    groupedExecutionForScanNode);
            sourceScheduler.startLifespan(Lifespan.taskWide(), NOT_PARTITIONED);
            sourceSchedulers.add(sourceScheduler);
        }
        this.stageExecution = requireNonNull(stageExecution, "stageExecution is null");
        this.sourceSchedulers = new ArrayDeque<>(sourceSchedulers.build());
    }

    @Override
    public synchronized ScheduleResult schedule()
    {
        ImmutableSet.Builder<RemoteTask> newScheduledTasks = ImmutableSet.builder();
        ListenableFuture<?> blocked = immediateVoidFuture();
        Optional<ScheduleResult.BlockedReason> blockedReason = Optional.empty();
        int splitsScheduled = 0;

        while (!sourceSchedulers.isEmpty()) {
            SourceScheduler scheduler = sourceSchedulers.peek();
            ScheduleResult scheduleResult = scheduler.schedule();
            scheduler.drainCompletelyScheduledLifespans();

            splitsScheduled += scheduleResult.getSplitsScheduled();
            newScheduledTasks.addAll(scheduleResult.getNewTasks());
            blocked = scheduleResult.getBlocked();
            blockedReason = scheduleResult.getBlockedReason();

            // if the source is not done scheduling, stop scheduling for now
            if (!blocked.isDone() || !scheduleResult.isFinished()) {
                break;
            }

            stageExecution.schedulingComplete(scheduler.getPlanNodeId());
            sourceSchedulers.remove().close();
        }
        if (blockedReason.isPresent()) {
            return ScheduleResult.blocked(sourceSchedulers.isEmpty(), newScheduledTasks.build(), blocked, blockedReason.get(), splitsScheduled);
        }
        return ScheduleResult.nonBlocked(sourceSchedulers.isEmpty(), newScheduledTasks.build(), splitsScheduled);
    }

    @Override
    public void close()
    {
        for (SourceScheduler sourceScheduler : sourceSchedulers) {
            try {
                sourceScheduler.close();
            }
            catch (Throwable t) {
                log.warn(t, "Error closing split source");
            }
        }
        sourceSchedulers.clear();
    }
}
