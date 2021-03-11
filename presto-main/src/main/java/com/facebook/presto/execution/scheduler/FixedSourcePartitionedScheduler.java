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
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason;
import com.facebook.presto.execution.scheduler.group.DynamicLifespanScheduler;
import com.facebook.presto.execution.scheduler.group.FixedLifespanScheduler;
import com.facebook.presto.execution.scheduler.group.LifespanScheduler;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.SplitSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.MoreFutures.whenAnyComplete;
import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsSourceScheduler;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class FixedSourcePartitionedScheduler
        implements StageScheduler
{
    private static final Logger log = Logger.get(FixedSourcePartitionedScheduler.class);

    private final SqlStageExecution stage;
    private final List<InternalNode> nodes;

    private final List<SourceScheduler> sourceSchedulers;
    private final List<ConnectorPartitionHandle> partitionHandles;
    private boolean scheduledTasks;
    private boolean anySourceSchedulingFinished;
    private final Optional<LifespanScheduler> groupedLifespanScheduler;

    private final Queue<Integer> tasksToRecover = new ConcurrentLinkedQueue<>();

    @GuardedBy("this")
    private boolean closed;

    public FixedSourcePartitionedScheduler(
            SqlStageExecution stage,
            Map<PlanNodeId, SplitSource> splitSources,
            StageExecutionDescriptor stageExecutionDescriptor,
            List<PlanNodeId> schedulingOrder,
            List<InternalNode> nodes,
            BucketNodeMap bucketNodeMap,
            int splitBatchSize,
            OptionalInt concurrentLifespansPerTask,
            NodeSelector nodeSelector,
            List<ConnectorPartitionHandle> partitionHandles)
    {
        requireNonNull(stage, "stage is null");
        requireNonNull(splitSources, "splitSources is null");
        requireNonNull(bucketNodeMap, "bucketNodeMap is null");
        checkArgument(!requireNonNull(nodes, "nodes is null").isEmpty(), "nodes is empty");
        requireNonNull(partitionHandles, "partitionHandles is null");

        this.stage = stage;
        this.nodes = ImmutableList.copyOf(nodes);
        this.partitionHandles = ImmutableList.copyOf(partitionHandles);

        checkArgument(splitSources.keySet().equals(ImmutableSet.copyOf(schedulingOrder)));

        BucketedSplitPlacementPolicy splitPlacementPolicy = new BucketedSplitPlacementPolicy(nodeSelector, nodes, bucketNodeMap, stage::getAllTasks);

        ArrayList<SourceScheduler> sourceSchedulers = new ArrayList<>();
        checkArgument(
                partitionHandles.equals(ImmutableList.of(NOT_PARTITIONED)) != stageExecutionDescriptor.isStageGroupedExecution(),
                "PartitionHandles should be [NOT_PARTITIONED] if and only if all scan nodes use ungrouped execution strategy");
        int nodeCount = nodes.size();
        int concurrentLifespans;
        if (concurrentLifespansPerTask.isPresent() && concurrentLifespansPerTask.getAsInt() * nodeCount <= partitionHandles.size()) {
            concurrentLifespans = concurrentLifespansPerTask.getAsInt() * nodeCount;
        }
        else {
            concurrentLifespans = partitionHandles.size();
        }

        boolean firstPlanNode = true;
        Optional<LifespanScheduler> groupedLifespanScheduler = Optional.empty();
        for (PlanNodeId planNodeId : schedulingOrder) {
            SplitSource splitSource = splitSources.get(planNodeId);
            boolean groupedExecutionForScanNode = stageExecutionDescriptor.isScanGroupedExecution(planNodeId);
            SourceScheduler sourceScheduler = newSourcePartitionedSchedulerAsSourceScheduler(
                    stage,
                    planNodeId,
                    splitSource,
                    splitPlacementPolicy,
                    Math.max(splitBatchSize / concurrentLifespans, 1),
                    groupedExecutionForScanNode);

            if (stageExecutionDescriptor.isStageGroupedExecution() && !groupedExecutionForScanNode) {
                sourceScheduler = new AsGroupedSourceScheduler(sourceScheduler);
            }
            sourceSchedulers.add(sourceScheduler);

            if (firstPlanNode) {
                firstPlanNode = false;
                if (!stageExecutionDescriptor.isStageGroupedExecution()) {
                    sourceScheduler.startLifespan(Lifespan.taskWide(), NOT_PARTITIONED);
                }
                else {
                    LifespanScheduler lifespanScheduler;
                    if (bucketNodeMap.isDynamic()) {
                        // Callee of the constructor guarantees dynamic bucket node map will only be
                        // used when the stage has no remote source.
                        //
                        // When the stage has no remote source, any scan is grouped execution guarantees
                        // all scan is grouped execution.
                        lifespanScheduler = new DynamicLifespanScheduler(bucketNodeMap, nodes, partitionHandles, concurrentLifespansPerTask);
                    }
                    else {
                        lifespanScheduler = new FixedLifespanScheduler(bucketNodeMap, partitionHandles, concurrentLifespansPerTask);
                    }

                    // Schedule the first few lifespans
                    lifespanScheduler.scheduleInitial(sourceScheduler);
                    // Schedule new lifespans for finished ones
                    stage.addCompletedDriverGroupsChangedListener(lifespanScheduler::onLifespanExecutionFinished);
                    groupedLifespanScheduler = Optional.of(lifespanScheduler);
                }
            }
        }
        this.groupedLifespanScheduler = groupedLifespanScheduler;

        // use a CopyOnWriteArrayList to prevent ConcurrentModificationExceptions
        // if close() is called while the main thread is in the scheduling loop
        this.sourceSchedulers = new CopyOnWriteArrayList<>(sourceSchedulers);
    }

    private ConnectorPartitionHandle partitionHandleFor(Lifespan lifespan)
    {
        if (lifespan.isTaskWide()) {
            return NOT_PARTITIONED;
        }
        return partitionHandles.get(lifespan.getId());
    }

    @Override
    public ScheduleResult schedule()
    {
        // schedule a task on every node in the distribution
        List<RemoteTask> newTasks = ImmutableList.of();
        if (!scheduledTasks) {
            newTasks = Streams.mapWithIndex(
                    nodes.stream(),
                    (node, id) -> stage.scheduleTask(node, toIntExact(id)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());
            scheduledTasks = true;

            // notify listeners that we have scheduled all tasks so they can set no more buffers or exchange splits
            stage.transitionToFinishedTaskScheduling();
        }

        boolean allBlocked = true;
        List<ListenableFuture<?>> blocked = new ArrayList<>();
        BlockedReason blockedReason = BlockedReason.NO_ACTIVE_DRIVER_GROUP;

        if (groupedLifespanScheduler.isPresent()) {
            while (!tasksToRecover.isEmpty()) {
                if (anySourceSchedulingFinished) {
                    throw new IllegalStateException("Recover after any source scheduling finished is not supported");
                }
                groupedLifespanScheduler.get().onTaskFailed(tasksToRecover.poll(), sourceSchedulers);
            }

            if (groupedLifespanScheduler.get().allLifespanExecutionFinished()) {
                for (SourceScheduler sourceScheduler : sourceSchedulers) {
                    sourceScheduler.notifyAllLifespansFinishedExecution();
                }
            }
            else {
                // Start new driver groups on the first scheduler if necessary,
                // i.e. when previous ones have finished execution (not finished scheduling).
                //
                // Invoke schedule method to get a new SettableFuture every time.
                // Reusing previously returned SettableFuture could lead to the ListenableFuture retaining too many listeners.
                blocked.add(groupedLifespanScheduler.get().schedule(sourceSchedulers.get(0)));
            }
        }

        int splitsScheduled = 0;
        Iterator<SourceScheduler> schedulerIterator = sourceSchedulers.iterator();
        List<Lifespan> driverGroupsToStart = ImmutableList.of();
        while (schedulerIterator.hasNext()) {
            synchronized (this) {
                // if a source scheduler is closed while it is scheduling, we can get an error
                // prevent that by checking if scheduling has been cancelled first.
                if (closed) {
                    break;
                }
                SourceScheduler sourceScheduler = schedulerIterator.next();

                for (Lifespan lifespan : driverGroupsToStart) {
                    sourceScheduler.startLifespan(lifespan, partitionHandleFor(lifespan));
                }

                ScheduleResult schedule = sourceScheduler.schedule();
                if (schedule.getSplitsScheduled() > 0) {
                    stage.transitionToSchedulingSplits();
                }
                splitsScheduled += schedule.getSplitsScheduled();
                if (schedule.getBlockedReason().isPresent()) {
                    blocked.add(schedule.getBlocked());
                    blockedReason = blockedReason.combineWith(schedule.getBlockedReason().get());
                }
                else {
                    verify(schedule.getBlocked().isDone(), "blockedReason not provided when scheduler is blocked");
                    allBlocked = false;
                }

                driverGroupsToStart = sourceScheduler.drainCompletelyScheduledLifespans();

                if (schedule.isFinished()) {
                    stage.schedulingComplete(sourceScheduler.getPlanNodeId());
                    sourceSchedulers.remove(sourceScheduler);
                    sourceScheduler.close();
                    anySourceSchedulingFinished = true;
                }
            }
        }

        if (allBlocked) {
            return ScheduleResult.blocked(sourceSchedulers.isEmpty(), newTasks, whenAnyComplete(blocked), blockedReason, splitsScheduled);
        }
        else {
            return ScheduleResult.nonBlocked(sourceSchedulers.isEmpty(), newTasks, splitsScheduled);
        }
    }

    public void recover(TaskId taskId)
    {
        tasksToRecover.add(taskId.getId());
    }

    @Override
    public synchronized void close()
    {
        closed = true;
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

    public static class BucketedSplitPlacementPolicy
            implements SplitPlacementPolicy
    {
        private final NodeSelector nodeSelector;
        private final List<InternalNode> activeNodes;
        private final BucketNodeMap bucketNodeMap;
        private final Supplier<? extends List<RemoteTask>> remoteTasks;

        public BucketedSplitPlacementPolicy(
                NodeSelector nodeSelector,
                List<InternalNode> activeNodes,
                BucketNodeMap bucketNodeMap,
                Supplier<? extends List<RemoteTask>> remoteTasks)
        {
            this.nodeSelector = requireNonNull(nodeSelector, "nodeSelector is null");
            this.activeNodes = ImmutableList.copyOf(requireNonNull(activeNodes, "activeNodes is null"));
            this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
            this.remoteTasks = requireNonNull(remoteTasks, "remoteTasks is null");
        }

        @Override
        public SplitPlacementResult computeAssignments(Set<Split> splits)
        {
            return nodeSelector.computeAssignments(splits, remoteTasks.get(), bucketNodeMap);
        }

        @Override
        public void lockDownNodes()
        {
        }

        @Override
        public List<InternalNode> getActiveNodes()
        {
            return activeNodes;
        }

        public InternalNode getNodeForBucket(int bucketId)
        {
            return bucketNodeMap.getAssignedNode(bucketId).get();
        }
    }

    private static class AsGroupedSourceScheduler
            implements SourceScheduler
    {
        private final SourceScheduler sourceScheduler;
        private boolean started;
        private boolean scheduleCompleted;
        private final List<Lifespan> pendingCompleted;

        public AsGroupedSourceScheduler(SourceScheduler sourceScheduler)
        {
            this.sourceScheduler = requireNonNull(sourceScheduler, "sourceScheduler is null");
            pendingCompleted = new ArrayList<>();
        }

        @Override
        public ScheduleResult schedule()
        {
            return sourceScheduler.schedule();
        }

        @Override
        public void close()
        {
            sourceScheduler.close();
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return sourceScheduler.getPlanNodeId();
        }

        @Override
        public void startLifespan(Lifespan lifespan, ConnectorPartitionHandle partitionHandle)
        {
            pendingCompleted.add(lifespan);
            if (started) {
                return;
            }
            started = true;
            sourceScheduler.startLifespan(Lifespan.taskWide(), NOT_PARTITIONED);
        }

        @Override
        public void rewindLifespan(Lifespan lifespan, ConnectorPartitionHandle partitionHandle)
        {
            throw new UnsupportedOperationException("rewindLifespan is not supported in AsGroupedSourceScheduler");
        }

        @Override
        public List<Lifespan> drainCompletelyScheduledLifespans()
        {
            if (!scheduleCompleted) {
                List<Lifespan> lifespans = sourceScheduler.drainCompletelyScheduledLifespans();
                if (lifespans.isEmpty()) {
                    return ImmutableList.of();
                }
                checkState(ImmutableList.of(Lifespan.taskWide()).equals(lifespans));
                scheduleCompleted = true;
            }
            List<Lifespan> result = ImmutableList.copyOf(pendingCompleted);
            pendingCompleted.clear();
            return result;
        }

        @Override
        public void notifyAllLifespansFinishedExecution()
        {
            checkState(scheduleCompleted);
            // no-op
        }
    }
}
