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
package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.SourceScheduler;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * See {@link LifespanScheduler} about thread safety
 */
public class DynamicLifespanScheduler
        implements LifespanScheduler
{
    private static final int NOT_ASSIGNED = -1;

    private final BucketNodeMap bucketNodeMap;
    private final List<InternalNode> nodeByTaskId;
    private final List<ConnectorPartitionHandle> partitionHandles;
    private final OptionalInt concurrentLifespansPerTask;

    private final IntSet[] runningDriverGroupIdsByTask;
    private final int[] taskByDriverGroup;
    private final IntArrayFIFOQueue noPreferenceDriverGroups;
    private final Map<InternalNode, IntArrayFIFOQueue> nodeToPreferredDriverGroups;
    private final IntSet failedTasks;

    // initialScheduled does not need to be guarded because this object
    // is safely published after its mutation.
    private boolean initialScheduled;
    // Write to newDriverGroupReady field is guarded. Read of the reference
    // is either guarded, or is guaranteed to happen in the same thread as the write.
    private SettableFuture<?> newDriverGroupReady = SettableFuture.create();

    // availableTasks is used to track tasks that finish lifespan to schedule the next batch of buckets.
    @GuardedBy("this")
    private final IntArrayFIFOQueue availableTasks = new IntArrayFIFOQueue();
    @GuardedBy("this")
    private int totalLifespanExecutionFinished;

    public DynamicLifespanScheduler(
            BucketNodeMap bucketNodeMap,
            List<InternalNode> nodeByTaskId,
            List<ConnectorPartitionHandle> partitionHandles,
            OptionalInt concurrentLifespansPerTask)
    {
        this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
        this.nodeByTaskId = requireNonNull(nodeByTaskId, "nodeByTaskId is null");
        this.partitionHandles = unmodifiableList(new ArrayList<>(
                requireNonNull(partitionHandles, "partitionHandles is null")));

        this.concurrentLifespansPerTask = requireNonNull(concurrentLifespansPerTask, "concurrentLifespansPerTask is null");
        concurrentLifespansPerTask.ifPresent(lifespansPerTask -> checkArgument(lifespansPerTask >= 1, "concurrentLifespansPerTask must be great or equal to 1 if present"));

        int bucketCount = partitionHandles.size();
        verify(bucketCount > 0);
        this.runningDriverGroupIdsByTask = new IntSet[nodeByTaskId.size()];
        for (int i = 0; i < nodeByTaskId.size(); i++) {
            runningDriverGroupIdsByTask[i] = new IntOpenHashSet();
        }
        this.taskByDriverGroup = new int[bucketCount];
        this.noPreferenceDriverGroups = new IntArrayFIFOQueue();
        this.nodeToPreferredDriverGroups = new HashMap<>();
        Set<InternalNode> nodeSet = ImmutableSet.copyOf(nodeByTaskId);
        for (int i = 0; i < bucketCount; i++) {
            taskByDriverGroup[i] = NOT_ASSIGNED;
            if (bucketNodeMap.getAssignedNode(i).isPresent() && nodeSet.contains(bucketNodeMap.getAssignedNode(i).get())) {
                InternalNode preferredNode = bucketNodeMap.getAssignedNode(i).get();
                nodeToPreferredDriverGroups.computeIfAbsent(preferredNode, k -> new IntArrayFIFOQueue());
                nodeToPreferredDriverGroups.get(preferredNode).enqueue(i);
            }
            else {
                noPreferenceDriverGroups.enqueue(i);
            }
        }
        this.failedTasks = new IntOpenHashSet();
    }

    @Override
    public void scheduleInitial(SourceScheduler scheduler)
    {
        checkState(!initialScheduled, "Initial scheduling happened before scheduleInitial is called");

        int driverGroupsScheduledPerTask = 0;
        synchronized (this) {
            while (!noPreferenceDriverGroups.isEmpty() || !nodeToPreferredDriverGroups.isEmpty()) {
                for (int taskId = 0; taskId < nodeByTaskId.size() && (!noPreferenceDriverGroups.isEmpty() || !nodeToPreferredDriverGroups.isEmpty()); taskId++) {
                    InternalNode node = nodeByTaskId.get(taskId);
                    OptionalInt driverGroupId = getNextDriverGroup(node);
                    if (!driverGroupId.isPresent()) {
                        continue;
                    }
                    scheduler.startLifespan(Lifespan.driverGroup(driverGroupId.getAsInt()), partitionHandles.get(driverGroupId.getAsInt()));
                    taskByDriverGroup[driverGroupId.getAsInt()] = taskId;
                    runningDriverGroupIdsByTask[taskId].add(driverGroupId.getAsInt());
                }

                driverGroupsScheduledPerTask++;
                if (concurrentLifespansPerTask.isPresent() && driverGroupsScheduledPerTask == concurrentLifespansPerTask.getAsInt()) {
                    break;
                }
            }
            initialScheduled = true;
        }
    }

    @Override
    public void onLifespanExecutionFinished(Iterable<Lifespan> newlyCompletelyExecutedDriverGroups)
    {
        checkState(initialScheduled, "onLifespanExecutionFinished should only be called after initial scheduling finished");

        SettableFuture<?> newDriverGroupReady;
        synchronized (this) {
            for (Lifespan newlyCompletelyExecutedDriverGroup : newlyCompletelyExecutedDriverGroups) {
                checkArgument(!newlyCompletelyExecutedDriverGroup.isTaskWide());
                int driverGroupId = newlyCompletelyExecutedDriverGroup.getId();
                availableTasks.enqueue(taskByDriverGroup[driverGroupId]);
                totalLifespanExecutionFinished++;
                runningDriverGroupIdsByTask[taskByDriverGroup[driverGroupId]].remove(driverGroupId);
            }
            newDriverGroupReady = this.newDriverGroupReady;
        }
        newDriverGroupReady.set(null);
    }

    @Override
    public void onTaskFailed(int taskId, List<SourceScheduler> sourceSchedulers)
    {
        checkState(initialScheduled, "onTaskFailed should only be called after initial scheduling finished");

        synchronized (this) {
            this.failedTasks.add(taskId);
            for (int driverGroupId : runningDriverGroupIdsByTask[taskId]) {
                for (SourceScheduler sourceScheduler : sourceSchedulers) {
                    sourceScheduler.rewindLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
                }
                noPreferenceDriverGroups.enqueue(driverGroupId);
            }

            // When a task fails, all driverGroups that prefer this task/node would be not able to execute
            // Thus they should be relocated to driverGroupNoPreferenceQueue
            if (nodeToPreferredDriverGroups.containsKey(nodeByTaskId.get(taskId))) {
                IntArrayFIFOQueue preferredDriverGroupQueue = nodeToPreferredDriverGroups.get(nodeByTaskId.get(taskId));
                while (!preferredDriverGroupQueue.isEmpty()) {
                    noPreferenceDriverGroups.enqueue(preferredDriverGroupQueue.dequeueInt());
                }
                nodeToPreferredDriverGroups.remove(nodeByTaskId.get(taskId));
            }

            runningDriverGroupIdsByTask[taskId].clear();
        }
    }

    @Override
    public SettableFuture schedule(SourceScheduler scheduler)
    {
        // Return a new future even if newDriverGroupReady has not finished.
        // Returning the same SettableFuture instance could lead to ListenableFuture retaining too many listener objects.

        // TODO: After initial scheduling, tasks would only be available after they finished at least one bucket.
        //  This is not necessarily the case if the initial scheduling covered all buckets and
        //  the available slots is not fully utilized (concurrentLifespansPerTask is large or infinite).
        //  In this case if a task failed, the recovered driver groups have to wait for tasks to be available again after finishing at least one bucket,
        //  even though by definition of concurrentLifespansPerTask they are already available.

        checkState(initialScheduled, "schedule should only be called after initial scheduling finished");
        checkState(failedTasks.size() < nodeByTaskId.size(), "All tasks have failed");

        synchronized (this) {
            newDriverGroupReady = SettableFuture.create();
            while (!availableTasks.isEmpty() && (!noPreferenceDriverGroups.isEmpty() || !nodeToPreferredDriverGroups.isEmpty())) {
                int taskId = availableTasks.dequeueInt();
                if (failedTasks.contains(taskId)) {
                    continue;
                }

                OptionalInt nextDriverGroupId = getNextDriverGroup(nodeByTaskId.get(taskId));
                if (!nextDriverGroupId.isPresent()) {
                    continue;
                }
                scheduler.startLifespan(Lifespan.driverGroup(nextDriverGroupId.getAsInt()), partitionHandles.get(nextDriverGroupId.getAsInt()));
                taskByDriverGroup[nextDriverGroupId.getAsInt()] = taskId;
                runningDriverGroupIdsByTask[taskId].add(nextDriverGroupId.getAsInt());
            }
        }
        return newDriverGroupReady;
    }

    @Override
    public synchronized boolean allLifespanExecutionFinished()
    {
        return totalLifespanExecutionFinished == partitionHandles.size();
    }

    private OptionalInt getNextDriverGroup(InternalNode node)
    {
        OptionalInt driverGroupId = OptionalInt.empty();
        if ((nodeToPreferredDriverGroups.get(node) != null) && !nodeToPreferredDriverGroups.get(node).isEmpty()) {
            driverGroupId = OptionalInt.of(nodeToPreferredDriverGroups.get(node).dequeueInt());
            if (nodeToPreferredDriverGroups.get(node).isEmpty()) {
                nodeToPreferredDriverGroups.remove(node);
            }
        }
        else if (!noPreferenceDriverGroups.isEmpty()) {
            driverGroupId = OptionalInt.of(noPreferenceDriverGroups.dequeueInt());
            bucketNodeMap.assignOrUpdateBucketToNode(driverGroupId.getAsInt(), node, false);
        }
        return driverGroupId;
    }
}
