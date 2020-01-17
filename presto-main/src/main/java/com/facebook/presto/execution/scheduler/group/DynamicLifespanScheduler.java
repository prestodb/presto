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
import com.google.common.util.concurrent.SettableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

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
    private final IntArrayFIFOQueue driverGroupQueue;
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
        this.driverGroupQueue = new IntArrayFIFOQueue(bucketCount);
        for (int i = 0; i < bucketCount; i++) {
            taskByDriverGroup[i] = NOT_ASSIGNED;
            driverGroupQueue.enqueue(i);
        }
        this.failedTasks = new IntOpenHashSet();
    }

    @Override
    public void scheduleInitial(SourceScheduler scheduler)
    {
        checkState(!initialScheduled, "Initial scheduling happened before scheduleInitial is called");

        int driverGroupsScheduledPerTask = 0;
        synchronized (this) {
            while (!driverGroupQueue.isEmpty()) {
                for (int i = 0; i < nodeByTaskId.size() && !driverGroupQueue.isEmpty(); i++) {
                    int driverGroupId = driverGroupQueue.dequeueInt();
                    checkState(!bucketNodeMap.getAssignedNode(driverGroupId).isPresent());
                    bucketNodeMap.assignOrUpdateBucketToNode(driverGroupId, nodeByTaskId.get(i));
                    scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
                    taskByDriverGroup[driverGroupId] = i;
                    runningDriverGroupIdsByTask[i].add(driverGroupId);
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
                driverGroupQueue.enqueue(driverGroupId);
            }
            runningDriverGroupIdsByTask[taskId].clear();
        }
    }

    @Override
    public SettableFuture schedule(SourceScheduler scheduler)
    {
        // Return a new future even if newDriverGroupReady has not finished.
        // Returning the same SettableFuture instance could lead to ListenableFuture retaining too many listener objects.

        checkState(initialScheduled, "schedule should only be called after initial scheduling finished");
        checkState(failedTasks.size() < nodeByTaskId.size(), "All tasks have failed");

        synchronized (this) {
            newDriverGroupReady = SettableFuture.create();
            while (!availableTasks.isEmpty() && !driverGroupQueue.isEmpty()) {
                int taskId = availableTasks.dequeueInt();
                if (failedTasks.contains(taskId)) {
                    continue;
                }
                int nextDriverGroupId = driverGroupQueue.dequeueInt();
                InternalNode nodeForCompletedDriverGroup = nodeByTaskId.get(taskId);
                bucketNodeMap.assignOrUpdateBucketToNode(nextDriverGroupId, nodeForCompletedDriverGroup);
                scheduler.startLifespan(Lifespan.driverGroup(nextDriverGroupId), partitionHandles.get(nextDriverGroupId));
                taskByDriverGroup[nextDriverGroupId] = taskId;
                runningDriverGroupIdsByTask[taskId].add(nextDriverGroupId);
            }
        }
        return newDriverGroupReady;
    }

    @Override
    public synchronized boolean allLifespanExecutionFinished()
    {
        return totalLifespanExecutionFinished == partitionHandles.size();
    }
}
