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
package com.facebook.presto.execution;

import com.facebook.presto.spi.Node;
import com.facebook.presto.util.FinalizerService;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class NodeTaskMap
{
    private static final Logger log = Logger.get(NodeTaskMap.class);
    private final ConcurrentHashMap<Node, NodeTasks> nodeTasksMap = new ConcurrentHashMap<>();
    private final FinalizerService finalizerService;

    @Inject
    public NodeTaskMap(FinalizerService finalizerService)
    {
        this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");
    }

    public void addTask(Node node, RemoteTask task)
    {
        createOrGetNodeTasks(node).addTask(task);
    }

    public int getPartitionedSplitsOnNode(Node node)
    {
        return createOrGetNodeTasks(node).getPartitionedSplitCount();
    }

    public PartitionedSplitCountTracker createPartitionedSplitCountTracker(Node node, TaskId taskId)
    {
        return createOrGetNodeTasks(node).createPartitionedSplitCountTracker(taskId);
    }

    private NodeTasks createOrGetNodeTasks(Node node)
    {
        NodeTasks nodeTasks = nodeTasksMap.get(node);
        if (nodeTasks == null) {
            nodeTasks = addNodeTask(node);
        }
        return nodeTasks;
    }

    private NodeTasks addNodeTask(Node node)
    {
        NodeTasks newNodeTasks = new NodeTasks(finalizerService);
        NodeTasks nodeTasks = nodeTasksMap.putIfAbsent(node, newNodeTasks);
        if (nodeTasks == null) {
            return newNodeTasks;
        }
        return nodeTasks;
    }

    private static class NodeTasks
    {
        private final Set<RemoteTask> remoteTasks = Sets.newConcurrentHashSet();
        private final AtomicInteger nodeTotalPartitionedSplitCount = new AtomicInteger();
        private final FinalizerService finalizerService;

        public NodeTasks(FinalizerService finalizerService)
        {
            this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");
        }

        private int getPartitionedSplitCount()
        {
            return nodeTotalPartitionedSplitCount.get();
        }

        private void addTask(RemoteTask task)
        {
            if (remoteTasks.add(task)) {
                task.addStateChangeListener(taskStatus -> {
                    if (taskStatus.getState().isDone()) {
                        remoteTasks.remove(task);
                    }
                });

                // Check if task state is already done before adding the listener
                if (task.getTaskStatus().getState().isDone()) {
                    remoteTasks.remove(task);
                }
            }
        }

        public PartitionedSplitCountTracker createPartitionedSplitCountTracker(TaskId taskId)
        {
            requireNonNull(taskId, "taskId is null");

            TaskPartitionedSplitCountTracker tracker = new TaskPartitionedSplitCountTracker(taskId);
            PartitionedSplitCountTracker partitionedSplitCountTracker = new PartitionedSplitCountTracker(tracker::setPartitionedSplitCount);

            // when partitionedSplitCountTracker is garbage collected, run the cleanup method on the tracker
            // Note: tracker can not have a reference to partitionedSplitCountTracker
            finalizerService.addFinalizer(partitionedSplitCountTracker, tracker::cleanup);

            return partitionedSplitCountTracker;
        }

        @ThreadSafe
        private class TaskPartitionedSplitCountTracker
        {
            private final TaskId taskId;
            private final AtomicInteger localPartitionedSplitCount = new AtomicInteger();

            public TaskPartitionedSplitCountTracker(TaskId taskId)
            {
                this.taskId = requireNonNull(taskId, "taskId is null");
            }

            public synchronized void setPartitionedSplitCount(int partitionedSplitCount)
            {
                if (partitionedSplitCount < 0) {
                    int oldValue = localPartitionedSplitCount.getAndSet(0);
                    nodeTotalPartitionedSplitCount.addAndGet(-oldValue);
                    throw new IllegalArgumentException("partitionedSplitCount is negative");
                }

                int oldValue = localPartitionedSplitCount.getAndSet(partitionedSplitCount);
                nodeTotalPartitionedSplitCount.addAndGet(partitionedSplitCount - oldValue);
            }

            public void cleanup()
            {
                int leakedSplits = localPartitionedSplitCount.getAndSet(0);
                if (leakedSplits == 0) {
                    return;
                }

                log.error("BUG! %s for %s leaked with %s partitioned splits.  Cleaning up so server can continue to function.",
                        getClass().getName(),
                        taskId,
                        leakedSplits);

                nodeTotalPartitionedSplitCount.addAndGet(-leakedSplits);
            }

            @Override
            public String toString()
            {
                return MoreObjects.toStringHelper(this)
                        .add("taskId", taskId)
                        .add("splits", localPartitionedSplitCount)
                        .toString();
            }
        }
    }

    public static class PartitionedSplitCountTracker
    {
        private final IntConsumer splitSetter;

        public PartitionedSplitCountTracker(IntConsumer splitSetter)
        {
            this.splitSetter = requireNonNull(splitSetter, "splitSetter is null");
        }

        public void setPartitionedSplitCount(int partitionedSplitCount)
        {
            splitSetter.accept(partitionedSplitCount);
        }

        @Override
        public String toString()
        {
            return splitSetter.toString();
        }
    }
}
