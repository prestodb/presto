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
import com.google.common.collect.Sets;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class NodeTaskMap
{
    private static final Logger log = Logger.get(NodeTaskMap.class);
    private final ConcurrentHashMap<Node, NodeTasks> nodeTasksMap = new ConcurrentHashMap<>();

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
        NodeTasks newNodeTasks = new NodeTasks();
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

        private int getPartitionedSplitCount()
        {
            return nodeTotalPartitionedSplitCount.get();
        }

        private void addTask(RemoteTask task)
        {
            if (remoteTasks.add(task)) {
                task.addStateChangeListener(taskInfo -> {
                    if (taskInfo.getState().isDone()) {
                        remoteTasks.remove(task);
                    }
                });

                // Check if task state is already done before adding the listener
                if (task.getTaskInfo().getState().isDone()) {
                    remoteTasks.remove(task);
                }
            }
        }

        public PartitionedSplitCountTracker createPartitionedSplitCountTracker(TaskId taskId)
        {
            return new TaskPartitionedSplitCountTracker(taskId);
        }

        @ThreadSafe
        private class TaskPartitionedSplitCountTracker
                implements PartitionedSplitCountTracker
        {
            private final TaskId taskId;
            private int localPartitionedSplitCount;

            public TaskPartitionedSplitCountTracker(TaskId taskId)
            {
                this.taskId = requireNonNull(taskId, "taskId is null");
            }

            @Override
            public synchronized void setPartitionedSplitCount(int partitionedSplitCount)
            {
                if (partitionedSplitCount < 0) {
                    nodeTotalPartitionedSplitCount.addAndGet(-localPartitionedSplitCount);
                    localPartitionedSplitCount = 0;
                    throw new IllegalArgumentException("partitionedSplitCount is negative");
                }

                int delta = partitionedSplitCount - localPartitionedSplitCount;
                nodeTotalPartitionedSplitCount.addAndGet(delta);
                localPartitionedSplitCount = partitionedSplitCount;
            }

            @Override
            protected synchronized void finalize()
            {
                if (localPartitionedSplitCount == 0) {
                    return;
                }

                log.error("BUG! %s for %s leaked with %s splits.  Cleaning up so server can continue to function.",
                        getClass().getName(),
                        taskId,
                        localPartitionedSplitCount);

                nodeTotalPartitionedSplitCount.addAndGet(-localPartitionedSplitCount);
                localPartitionedSplitCount = 0;
            }
        }
    }

    public interface PartitionedSplitCountTracker
    {
        void setPartitionedSplitCount(int partitionedSplitCount);
    }
}
