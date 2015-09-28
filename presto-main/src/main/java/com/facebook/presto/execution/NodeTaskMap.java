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

import javax.annotation.concurrent.ThreadSafe;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public class NodeTaskMap
{
    private final ConcurrentHashMap<Node, NodeTasks> nodeTasksMap = new ConcurrentHashMap<>();

    public void addTask(Node node, RemoteTask task)
    {
        createOrGetNodeTasks(node).addTask(task);
    }

    public int getPartitionedSplitsOnNode(Node node)
    {
        return createOrGetNodeTasks(node).getPartitionedSplitCount();
    }

    public SplitCountChangeListener getSplitCountChangeListener(Node node)
    {
        return createOrGetNodeTasks(node).getSplitCountChangeListener();
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
        private final AtomicInteger partitionedSplitCount = new AtomicInteger();
        private final SplitCountChangeListener splitCountChangeListener = partitionedSplitCount::addAndGet;

        private int getPartitionedSplitCount()
        {
            return partitionedSplitCount.get();
        }

        private void addTask(RemoteTask task)
        {
            if (remoteTasks.add(task)) {
                task.addStateChangeListener(taskInfo -> {
                    if (task.getTaskInfo().getState().isDone()) {
                        remoteTasks.remove(task);
                    }
                });

                // Check if task state is already done before adding the listener
                if (task.getTaskInfo().getState().isDone()) {
                    remoteTasks.remove(task);
                }
            }
        }

        public SplitCountChangeListener getSplitCountChangeListener()
        {
            return splitCountChangeListener;
        }
    }

    public interface SplitCountChangeListener
    {
        void splitCountChanged(int delta);
    }
}
