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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class NodeTaskMap
{
    private final ConcurrentHashMap<Node, NodeTasks> nodeTasksMap = new ConcurrentHashMap<>();

    public void addTask(Node node, RemoteTask task)
    {
        NodeTasks nodeTasks = nodeTasksMap.get(node);
        if (nodeTasks == null) {
            nodeTasks = addNodeTask(node);
        }
        nodeTasks.addTask(task);
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

    public int getPartitionedSplitsOnNode(Node node)
    {
        NodeTasks nodeTasks = nodeTasksMap.get(node);
        if (nodeTasks == null) {
            nodeTasks = addNodeTask(node);
        }
        return nodeTasks.getPartitionedSplitCount();
    }

    private static class NodeTasks
    {
        @GuardedBy("this")
        private final List<RemoteTask> remoteTasks = new ArrayList<>();

        private synchronized int getPartitionedSplitCount()
        {
            int partitionedSplitCount = 0;
            for (RemoteTask task : remoteTasks) {
                partitionedSplitCount += task.getPartitionedSplitCount();
            }
            return partitionedSplitCount;
        }

        private synchronized void addTask(RemoteTask task)
        {
            remoteTasks.add(task);
            task.addStateChangeListener(taskInfo -> {
                if (taskInfo.getState().isDone()) {
                    synchronized (NodeTasks.this) {
                        remoteTasks.remove(task);
                    }
                }
            });

            // Check if task state changes before adding the listener
            if (task.getTaskInfo().getState().isDone()) {
                remoteTasks.remove(task);
            }
        }
    }
}
