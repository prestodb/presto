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
import com.google.common.collect.MapMaker;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ThreadSafe
public class NodeTaskMap
{
    private final ConcurrentMap<Node, NodeTasks> tasksByNode = new ConcurrentHashMap<>();

    public synchronized void addTask(Node node, RemoteTask task)
    {
        NodeTasks tasks = tasksByNode.get(node);
        if (tasks == null) {
            tasks = new NodeTasks(node);
            tasksByNode.put(node, tasks);
        }
        tasks.addTask(task);
    }

    public int getPartitionedSplitsOnNode(Node node)
    {
        NodeTasks nodeTasks = tasksByNode.get(node);
        if (nodeTasks == null) {
            return 0;
        }
        return nodeTasks.getPartitionedSplitCount();
    }

    private static final class NodeTasks
    {
        private final Node node;
        private final ConcurrentMap<TaskId, RemoteTask> tasks;

        private NodeTasks(Node node)
        {
            this.node = node;
            this.tasks = new MapMaker().weakValues().makeMap();
        }

        public Node getNode()
        {
            return node;
        }

        public int getPartitionedSplitCount()
        {
            int partitionedSplitCount = 0;
            for (RemoteTask task : tasks.values()) {
                partitionedSplitCount += task.getPartitionedSplitCount();
            }
            return partitionedSplitCount;
        }

        @GuardedBy("this")
        public void addTask(final RemoteTask task)
        {
            tasks.put(task.getTaskInfo().getTaskId(), task);
        }
    }
}
