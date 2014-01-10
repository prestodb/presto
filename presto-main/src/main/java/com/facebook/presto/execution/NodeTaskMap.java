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

import com.facebook.presto.metadata.Node;
import com.google.common.collect.MapMaker;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.execution.StateMachine.StateChangeListener;

@ThreadSafe
public class NodeTaskMap
{
    private final ConcurrentMap<Node, NodeTasks> nodes = new ConcurrentHashMap<>();

    public void addTask(Node node, RemoteTask task)
    {
        NodeTasks nodeTasks = nodes.get(node);
        if (nodeTasks == null) {
            nodeTasks = new NodeTasks(node);
            NodeTasks existingNodeTasks = nodes.putIfAbsent(node, nodeTasks);
            if (existingNodeTasks != null) {
                nodeTasks = existingNodeTasks;
            }
        }
        nodeTasks.addTask(task);
    }

    public int getSplitsOnNode(Node node)
    {
        NodeTasks nodeTasks = nodes.get(node);
        if (nodeTasks == null) {
            return 0;
        }
        return nodeTasks.getSplitCount();
    }

    private static final class NodeTasks
    {
        private final Node node;
        private final ConcurrentMap<TaskId, RemoteTask> tasks;
        private final AtomicInteger splitCount = new AtomicInteger();

        private NodeTasks(Node node)
        {
            this.node = node;
            this.tasks = new MapMaker().weakValues().makeMap();
        }

        public Node getNode()
        {
            return node;
        }

        public int getSplitCount()
        {
            return splitCount.get();
        }

        public synchronized void  addTask(final RemoteTask task)
        {
            TaskInfo initialTaskInfo = task.getTaskInfo();
            if (tasks.putIfAbsent(initialTaskInfo.getTaskId(), task) != null) {
                // already tracking this task
                return;
            }

            StateChangeListener<Integer> stateChangeListener = new StateChangeListener<Integer>()
            {
                private int currentSplitCount;

                @Override
                public void stateChanged(Integer newSplitCount)
                {
                    splitCount.addAndGet(newSplitCount - currentSplitCount);
                    currentSplitCount = newSplitCount;
                }
            };

            // set the initial state
            stateChangeListener.stateChanged(task.getSplitCount());
            task.addSplitCountStateChangeListener(stateChangeListener);
        }
    }
}
