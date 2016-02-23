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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.Session;
import com.facebook.presto.execution.NodeTaskMap.PartitionedSplitCountTracker;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;

import static java.util.Objects.requireNonNull;

public class MemoryTrackingRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final RemoteTaskFactory remoteTaskFactory;
    private final QueryStateMachine stateMachine;

    public MemoryTrackingRemoteTaskFactory(RemoteTaskFactory remoteTaskFactory, QueryStateMachine stateMachine)
    {
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
    }

    @Override
    public RemoteTask createRemoteTask(Session session,
            TaskId taskId,
            Node node,
            int partition,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            boolean summarizeTaskInfo)
    {
        RemoteTask task = remoteTaskFactory.createRemoteTask(session,
                taskId,
                node,
                partition,
                fragment,
                initialSplits,
                outputBuffers,
                partitionedSplitCountTracker,
                summarizeTaskInfo);

        task.addStateChangeListener(new UpdatePeakMemory(stateMachine));
        return task;
    }

    private static final class UpdatePeakMemory
            implements StateChangeListener<TaskStatus>
    {
        private final QueryStateMachine stateMachine;
        private long previousMemory;

        public UpdatePeakMemory(QueryStateMachine stateMachine)
        {
            this.stateMachine = stateMachine;
        }

        @Override
        public synchronized void stateChanged(TaskStatus newStatus)
        {
            long currentMemory = newStatus.getMemoryReservation().toBytes();
            long deltaMemoryInBytes = currentMemory - previousMemory;
            previousMemory = currentMemory;
            stateMachine.updateMemoryUsage(deltaMemoryInBytes);
        }
    }
}
