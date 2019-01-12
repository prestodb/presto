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
package io.prestosql.execution;

import com.google.common.collect.Multimap;
import io.prestosql.OutputBuffers;
import io.prestosql.Session;
import io.prestosql.execution.NodeTaskMap.PartitionedSplitCountTracker;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.metadata.Split;
import io.prestosql.spi.Node;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.OptionalInt;

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
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OptionalInt totalPartitions,
            OutputBuffers outputBuffers,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            boolean summarizeTaskInfo)
    {
        RemoteTask task = remoteTaskFactory.createRemoteTask(session,
                taskId,
                node,
                fragment,
                initialSplits,
                totalPartitions,
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
        private long previousUserMemory;
        private long previousSystemMemory;

        public UpdatePeakMemory(QueryStateMachine stateMachine)
        {
            this.stateMachine = stateMachine;
        }

        @Override
        public synchronized void stateChanged(TaskStatus newStatus)
        {
            long currentUserMemory = newStatus.getMemoryReservation().toBytes();
            long currentSystemMemory = newStatus.getSystemMemoryReservation().toBytes();
            long currentTotalMemory = currentUserMemory + currentSystemMemory;
            long deltaUserMemoryInBytes = currentUserMemory - previousUserMemory;
            long deltaTotalMemoryInBytes = currentTotalMemory - (previousUserMemory + previousSystemMemory);
            previousUserMemory = currentUserMemory;
            previousSystemMemory = currentSystemMemory;
            stateMachine.updateMemoryUsage(deltaUserMemoryInBytes, deltaTotalMemoryInBytes, currentTotalMemory);
        }
    }
}
