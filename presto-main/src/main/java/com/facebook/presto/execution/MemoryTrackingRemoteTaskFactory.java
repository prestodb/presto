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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.OptionalInt;
import java.util.function.Consumer;

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

    @Override
    public void destroyExchangeSources(List<ExchangeBufferLocation> locationsToDestroy, Consumer<PrestoException> onFailure)
    {
        remoteTaskFactory.destroyExchangeSources(locationsToDestroy, onFailure);
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
