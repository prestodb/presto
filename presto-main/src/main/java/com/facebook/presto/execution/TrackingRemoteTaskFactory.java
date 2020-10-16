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

import com.facebook.presto.Session;
import com.facebook.presto.execution.NodeTaskMap.PartitionedSplitCountTracker;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.Multimap;

import static com.facebook.presto.execution.TaskState.PLANNED;
import static com.facebook.presto.execution.TaskState.RUNNING;
import static java.util.Objects.requireNonNull;

public class TrackingRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final RemoteTaskFactory remoteTaskFactory;
    private final QueryStateMachine stateMachine;

    public TrackingRemoteTaskFactory(RemoteTaskFactory remoteTaskFactory, QueryStateMachine stateMachine)
    {
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
    }

    @Override
    public RemoteTask createRemoteTask(Session session,
            TaskId taskId,
            InternalNode node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            boolean summarizeTaskInfo,
            TableWriteInfo tableWriteInfo)
    {
        RemoteTask task = remoteTaskFactory.createRemoteTask(session,
                taskId,
                node,
                fragment,
                initialSplits,
                outputBuffers,
                partitionedSplitCountTracker,
                summarizeTaskInfo,
                tableWriteInfo);

        task.addStateChangeListener(new UpdateQueryStats(stateMachine));
        return task;
    }

    private static final class UpdateQueryStats
            implements StateChangeListener<TaskStatus>
    {
        private final QueryStateMachine stateMachine;
        private long previousUserMemory;
        private long previousSystemMemory;
        private TaskState state;

        public UpdateQueryStats(QueryStateMachine stateMachine)
        {
            this.stateMachine = stateMachine;
            this.state = PLANNED;
        }

        @Override
        public synchronized void stateChanged(TaskStatus newStatus)
        {
            long currentUserMemory = newStatus.getMemoryReservationInBytes();
            long currentSystemMemory = newStatus.getSystemMemoryReservationInBytes();
            long currentTotalMemory = currentUserMemory + currentSystemMemory;
            long deltaUserMemoryInBytes = currentUserMemory - previousUserMemory;
            long deltaTotalMemoryInBytes = currentTotalMemory - (previousUserMemory + previousSystemMemory);
            previousUserMemory = currentUserMemory;
            previousSystemMemory = currentSystemMemory;
            stateMachine.updateMemoryUsage(deltaUserMemoryInBytes, deltaTotalMemoryInBytes, currentUserMemory, currentTotalMemory, newStatus.getPeakNodeTotalMemoryReservationInBytes());

            if (state == PLANNED && newStatus.getState() == RUNNING) {
                stateMachine.incrementCurrentRunningTaskCount();
            }
            else if (state == RUNNING && newStatus.getState().isDone()) {
                stateMachine.decrementCurrentRunningTaskCount();
            }
            state = newStatus.getState();
        }
    }
}
