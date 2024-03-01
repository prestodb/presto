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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class StageLinkage
{
    private final PlanFragmentId currentStageFragmentId;
    private final ExchangeLocationsConsumer parent;
    private final Set<OutputBufferManager> childOutputBufferManagers;

    public StageLinkage(PlanFragmentId fragmentId, ExchangeLocationsConsumer parent, Set<SqlStageExecution> children)
    {
        this.currentStageFragmentId = fragmentId;
        this.parent = parent;
        this.childOutputBufferManagers = children.stream()
                .map(childStage -> {
                    PartitioningHandle partitioningHandle = childStage.getFragment().getPartitioningScheme().getPartitioning().getHandle();
                    if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
                        return new BroadcastOutputBufferManager(childStage::setOutputBuffers);
                    }
                    else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                        return new ScaledOutputBufferManager(childStage::setOutputBuffers);
                    }
                    else {
                        int partitionCount = Ints.max(childStage.getFragment().getPartitioningScheme().getBucketToPartition().get()) + 1;
                        return new PartitionedOutputBufferManager(partitioningHandle, partitionCount, childStage::setOutputBuffers);
                    }
                })
                .collect(toImmutableSet());
    }

    public void processScheduleResults(StageExecutionState newState, Set<RemoteTask> newTasks)
    {
        boolean noMoreTasks = false;
        switch (newState) {
            case PLANNED:
            case SCHEDULING:
                // workers are still being added to the query
                break;
            case FINISHED_TASK_SCHEDULING:
            case SCHEDULING_SPLITS:
            case SCHEDULED:
            case RUNNING:
            case FINISHED:
            case CANCELED:
                // no more workers will be added to the query
                noMoreTasks = true;
            case ABORTED:
            case FAILED:
                // DO NOT complete a FAILED or ABORTED stage.  This will cause the
                // stage above to finish normally, which will result in a query
                // completing successfully when it should fail..
                break;
        }

        // Add an exchange location to the parent stage for each new task
        parent.addExchangeLocations(currentStageFragmentId, newTasks, noMoreTasks);

        if (!childOutputBufferManagers.isEmpty()) {
            // Add an output buffer to the child stages for each new task
            List<OutputBuffers.OutputBufferId> newOutputBuffers = newTasks.stream()
                    .map(task -> new OutputBuffers.OutputBufferId(task.getTaskId().getId()))
                    .collect(toImmutableList());
            for (OutputBufferManager child : childOutputBufferManagers) {
                child.addOutputBuffers(newOutputBuffers, noMoreTasks);
            }
        }
    }
}
