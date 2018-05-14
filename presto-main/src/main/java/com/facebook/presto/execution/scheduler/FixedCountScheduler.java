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
import com.facebook.presto.spi.Node;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class FixedCountScheduler
        implements StageScheduler
{
    public interface TaskScheduler
    {
        RemoteTask scheduleTask(Node node, int partition, OptionalInt totalPartitions);
    }

    private final TaskScheduler taskScheduler;
    private final Map<Integer, Node> partitionToNode;

    public FixedCountScheduler(SqlStageExecution stage, Map<Integer, Node> partitionToNode)
    {
        requireNonNull(stage, "stage is null");
        this.taskScheduler = stage::scheduleTask;
        this.partitionToNode = requireNonNull(partitionToNode, "partitionToNode is null");
    }

    @VisibleForTesting
    public FixedCountScheduler(TaskScheduler taskScheduler, Map<Integer, Node> partitionToNode)
    {
        this.taskScheduler = requireNonNull(taskScheduler, "taskScheduler is null");
        this.partitionToNode = requireNonNull(partitionToNode, "partitionToNode is null");
    }

    @Override
    public ScheduleResult schedule()
    {
        OptionalInt totalPartitions = OptionalInt.of(partitionToNode.size());
        List<RemoteTask> newTasks = partitionToNode.entrySet().stream()
                .map(entry -> taskScheduler.scheduleTask(entry.getValue(), entry.getKey(), totalPartitions))
                .collect(toImmutableList());

        return new ScheduleResult(true, newTasks, 0);
    }
}
