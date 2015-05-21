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

import com.facebook.presto.execution.NodeScheduler.NodeSelector;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.spi.Node;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FixedCountScheduler
        implements StageScheduler
{
    private final Function<Node, RemoteTask> taskScheduler;
    private final Function<Integer, List<Node>> randomNodeSupplier;
    private final int nodeCount;

    public FixedCountScheduler(SqlStageExecution stage, NodeSelector nodeSelector, int nodeCount)
    {
        this(requireNonNull(stage, "stage is null")::scheduleTask, requireNonNull(nodeSelector, "nodeSelector is null")::selectRandomNodes, nodeCount);
    }

    @VisibleForTesting
    FixedCountScheduler(Function<Node, RemoteTask> taskScheduler, Function<Integer, List<Node>> randomNodeSupplier, int nodeCount)
    {
        this.taskScheduler = requireNonNull(taskScheduler, "taskScheduler is null");
        this.randomNodeSupplier = requireNonNull(randomNodeSupplier, "randomNodeSupplier is null");

        checkArgument(nodeCount > 0, "nodeCount must be at least one");
        this.nodeCount = nodeCount;
    }

    @Override
    public ScheduleResult schedule()
    {
        List<Node> nodes = randomNodeSupplier.apply(nodeCount);
        checkCondition(!nodes.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");

        List<RemoteTask> newTasks = nodes.stream()
                .map(taskScheduler::apply)
                .collect(toImmutableList());

        return new ScheduleResult(true, newTasks, CompletableFuture.completedFuture(null));
    }
}
