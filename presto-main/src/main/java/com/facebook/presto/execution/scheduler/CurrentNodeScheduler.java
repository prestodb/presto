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
import com.google.common.collect.ImmutableList;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class CurrentNodeScheduler
        implements StageScheduler
{
    private final Function<Node, RemoteTask> taskScheduler;
    private final Supplier<Node> currentNodeSupplier;

    public CurrentNodeScheduler(SqlStageExecution stage, NodeSelector nodeSelector)
    {
        this(requireNonNull(stage, "stage is null")::scheduleTask, requireNonNull(nodeSelector, "nodeSelector is null")::selectCurrentNode);
    }

    public CurrentNodeScheduler(Function<Node, RemoteTask> taskScheduler, Supplier<Node> currentNodeSupplier)
    {
        this.taskScheduler = requireNonNull(taskScheduler, "taskScheduler is null");
        this.currentNodeSupplier = requireNonNull(currentNodeSupplier, "currentNodeSupplier is null");
    }

    @Override
    public ScheduleResult schedule()
    {
        RemoteTask task = taskScheduler.apply(currentNodeSupplier.get());
        return new ScheduleResult(true, ImmutableList.of(task), CompletableFuture.completedFuture(null));
    }
}
