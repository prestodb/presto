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
import com.facebook.presto.execution.SafeEventLoopGroup;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.metadata.InternalNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason.WRITER_SCALING;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ScaledWriterScheduler
        implements StageScheduler
{
    private final SqlStageExecution stage;
    private final Supplier<Collection<TaskStatus>> sourceTasksProvider;
    private final Supplier<Collection<TaskStatus>> writerTasksProvider;
    private final NodeSelector nodeSelector;
    private final SafeEventLoopGroup.SafeEventLoop stageEventLoop;

    private final boolean optimizedScaleWriterProducerBuffer;
    private final long writerMinSizeBytes;
    private final Optional<Integer> initialTaskCount;

    private final Set<InternalNode> scheduledNodes = new HashSet<>();

    private final AtomicBoolean done = new AtomicBoolean();
    private volatile SettableFuture<?> future = SettableFuture.create();

    public ScaledWriterScheduler(
            SqlStageExecution stage,
            Supplier<Collection<TaskStatus>> sourceTasksProvider,
            Supplier<Collection<TaskStatus>> writerTasksProvider,
            NodeSelector nodeSelector,
            DataSize writerMinSize,
            boolean optimizedScaleWriterProducerBuffer,
            Optional<Integer> initialTaskCount)
    {
        this.stage = requireNonNull(stage, "stage is null");
        this.sourceTasksProvider = requireNonNull(sourceTasksProvider, "sourceTasksProvider is null");
        this.writerTasksProvider = requireNonNull(writerTasksProvider, "writerTasksProvider is null");
        this.nodeSelector = requireNonNull(nodeSelector, "nodeSelector is null");
        this.stageEventLoop = requireNonNull(stage.getStageEventLoop(), "stageEventLoop is null");
        this.writerMinSizeBytes = requireNonNull(writerMinSize, "minWriterSize is null").toBytes();
        this.optimizedScaleWriterProducerBuffer = optimizedScaleWriterProducerBuffer;
        this.initialTaskCount = requireNonNull(initialTaskCount, "initialTaskCount is null");
    }

    public void finish()
    {
        done.set(true);
        future.set(null);
    }

    @Override
    public ScheduleResult schedule()
    {
        Set<ListenableFuture<RemoteTask>> writers = scheduleTasks(getNewTaskCount());

        future.set(null);
        future = SettableFuture.create();
        stageEventLoop.schedule(() -> future.set(null), 200, MILLISECONDS);

        return ScheduleResult.blocked(done.get(), writers, future, WRITER_SCALING, 0);
    }

    private int getNewTaskCount()
    {
        if (scheduledNodes.isEmpty()) {
            return initialTaskCount.orElse(1);
        }

        double fullTasks = sourceTasksProvider.get().stream()
                .filter(task -> !task.getState().isDone())
                .map(TaskStatus::isOutputBufferOverutilized)
                .mapToDouble(full -> full ? 1.0 : 0.0)
                .average().orElse(0.0);

        long writtenBytes = writerTasksProvider.get().stream()
                .mapToLong(TaskStatus::getPhysicalWrittenDataSizeInBytes)
                .sum();

        if ((fullTasks >= 0.5) && (writtenBytes >= (writerMinSizeBytes * scheduledNodes.size()))) {
            return 1;
        }

        if (optimizedScaleWriterProducerBuffer) {
            double totalProducerBufferUtilization = sourceTasksProvider.get().stream()
                    .filter(task -> !task.getState().isDone())
                    .mapToDouble(TaskStatus::getOutputBufferUtilization)
                    .sum();

            if (totalProducerBufferUtilization >= scheduledNodes.size() &&
                    writtenBytes >= writerMinSizeBytes * scheduledNodes.size()) {
                return 1;
            }
        }

        return 0;
    }

    private Set<ListenableFuture<RemoteTask>> scheduleTasks(int count)
    {
        if (count == 0) {
            return ImmutableSet.of();
        }

        List<InternalNode> nodes = nodeSelector.selectRandomNodes(count, scheduledNodes);

        checkCondition(!scheduledNodes.isEmpty() || !nodes.isEmpty(), NO_NODES_AVAILABLE, "No nodes available to run query");

        ImmutableSet.Builder<ListenableFuture<RemoteTask>> newFutureTasks = ImmutableSet.builder();
        for (InternalNode node : nodes) {
            ListenableFuture<RemoteTask> remoteTask = stage.scheduleTask(node, scheduledNodes.size());
            if (remoteTask.isDone() && getUnchecked(remoteTask) == null) {
                continue;
            }
            newFutureTasks.add(remoteTask);
            scheduledNodes.add(node);
        }

        return newFutureTasks.build();
    }
}
