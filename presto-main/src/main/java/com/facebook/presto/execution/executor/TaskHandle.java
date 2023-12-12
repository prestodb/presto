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
package com.facebook.presto.execution.executor;

import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.SplitConcurrencyController;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TaskHandle
{
    private volatile boolean destroyed;
    private final TaskId taskId;
    private final DoubleSupplier utilizationSupplier;
    private final TaskPriorityTracker priorityTracker;
    private final OptionalInt maxDriversPerTask;

    @GuardedBy("this")
    protected final Queue<PrioritizedSplitRunner> queuedLeafSplits = new ArrayDeque<>(10);
    @GuardedBy("this")
    protected final List<PrioritizedSplitRunner> runningLeafSplits = new ArrayList<>(10);
    @GuardedBy("this")
    protected final List<PrioritizedSplitRunner> runningIntermediateSplits = new ArrayList<>(10);
    @GuardedBy("this")
    protected final SplitConcurrencyController concurrencyController;

    private final AtomicInteger nextSplitId = new AtomicInteger();
    private final Optional<TaskShutDownListener> hostShutDownListener;
    private final Optional<OutputBuffer> outputBuffer;
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private AtomicBoolean anySplitProcessed = new AtomicBoolean(false);
    private boolean enableGracefulShutdown;
    private boolean enableRetryForFailedSplits;

    @VisibleForTesting
    public TaskHandle(
            TaskId taskId,
            TaskPriorityTracker priorityTracker,
            DoubleSupplier utilizationSupplier,
            int initialSplitConcurrency,
            Duration splitConcurrencyAdjustFrequency,
            OptionalInt maxDriversPerTask)
    {
        this(taskId, priorityTracker, utilizationSupplier, initialSplitConcurrency, splitConcurrencyAdjustFrequency, maxDriversPerTask, Optional.empty(), Optional.empty(), false, false);
    }

    public TaskHandle(
            TaskId taskId,
            TaskPriorityTracker priorityTracker,
            DoubleSupplier utilizationSupplier,
            int initialSplitConcurrency,
            Duration splitConcurrencyAdjustFrequency,
            OptionalInt maxDriversPerTask,
            Optional<TaskShutDownListener> hostShutDownListener,
            Optional<OutputBuffer> outputBuffer,
            boolean enableGracefulShutdown,
            boolean enableRetryForFailedSplits)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.utilizationSupplier = requireNonNull(utilizationSupplier, "utilizationSupplier is null");
        this.priorityTracker = requireNonNull(priorityTracker, "queryPriorityTracker is null");
        this.maxDriversPerTask = requireNonNull(maxDriversPerTask, "maxDriversPerTask is null");
        this.concurrencyController = new SplitConcurrencyController(
                initialSplitConcurrency,
                requireNonNull(splitConcurrencyAdjustFrequency, "splitConcurrencyAdjustFrequency is null"));
        this.hostShutDownListener = requireNonNull(hostShutDownListener, "hostShutDownListener is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        this.enableGracefulShutdown = enableGracefulShutdown;
        this.enableRetryForFailedSplits = enableRetryForFailedSplits;
    }

    public synchronized Priority addScheduledNanos(long durationNanos)
    {
        concurrencyController.update(durationNanos, utilizationSupplier.getAsDouble(), runningLeafSplits.size());
        return priorityTracker.updatePriority(durationNanos);
    }

    public synchronized Priority resetLevelPriority()
    {
        return priorityTracker.resetLevelPriority();
    }

    public boolean isDestroyed()
    {
        return destroyed;
    }

    public Priority getPriority()
    {
        return priorityTracker.getPriority();
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public OptionalInt getMaxDriversPerTask()
    {
        return maxDriversPerTask;
    }

    // Returns any remaining splits. The caller must destroy these.
    public synchronized List<PrioritizedSplitRunner> destroy()
    {
        destroyed = true;

        ImmutableList.Builder<PrioritizedSplitRunner> builder = ImmutableList.builderWithExpectedSize(runningIntermediateSplits.size() + runningLeafSplits.size() + queuedLeafSplits.size());
        builder.addAll(runningIntermediateSplits);
        builder.addAll(runningLeafSplits);
        builder.addAll(queuedLeafSplits);

        runningIntermediateSplits.clear();
        runningLeafSplits.clear();

        // We need to keep the queuedLeafSplits in case the TaskStatus is delayed and fetched after the Task is marked as done.
        if (!enableRetryForFailedSplits || !isShuttingDown.get()) {
            queuedLeafSplits.clear();
        }
        return builder.build();
    }

    public synchronized boolean enqueueSplit(PrioritizedSplitRunner split)
    {
        if (destroyed) {
            return false;
        }
        queuedLeafSplits.add(split);
        return true;
    }

    public synchronized List<ScheduledSplit> getUnprocessedSplits()
    {
        return queuedLeafSplits.stream().map(PrioritizedSplitRunner::getScheduledSplit).collect(Collectors.toList());
    }

    public synchronized boolean recordIntermediateSplit(PrioritizedSplitRunner split)
    {
        if (destroyed) {
            return false;
        }
        runningIntermediateSplits.add(split);
        return true;
    }

    synchronized int getRunningLeafSplits()
    {
        return runningLeafSplits.size();
    }

    synchronized boolean isTotalRunningSplitEmpty()
    {
        return runningLeafSplits.isEmpty() && runningIntermediateSplits.isEmpty();
    }

    public synchronized long getScheduledNanos()
    {
        return priorityTracker.getScheduledNanos();
    }

    public synchronized boolean isTaskIdling()
    {
        return !destroyed && runningLeafSplits.isEmpty() && runningIntermediateSplits.isEmpty() && queuedLeafSplits.isEmpty() && anySplitProcessed.get();
    }

    public synchronized PrioritizedSplitRunner pollNextSplit()
    {
        if (destroyed) {
            return null;
        }

        if (enableRetryForFailedSplits && isShuttingDown.get()) {
            boolean isAnyQueuedSplitStarted = isAnySplitStarted(queuedLeafSplits);
            checkState(!isAnyQueuedSplitStarted, String.format("queued split contains started splits for task %s", taskId));
            return null;
        }

        if (runningLeafSplits.size() >= concurrencyController.getTargetConcurrency()) {
            return null;
        }

        PrioritizedSplitRunner split = queuedLeafSplits.poll();
        if (split != null) {
            runningLeafSplits.add(split);
        }
        return split;
    }

    private boolean isAnySplitStarted(Queue<PrioritizedSplitRunner> queuedLeafSplits)
    {
        for (PrioritizedSplitRunner splitRunner : queuedLeafSplits) {
            if (splitRunner.isSplitAlreadyStarted()) {
                return true;
            }
        }
        return false;
    }

    public void gracefulShutdown()
    {
        checkState(enableGracefulShutdown || enableRetryForFailedSplits, "gracefulShutdown should only be called when either enableGracefulShutdown or enableRetryForFailedSplits is set to true");
        isShuttingDown.set(true);
    }

    public void handleShutDown()
    {
        if (!hostShutDownListener.isPresent()) {
            return;
        }
        hostShutDownListener.get().handleShutdown(taskId);
    }

    public void forceFailure(String errorMessage)
    {
        if (!hostShutDownListener.isPresent()) {
            return;
        }
        hostShutDownListener.get().forceFailure(taskId, errorMessage);
    }

    public boolean isTaskDone()
    {
        if (!hostShutDownListener.isPresent()) {
            return false;
        }
        return hostShutDownListener.get().isTaskDone();
    }

    public synchronized int getQueuedSplitSize()
    {
        return queuedLeafSplits.size();
    }

    public synchronized void splitComplete(PrioritizedSplitRunner split)
    {
        concurrencyController.splitFinished(split.getScheduledNanos(), utilizationSupplier.getAsDouble(), runningLeafSplits.size());
        runningIntermediateSplits.remove(split);
        runningLeafSplits.remove(split);
        anySplitProcessed.set(true);
    }

    public int getNextSplitId()
    {
        return nextSplitId.getAndIncrement();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .toString();
    }

    public boolean isOutputBufferEmpty()
    {
        return outputBuffer.isPresent() && outputBuffer.get().isAllPagesConsumed() && outputBuffer.get().getInfo().getState().isTerminal();
    }

    public Optional<OutputBuffer> getOutputBuffer()
    {
        return outputBuffer;
    }

    public boolean isEnableGracefulShutdownOrSplitRetry()
    {
        return enableGracefulShutdown || enableRetryForFailedSplits;
    }

    public boolean isShutdownInProgress()
    {
        checkState(enableGracefulShutdown || enableRetryForFailedSplits, "isShutdownInProgress should only be called when either enableGracefulShutdown or enableRetryForFailedSplits is set to true");
        return isShuttingDown.get();
    }
}
