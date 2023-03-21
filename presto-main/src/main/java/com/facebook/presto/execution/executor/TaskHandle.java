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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.SplitConcurrencyController;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffer;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TaskHandle
{
    private volatile boolean destroyed;
    private static final Logger log = Logger.get(TaskHandle.class);
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
    private final Optional<HostShutDownListener> hostShutDownListener;
    private final Optional<OutputBuffer> outputBuffer;
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    public TaskHandle(
            TaskId taskId,
            TaskPriorityTracker priorityTracker,
            DoubleSupplier utilizationSupplier,
            int initialSplitConcurrency,
            Duration splitConcurrencyAdjustFrequency,
            OptionalInt maxDriversPerTask)
    {
        this(taskId, priorityTracker, utilizationSupplier, initialSplitConcurrency, splitConcurrencyAdjustFrequency, maxDriversPerTask, Optional.empty(), Optional.empty());
    }

    public TaskHandle(
            TaskId taskId,
            TaskPriorityTracker priorityTracker,
            DoubleSupplier utilizationSupplier,
            int initialSplitConcurrency,
            Duration splitConcurrencyAdjustFrequency,
            OptionalInt maxDriversPerTask,
            Optional<HostShutDownListener> hostShutDownListener,
            Optional<OutputBuffer> outputBuffer)
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
        queuedLeafSplits.clear();
        return builder.build();
    }

    public synchronized boolean enqueueSplit(PrioritizedSplitRunner split)
    {
        if (destroyed || isShuttingDown.get()) {
            return false;
        }
        queuedLeafSplits.add(split);
        return true;
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

    public synchronized long getScheduledNanos()
    {
        return priorityTracker.getScheduledNanos();
    }

    public synchronized PrioritizedSplitRunner pollNextSplit()
    {
        if (destroyed || isShuttingDown.get()) {
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

    public void gracefulShutdown()
    {
        isShuttingDown.set(true);
    }

    public void handleShutDown(Set<PrioritizedSplitRunner> runningSplits)
    {
        if (!hostShutDownListener.isPresent()) {
            return;
        }
        //wait for running split to be over
        long waitTimeMillis = 100; // Wait for 100 milliseconds between checks
        while (runningSplits.size() > 0) {
            try {
                log.info("queued leaf split = %s, running leaf splits = %s,  waiting for running split to be over to kill the task - %s", queuedLeafSplits.size(), runningSplits.size(), taskId);
                Thread.sleep(waitTimeMillis);
            }
            catch (InterruptedException e) {
                // Handle interruption
            }
        }
        log.warn("GracefulShutdown:: Waiting for running split is over, going to kill task - %s", taskId);
        hostShutDownListener.get().handleShutdown(taskId);
        log.warn("GracefulShutdown:: Waiting for running split is over, going to kill task - %s", taskId);
        //TODO wait for ack from coordinator
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
        return outputBuffer.get().isAllPagesConsumed();
    }
}
