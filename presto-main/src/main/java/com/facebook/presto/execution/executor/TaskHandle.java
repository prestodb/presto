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

import com.facebook.presto.execution.SplitConcurrencyController;
import com.facebook.presto.execution.TaskId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;

import static com.facebook.presto.execution.executor.MultilevelSplitQueue.LEVELS;
import static com.facebook.presto.execution.executor.MultilevelSplitQueue.computeLevel;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class TaskHandle
{
    private final TaskId taskId;
    private final DoubleSupplier utilizationSupplier;

    @GuardedBy("this")
    private final Queue<PrioritizedSplitRunner> queuedLeafSplits = new ArrayDeque<>(10);
    @GuardedBy("this")
    private final List<PrioritizedSplitRunner> runningLeafSplits = new ArrayList<>(10);
    @GuardedBy("this")
    private final List<PrioritizedSplitRunner> runningIntermediateSplits = new ArrayList<>(10);
    @GuardedBy("this")
    private long scheduledNanos;
    @GuardedBy("this")
    private boolean destroyed;
    @GuardedBy("this")
    private final SplitConcurrencyController concurrencyController;

    private final AtomicInteger nextSplitId = new AtomicInteger();

    private final AtomicReference<Priority> priority = new AtomicReference<>(new Priority());
    private final MultilevelSplitQueue splitQueue;

    public TaskHandle(TaskId taskId, MultilevelSplitQueue splitQueue, DoubleSupplier utilizationSupplier, int initialSplitConcurrency, Duration splitConcurrencyAdjustFrequency)
    {
        this.taskId = taskId;
        this.splitQueue = splitQueue;
        this.utilizationSupplier = utilizationSupplier;
        this.concurrencyController = new SplitConcurrencyController(initialSplitConcurrency, splitConcurrencyAdjustFrequency);
    }

    public synchronized Priority addScheduledNanos(long durationNanos)
    {
        concurrencyController.update(durationNanos, utilizationSupplier.getAsDouble(), runningLeafSplits.size());
        scheduledNanos += durationNanos;

        Priority oldPriority = priority.get();
        Priority newPriority;

        if (oldPriority.getLevel() < (LEVELS.length - 1) && scheduledNanos >= LEVELS[oldPriority.getLevel() + 1]) {
            int newLevel = computeLevel(scheduledNanos);
            long newLevelMinPriority = splitQueue.getLevelMinPriority(newLevel, scheduledNanos);
            newPriority = new Priority(newLevel, newLevelMinPriority + (scheduledNanos - LEVELS[oldPriority.getLevel()]));
        }
        else {
            newPriority = new Priority(oldPriority.getLevel(), oldPriority.getLevelPriority() + durationNanos);
        }

        priority.set(newPriority);
        return newPriority;
    }

    public synchronized Priority resetLevelPriority()
    {
        long levelMinPriority = splitQueue.getLevelMinPriority(priority.get().getLevel(), scheduledNanos);
        if (priority.get().getLevelPriority() < levelMinPriority) {
            Priority newPriority = new Priority(priority.get().getLevel(), levelMinPriority);
            priority.set(newPriority);
            return newPriority;
        }

        return priority.get();
    }

    public synchronized boolean isDestroyed()
    {
        return destroyed;
    }

    public int getLevel()
    {
        return priority.get().getLevel();
    }

    public long getLevelPriority()
    {
        return priority.get().getLevelPriority();
    }

    public Priority getPriority()
    {
        return priority.get();
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    // Returns any remaining splits. The caller must destroy these.
    public synchronized List<PrioritizedSplitRunner> destroy()
    {
        destroyed = true;

        ImmutableList.Builder<PrioritizedSplitRunner> builder = ImmutableList.builder();
        builder.addAll(runningIntermediateSplits);
        builder.addAll(runningLeafSplits);
        builder.addAll(queuedLeafSplits);
        runningIntermediateSplits.clear();
        runningLeafSplits.clear();
        queuedLeafSplits.clear();
        return builder.build();
    }

    public synchronized void enqueueSplit(PrioritizedSplitRunner split)
    {
        checkState(!destroyed, "Cannot add split to destroyed task handle");
        queuedLeafSplits.add(split);
    }

    public synchronized void recordIntermediateSplit(PrioritizedSplitRunner split)
    {
        checkState(!destroyed, "Cannot add split to destroyed task handle");
        runningIntermediateSplits.add(split);
    }

    synchronized int getRunningLeafSplits()
    {
        return runningLeafSplits.size();
    }

    public synchronized long getScheduledNanos()
    {
        return scheduledNanos;
    }

    public synchronized PrioritizedSplitRunner pollNextSplit()
    {
        if (destroyed) {
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
}
