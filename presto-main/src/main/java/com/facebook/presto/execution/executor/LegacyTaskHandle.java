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

import com.facebook.presto.execution.TaskId;
import io.airlift.units.Duration;

import java.util.function.DoubleSupplier;

import static com.facebook.presto.execution.executor.MultilevelSplitQueue.LEVEL_THRESHOLD_SECONDS;
import static com.facebook.presto.execution.executor.MultilevelSplitQueue.computeLevel;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LegacyTaskHandle
        extends TaskHandle
{
    public LegacyTaskHandle(TaskId taskId, MultilevelSplitQueue splitQueue, DoubleSupplier utilizationSupplier, int initialSplitConcurrency, Duration splitConcurrencyAdjustFrequency)
    {
        super(taskId, splitQueue, utilizationSupplier, initialSplitConcurrency, splitConcurrencyAdjustFrequency);
    }

    @Override
    public synchronized Priority addScheduledNanos(long durationNanos)
    {
        concurrencyController.update(durationNanos, utilizationSupplier.getAsDouble(), runningLeafSplits.size());
        scheduledNanos += durationNanos;

        Priority oldPriority = priority.get();
        Priority newPriority;

        if (oldPriority.getLevel() < (LEVEL_THRESHOLD_SECONDS.length - 1) && scheduledNanos >= SECONDS.toNanos(LEVEL_THRESHOLD_SECONDS[oldPriority.getLevel() + 1])) {
            int newLevel = computeLevel(scheduledNanos);
            newPriority = new Priority(newLevel, scheduledNanos);
        }
        else {
            newPriority = new Priority(oldPriority.getLevel(), scheduledNanos);
        }

        priority.set(newPriority);
        return newPriority;
    }

    @Override
    public synchronized Priority resetLevelPriority()
    {
        return priority.get();
    }
}
