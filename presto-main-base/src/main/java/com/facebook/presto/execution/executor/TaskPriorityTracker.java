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

import javax.annotation.concurrent.GuardedBy;

import static java.util.Objects.requireNonNull;

public class TaskPriorityTracker
{
    private final MultilevelSplitQueue splitQueue;

    @GuardedBy("this")
    private long scheduledNanos;
    @GuardedBy("this")
    private volatile Priority priority = new Priority(0, 0);

    public TaskPriorityTracker(MultilevelSplitQueue splitQueue)
    {
        this.splitQueue = requireNonNull(splitQueue, "splitQueue is null");
    }

    public synchronized Priority updatePriority(long durationNanos)
    {
        scheduledNanos += durationNanos;

        Priority newPriority = splitQueue.updatePriority(priority, durationNanos, scheduledNanos);

        priority = newPriority;
        return newPriority;
    }

    public synchronized Priority resetLevelPriority()
    {
        long levelMinPriority = splitQueue.getLevelMinPriority(priority.getLevel(), scheduledNanos);
        if (priority.getLevelPriority() < levelMinPriority) {
            Priority newPriority = new Priority(priority.getLevel(), levelMinPriority);
            priority = newPriority;
            return newPriority;
        }

        return priority;
    }

    public synchronized long getScheduledNanos()
    {
        return scheduledNanos;
    }

    public synchronized Priority getPriority()
    {
        return priority;
    }
}
