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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class ScheduleResult
{
    public enum BlockedReason
    {
        /**
         * When using scaled writers, we block after each scheduling call
         * to introduce a short delay between scheduling iterations. This allows
         * us to determine whether we need to scale up the number of tasks.
         */
        WRITER_SCALING,
        /**
         * All the lifespans currently scheduled have finished assigning all
         * their splits. This may mean that all splits have been assigned
         * and we are done scheduling the source, or it may mean that more lifespans
         * need to be scheduled. The latter is only applicable for grouped
         * execution where there are multiple lifespans per task.
         */
        NO_ACTIVE_DRIVER_GROUP,
        /**
         * Some or all splits could not be assigned because none of the candidate
         * nodes had space in the queue.
         */
        SPLIT_QUEUES_FULL,
        /**
         * Waiting for the source to return the next batch of splits.
         */
        WAITING_FOR_SOURCE,
        /**
         * At least one lifespan is blocked on SPLIT_QUEUES_FULL
         * and at least one is blocked on WAITING_FOR_SOURCE (only relevant for
         * grouped execution where there are multiple lifespans per task).
         */
        MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE,
        /**/;

        public BlockedReason combineWith(BlockedReason other)
        {
            switch (this) {
                case WRITER_SCALING:
                    throw new IllegalArgumentException("cannot be combined");
                case NO_ACTIVE_DRIVER_GROUP:
                    return other;
                case SPLIT_QUEUES_FULL:
                    return other == SPLIT_QUEUES_FULL || other == NO_ACTIVE_DRIVER_GROUP ? SPLIT_QUEUES_FULL : MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE;
                case WAITING_FOR_SOURCE:
                    return other == WAITING_FOR_SOURCE || other == NO_ACTIVE_DRIVER_GROUP ? WAITING_FOR_SOURCE : MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE;
                case MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE:
                    return MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE;
                default:
                    throw new IllegalArgumentException("Unknown blocked reason: " + other);
            }
        }
    }

    private final Set<RemoteTask> newTasks;
    private final ListenableFuture<?> blocked;
    private final Optional<BlockedReason> blockedReason;
    private final boolean finished;
    private final int splitsScheduled;

    public static ScheduleResult nonBlocked(boolean finished, Iterable<? extends RemoteTask> newTasks, int splitsScheduled)
    {
        return new ScheduleResult(finished, newTasks, immediateFuture(null), Optional.empty(), splitsScheduled);
    }

    public static ScheduleResult blocked(boolean finished, Iterable<? extends RemoteTask> newTasks, ListenableFuture<?> blocked, BlockedReason blockedReason, int splitsScheduled)
    {
        return new ScheduleResult(finished, newTasks, blocked, Optional.of(requireNonNull(blockedReason, "blockedReason is null")), splitsScheduled);
    }

    private ScheduleResult(boolean finished, Iterable<? extends RemoteTask> newTasks, ListenableFuture<?> blocked, Optional<BlockedReason> blockedReason, int splitsScheduled)
    {
        this.finished = finished;
        this.newTasks = ImmutableSet.copyOf(requireNonNull(newTasks, "newTasks is null"));
        this.blocked = requireNonNull(blocked, "blocked is null");
        this.blockedReason = requireNonNull(blockedReason, "blockedReason is null");
        this.splitsScheduled = splitsScheduled;
    }

    public boolean isFinished()
    {
        return finished;
    }

    public Set<RemoteTask> getNewTasks()
    {
        return newTasks;
    }

    public ListenableFuture<?> getBlocked()
    {
        return blocked;
    }

    public int getSplitsScheduled()
    {
        return splitsScheduled;
    }

    public Optional<BlockedReason> getBlockedReason()
    {
        return blockedReason;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("finished", finished)
                .add("newTasks", newTasks.size())
                .add("blocked", !blocked.isDone())
                .add("splitsScheduled", splitsScheduled)
                .add("blockedReason", blockedReason)
                .toString();
    }
}
