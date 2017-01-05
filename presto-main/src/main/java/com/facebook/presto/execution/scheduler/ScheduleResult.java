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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ScheduleResult
{
    public enum BlockedReason
    {
        SPLIT_QUEUES_FULL,
        WAITING_FOR_SOURCE
    }

    private final Set<RemoteTask> newTasks;
    private final CompletableFuture<?> blocked;
    private final Optional<BlockedReason> blockedReason;
    private final boolean finished;
    private final int splitsScheduled;

    public ScheduleResult(boolean finished, Iterable<? extends RemoteTask> newTasks, int splitsScheduled)
    {
        this(finished, newTasks, completedFuture(null), Optional.empty(), splitsScheduled);
    }

    public ScheduleResult(boolean finished, Iterable<? extends RemoteTask> newTasks, CompletableFuture<?> blocked, BlockedReason blockedReason, int splitsScheduled)
    {
        this(finished, newTasks, blocked, Optional.of(requireNonNull(blockedReason, "blockedReason is null")), splitsScheduled);
    }

    private ScheduleResult(boolean finished, Iterable<? extends RemoteTask> newTasks, CompletableFuture<?> blocked, Optional<BlockedReason> blockedReason, int splitsScheduled)
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

    public CompletableFuture<?> getBlocked()
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
                .add("blocked", blocked.isDone())
                .add("splitsScheduled", splitsScheduled)
                .add("blockedReason", blockedReason)
                .toString();
    }
}
