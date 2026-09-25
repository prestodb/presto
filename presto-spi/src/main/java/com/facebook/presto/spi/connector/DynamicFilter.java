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
package com.facebook.presto.spi.connector;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A dynamic filter collected from a join build side at runtime and passed to a
 * connector during split scheduling, so the connector can skip partitions and files
 * that cannot match the join.
 *
 * <p>Usage: wait on {@link #isBlocked()}, read {@link #getCurrentPredicate()}, repeat
 * until {@link #isComplete()}. The predicate narrows monotonically — unresolved
 * filters contribute {@link TupleDomain#all()}, so re-reading it picks up
 * later-arriving filters without blocking on the slowest one.
 *
 * <p>The {@code relevantColumns} overloads restrict waiting to filters over the given
 * columns; {@code Optional.empty()} means all columns.
 */
public interface DynamicFilter
{
    CompletableFuture<?> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    /** A complete filter that applies no filtering; use when dynamic filtering is disabled or not applicable. */
    DynamicFilter EMPTY = new DynamicFilter()
    {
        @Override
        public Duration getWaitTimeout()
        {
            return new Duration(0, TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean isComplete()
        {
            return true;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return TupleDomain.all();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return NOT_BLOCKED;
        }

        @Override
        public int getTaskCountHint()
        {
            return 0;
        }

        @Override
        public Set<ColumnHandle> getPendingFilterColumns()
        {
            return Collections.emptySet();
        }

        @Override
        public CompletableFuture<?> isBlocked(Optional<Set<ColumnHandle>> relevantColumns)
        {
            return NOT_BLOCKED;
        }

        @Override
        public boolean isComplete(Optional<Set<ColumnHandle>> relevantColumns)
        {
            return true;
        }

        @Override
        public boolean hasAnyComplete(Optional<Set<ColumnHandle>> relevantColumns)
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "DynamicFilter.EMPTY";
        }
    };

    Duration getWaitTimeout();

    boolean isComplete();

    /** Returns {@link TupleDomain#all()} until the filter resolves. */
    TupleDomain<ColumnHandle> getCurrentPredicate();

    /** Completes when the predicate changes; starts the wait timeout on first call. */
    CompletableFuture<?> isBlocked();

    /** Hint for the number of tasks consuming this scan's splits, for warmup budgets; 0 if unknown. */
    int getTaskCountHint();

    /** Column handles targeted by pending (unresolved) filters. */
    Set<ColumnHandle> getPendingFilterColumns();

    CompletableFuture<?> isBlocked(Optional<Set<ColumnHandle>> relevantColumns);

    boolean isComplete(Optional<Set<ColumnHandle>> relevantColumns);

    /**
     * True if at least one filter over {@code relevantColumns} is complete, or none are relevant.
     * Lets a warmup scan exit on the first useful constraint rather than blocking on the slowest filter.
     */
    boolean hasAnyComplete(Optional<Set<ColumnHandle>> relevantColumns);
}
