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
 * Represents a dynamic filter that can be used to prune partitions and files
 * during split generation in connectors.
 *
 * <p>Dynamic filters are collected from join build sides at runtime and passed
 * to connectors during split scheduling. Connectors can use the filter constraints
 * to skip partitions and files that cannot match the join condition.
 *
 * <p>Connectors use the {@link #isBlocked()} / {@link #getCurrentPredicate()}
 * loop: wait for the future, read the predicate, repeat until
 * {@link #isComplete()}.
 *
 * <p>The predicate returned by {@link #getCurrentPredicate()} narrows monotonically
 * as individual underlying filters resolve: incomplete filters contribute
 * {@code TupleDomain.all()} (no constraint), so the intersection tightens whenever
 * any one of them completes. Connectors can re-read the predicate on each batch to
 * pick up later-arriving filters without blocking on the slowest one — see
 * {@link #hasAnyComplete}.
 */
public interface DynamicFilter
{
    /** Sentinel future indicating the filter is not blocked. */
    CompletableFuture<?> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    /**
     * A dynamic filter that is already complete with no filtering applied.
     * Use this when dynamic filtering is disabled or not applicable.
     */
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

    /** Returns {@code TupleDomain.all()} until the filter is resolved. */
    TupleDomain<ColumnHandle> getCurrentPredicate();

    /**
     * Completes when the predicate changes. Starts the wait timeout on first call.
     * Returns {@link #NOT_BLOCKED} when complete.
     */
    CompletableFuture<?> isBlocked();

    /**
     * Returns a hint for the number of tasks that will consume splits from this scan.
     * Connectors can use this to compute weight-based warmup budgets (e.g., warmup splits
     * per task). Returns 0 if the task count is unknown.
     */
    int getTaskCountHint();

    /**
     * Returns column handles targeted by pending (not yet resolved) dynamic filters.
     * Allows connectors to pre-evaluate column discriminating power before values arrive.
     */
    Set<ColumnHandle> getPendingFilterColumns();

    /**
     * Returns a future that completes when any filter on a relevant column resolves.
     * Filters on columns NOT in relevantColumns are not waited on.
     * {@code Optional.empty()} means all columns are relevant and delegates to {@link #isBlocked()}.
     */
    CompletableFuture<?> isBlocked(Optional<Set<ColumnHandle>> relevantColumns);

    /**
     * Returns true if all filters on relevant columns are complete.
     * Filters on irrelevant columns are treated as already complete.
     * {@code Optional.empty()} means all columns are relevant and delegates to {@link #isComplete()}.
     */
    boolean isComplete(Optional<Set<ColumnHandle>> relevantColumns);

    /**
     * Returns true if at least one filter on a relevant column is complete.
     * Filters on irrelevant columns are ignored. If there are no relevant filters,
     * returns true (no waiting required).
     *
     * <p>Use this in preference to {@link #isComplete(Optional)} for warmup-exit
     * gates: it lets the scan exit as soon as some useful constraint is available
     * without blocking on the slowest filter. Late-arriving filters still narrow
     * {@link #getCurrentPredicate()} on subsequent reads.
     */
    boolean hasAnyComplete(Optional<Set<ColumnHandle>> relevantColumns);
}
