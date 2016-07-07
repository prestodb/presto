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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public final class PreferredPartitioning
{
    private final Set<Symbol> partitioningColumns;
    private final Optional<Partitioning> partitioning; // Specific partitioning requested
    private final boolean nullsReplicated;

    private PreferredPartitioning(Set<Symbol> partitioningColumns, Optional<Partitioning> partitioning, boolean nullsReplicated)
    {
        this.partitioningColumns = ImmutableSet.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
        this.partitioning = requireNonNull(partitioning, "function is null");
        this.nullsReplicated = nullsReplicated;

        checkArgument(!partitioning.isPresent() || partitioning.get().getColumns().equals(partitioningColumns), "Partitioning input must match partitioningColumns");
    }

    public PreferredPartitioning withNullsReplicated(boolean nullsReplicated)
    {
        return new PreferredPartitioning(partitioningColumns, partitioning, nullsReplicated);
    }

    public static PreferredPartitioning partitioned(Partitioning partitioning)
    {
        return new PreferredPartitioning(partitioning.getColumns(), Optional.of(partitioning), false);
    }

    public static PreferredPartitioning partitioned(Set<Symbol> columns)
    {
        return new PreferredPartitioning(columns, Optional.empty(), false);
    }

    public static PreferredPartitioning singlePartition()
    {
        return partitioned(ImmutableSet.of());
    }

    public Set<Symbol> getPartitioningColumns()
    {
        return partitioningColumns;
    }

    public Optional<Partitioning> getPartitioning()
    {
        return partitioning;
    }

    public boolean isNullsReplicated()
    {
        return nullsReplicated;
    }

    public PreferredPartitioning mergeWithParent(PreferredPartitioning parent)
    {
        // Non-negotiable if we require a specific partitioning
        if (partitioning.isPresent()) {
            return this;
        }

        // Partitioning with different null replication cannot be compared
        if (nullsReplicated != parent.nullsReplicated) {
            return this;
        }

        if (parent.partitioning.isPresent()) {
            // If the parent has a partitioning preference, propagate parent only if the parent's partitioning columns satisfies our preference.
            // Otherwise, ignore the parent since the parent will have to repartition anyways.
            return partitioningColumns.containsAll(parent.partitioningColumns) ? parent : this;
        }

        // Otherwise partition on any common columns if available
        Set<Symbol> common = Sets.intersection(partitioningColumns, parent.partitioningColumns);
        return common.isEmpty() ? this : partitioned(common).withNullsReplicated(nullsReplicated);
    }

    public Optional<PreferredPartitioning> translate(Function<Symbol, Optional<Symbol>> translator)
    {
        Set<Symbol> newPartitioningColumns = partitioningColumns.stream()
                .map(translator)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableSet());

        // Translation fails if we have prior partitioning columns and none could be translated
        if (!partitioningColumns.isEmpty() && newPartitioningColumns.isEmpty()) {
            return Optional.empty();
        }

        if (!partitioning.isPresent()) {
            return Optional.of(new PreferredPartitioning(newPartitioningColumns, Optional.empty(), nullsReplicated));
        }

        Optional<Partitioning> newPartitioning = partitioning.get().translate(translator, symbol -> Optional.empty());
        if (!newPartitioning.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new PreferredPartitioning(newPartitioningColumns, newPartitioning, nullsReplicated));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioningColumns, partitioning, nullsReplicated);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final PreferredPartitioning other = (PreferredPartitioning) obj;
        return Objects.equals(this.partitioningColumns, other.partitioningColumns)
                && Objects.equals(this.partitioning, other.partitioning)
                && this.nullsReplicated == other.nullsReplicated;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitioningColumns", partitioningColumns)
                .add("partitioning", partitioning)
                .add("nullsReplicated", nullsReplicated)
                .toString();
    }
}
