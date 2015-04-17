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

import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

class ActualProperties
{
    private final Optional<Set<Symbol>> partitioningColumns; // if missing => partitioned with some unknown scheme
    private final Optional<List<Symbol>> hashingColumns; // if present => hash partitioned on the given columns. partitioningColumns and hashingColumns must contain the same columns
    private final boolean coordinatorOnly;
    private final List<LocalProperty<Symbol>> localProperties;
    private final Map<Symbol, Object> constants;

    public ActualProperties(
            Optional<Set<Symbol>> partitioningColumns,
            Optional<List<Symbol>> hashingColumns,
            List<? extends LocalProperty<Symbol>> localProperties,
            boolean coordinatorOnly,
            Map<Symbol, Object> constants)
    {
        requireNonNull(partitioningColumns, "partitioningColumns is null");
        requireNonNull(hashingColumns, "hashingColumns is null");
        requireNonNull(localProperties, "localProperties is null");
        requireNonNull(constants, "constants is null");

        this.partitioningColumns = partitioningColumns;
        this.hashingColumns = hashingColumns;
        this.localProperties = ImmutableList.copyOf(localProperties);
        this.coordinatorOnly = coordinatorOnly;
        this.constants = Collections.unmodifiableMap(new HashMap<>(constants));
    }

    public static ActualProperties unpartitioned()
    {
        return new ActualProperties(Optional.of(ImmutableSet.of()), Optional.empty(), ImmutableList.of(), false, ImmutableMap.of());
    }

    public static ActualProperties hashPartitioned(List<Symbol> columns)
    {
        return new ActualProperties(
                Optional.of(ImmutableSet.copyOf(columns)),
                Optional.of(ImmutableList.copyOf(columns)),
                ImmutableList.of(),
                false,
                ImmutableMap.of());
    }

    public static ActualProperties partitioned()
    {
        return new ActualProperties(Optional.<Set<Symbol>>empty(), Optional.<List<Symbol>>empty(), ImmutableList.of(), false, ImmutableMap.of());
    }

    public static ActualProperties partitioned(Collection<Symbol> columns)
    {
        return new ActualProperties(Optional.<Set<Symbol>>of(ImmutableSet.copyOf(columns)), Optional.<List<Symbol>>empty(), ImmutableList.of(), false, ImmutableMap.of());
    }

    public static ActualProperties partitioned(ActualProperties other)
    {
        return new ActualProperties(other.partitioningColumns, other.hashingColumns, ImmutableList.of(), false, ImmutableMap.of());
    }

    public boolean isCoordinatorOnly()
    {
        return coordinatorOnly;
    }

    public boolean isPartitioned()
    {
        return !partitioningColumns.isPresent() || !partitioningColumns.get().isEmpty();
    }

    public boolean isPartitionedOn(Collection<Symbol> columns)
    {
        // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
        // constant partitioning columns can be ignored without impact
        return partitioningColumns.isPresent() && partitioningColumns.get().stream()
                .filter(symbol -> !constants.containsKey(symbol))
                .allMatch(ImmutableSet.copyOf(columns)::contains);
    }

    public boolean hasKnownPartitioningScheme()
    {
        return partitioningColumns.isPresent();
    }

    public Optional<Set<Symbol>> getPartitioningColumns()
    {
        return partitioningColumns;
    }

    public boolean isHashPartitionedOn(List<Symbol> columns)
    {
        return hashingColumns.isPresent() && hashingColumns.get().equals(columns);
    }

    public boolean isHashPartitioned()
    {
        return hashingColumns.isPresent();
    }

    public Optional<List<Symbol>> getHashPartitioningColumns()
    {
        return hashingColumns;
    }

    public boolean isUnpartitioned()
    {
        return partitioningColumns.isPresent() && partitioningColumns.get().isEmpty();
    }

    public Map<Symbol, Object> getConstants()
    {
        return constants;
    }

    public List<LocalProperty<Symbol>> getLocalProperties()
    {
        return localProperties;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Set<Symbol> getMaxGroupingSubset(List<Symbol> columns)
    {
        return LocalProperty.getMaxGroupingSubset(localProperties, constants.keySet(), columns);
    }

    public static class Builder
    {
        private Optional<Set<Symbol>> partitioningColumns; // if missing => partitioned with some unknown scheme
        private Optional<List<Symbol>> hashingColumns; // if present => hash partitioned on the given columns. partitioningColumns and hashingColumns must contain the same columns
        private boolean coordinatorOnly;
        private List<LocalProperty<Symbol>> localProperties = ImmutableList.of();
        private Map<Symbol, Object> constants = ImmutableMap.of();

        public Builder unpartitioned()
        {
            partitioningColumns = Optional.of(ImmutableSet.of());
            hashingColumns = Optional.empty();

            return this;
        }

        public Builder partitioned(ActualProperties other)
        {
            partitioningColumns = other.partitioningColumns;
            hashingColumns = other.hashingColumns;

            return this;
        }

        public Builder partitioned()
        {
            partitioningColumns = Optional.empty();
            hashingColumns = Optional.empty();

            return this;
        }

        public Builder coordinatorOnly(ActualProperties other)
        {
            coordinatorOnly = other.coordinatorOnly;

            return this;
        }

        public Builder partitioned(Set<Symbol> columns)
        {
            partitioningColumns = Optional.of(ImmutableSet.copyOf(columns));
            hashingColumns = Optional.empty();

            return this;
        }

        public Builder hashPartitioned(List<Symbol> columns)
        {
            partitioningColumns = Optional.of(ImmutableSet.copyOf(columns));
            hashingColumns = Optional.of(ImmutableList.copyOf(columns));

            return this;
        }

        public Builder local(List<? extends LocalProperty<Symbol>> localProperties)
        {
            this.localProperties = ImmutableList.copyOf(localProperties);
            return this;
        }

        public Builder local(ActualProperties other)
        {
            this.localProperties = ImmutableList.copyOf(other.localProperties);
            return this;
        }

        public Builder constants(Map<Symbol, Object> constants)
        {
            this.constants = Collections.unmodifiableMap(new HashMap<>(constants));
            return this;
        }

        public Builder constants(ActualProperties other)
        {
            this.constants = Collections.unmodifiableMap(new HashMap<>(other.constants));
            return this;
        }

        public ActualProperties build()
        {
            return new ActualProperties(partitioningColumns, hashingColumns, localProperties, coordinatorOnly, constants);
        }
    }
}
