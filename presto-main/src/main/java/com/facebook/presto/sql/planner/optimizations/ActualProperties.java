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

import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

class ActualProperties
{
    private final Optional<Set<Symbol>> partitioningColumns; // if missing => partitioned with some unknown scheme
    private final Optional<List<Symbol>> hashingColumns; // if present => hash partitioned on the given columns. partitioningColumns and hashingColumns must contain the same columns
    private final boolean partitioned; // true if executing on multiple instances; false if executing on a single instance, which implies partitioned on the empty set of columns
    private final boolean coordinatorOnly;
    private final List<LocalProperty<Symbol>> localProperties;
    private final Map<Symbol, Object> constants;

    private ActualProperties(
            Optional<Set<Symbol>> partitioningColumns,
            Optional<List<Symbol>> hashingColumns,
            List<? extends LocalProperty<Symbol>> localProperties,
            boolean partitioned,
            boolean coordinatorOnly,
            Map<Symbol, Object> constants)
    {
        requireNonNull(partitioningColumns, "partitioningColumns is null");
        requireNonNull(hashingColumns, "hashingColumns is null");
        checkArgument(!hashingColumns.isPresent() || hashingColumns.map(ImmutableSet::copyOf).equals(partitioningColumns), "hashColumns must contain the columns as partitioningColumns if present");
        checkArgument(partitioned || partitioningColumns.isPresent() && partitioningColumns.get().isEmpty(), "partitioningColumns must contain the empty set when unpartitioned");
        checkArgument(!coordinatorOnly || !partitioned, "must not be partitioned when running as coordinatorOnly");
        requireNonNull(localProperties, "localProperties is null");
        requireNonNull(constants, "constants is null");

        this.partitioningColumns = partitioningColumns.map(ImmutableSet::copyOf);
        this.hashingColumns = hashingColumns.map(ImmutableList::copyOf);
        this.partitioned = partitioned;
        this.coordinatorOnly = coordinatorOnly;

        // There is some overlap between the localProperties (ConstantProperty) and constants fields.
        // Let's normalize both of those structures here so that they are consistent with each other
        Set<Symbol> propertyConstants = LocalProperties.extractLeadingConstants(localProperties);
        localProperties = LocalProperties.stripLeadingConstants(localProperties);

        Map<Symbol, Object> updatedConstants = new HashMap<>();
        propertyConstants.stream()
                .forEach(symbol -> updatedConstants.put(symbol, new Object()));
        updatedConstants.putAll(constants);

        List<LocalProperty<Symbol>> updatedLocalProperties = LocalProperties.normalizeAndPrune(ImmutableList.<LocalProperty<Symbol>>builder()
                .addAll(transform(updatedConstants.keySet(), ConstantProperty::new))
                .addAll(localProperties)
                .build());

        this.localProperties = ImmutableList.copyOf(updatedLocalProperties);
        this.constants = ImmutableMap.copyOf(updatedConstants);
    }

    public static ActualProperties unpartitioned()
    {
        return new ActualProperties(Optional.of(ImmutableSet.of()), Optional.empty(), ImmutableList.of(), false, false, ImmutableMap.of());
    }

    public static ActualProperties hashPartitioned(List<Symbol> columns)
    {
        return new ActualProperties(
                Optional.of(ImmutableSet.copyOf(columns)),
                Optional.of(ImmutableList.copyOf(columns)),
                ImmutableList.of(),
                true,
                false,
                ImmutableMap.of());
    }

    public static ActualProperties partitioned()
    {
        return new ActualProperties(Optional.<Set<Symbol>>empty(), Optional.<List<Symbol>>empty(), ImmutableList.of(), true, false, ImmutableMap.of());
    }

    public static ActualProperties partitioned(Collection<Symbol> columns)
    {
        return new ActualProperties(Optional.<Set<Symbol>>of(ImmutableSet.copyOf(columns)), Optional.<List<Symbol>>empty(), ImmutableList.of(), true, false, ImmutableMap.of());
    }

    public static ActualProperties partitioned(ActualProperties other)
    {
        return new ActualProperties(other.partitioningColumns, other.hashingColumns, ImmutableList.of(), other.partitioned, false, ImmutableMap.of());
    }

    public boolean isCoordinatorOnly()
    {
        return coordinatorOnly;
    }

    public boolean isPartitioned()
    {
        return partitioned;
    }

    public boolean isPartitionedOn(Collection<Symbol> columns)
    {
        // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
        // can safely ignore all constant columns when comparing partition properties
        return partitioningColumns.isPresent() && ImmutableSet.copyOf(columns).containsAll(getPartitioningColumnsWithoutConstants().get());
    }

    /**
     * @return true if all the data will effectively land in a single stream
     */
    public boolean isSingleStream()
    {
        Optional<Set<Symbol>> partitioningWithoutConstants = getPartitioningColumnsWithoutConstants();
        return partitioningWithoutConstants.isPresent() && partitioningWithoutConstants.get().isEmpty();
    }

    /**
     * @return true if repartitioning on the keys will yield some difference
     */
    public boolean isRepartitionEffective(Collection<Symbol> keys)
    {
        Optional<Set<Symbol>> partitioningWithoutConstants = getPartitioningColumnsWithoutConstants();
        if (!partitioningWithoutConstants.isPresent()) {
            return true;
        }
        Set<Symbol> keysWithoutConstants = keys.stream()
                .filter(symbol -> !constants.containsKey(symbol))
                .collect(toImmutableSet());
        return !partitioningWithoutConstants.get().equals(keysWithoutConstants);
    }

    public boolean hasKnownPartitioningScheme()
    {
        return partitioningColumns.isPresent();
    }

    public Optional<Set<Symbol>> getPartitioningColumns()
    {
        // can safely ignore all constant columns when comparing partition properties
        return getPartitioningColumnsWithoutConstants();
    }

    private Optional<Set<Symbol>> getPartitioningColumnsWithoutConstants()
    {
        return partitioningColumns.map(symbols -> {
                    return symbols.stream()
                            .filter(symbol -> !constants.containsKey(symbol))
                            .collect(toImmutableSet());
                }
        );
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

    public static class Builder
    {
        private Optional<Set<Symbol>> partitioningColumns; // if missing => partitioned with some unknown scheme
        private Optional<List<Symbol>> hashingColumns; // if present => hash partitioned on the given columns. partitioningColumns and hashingColumns must contain the same columns
        private boolean partitioned; // true if executing on multiple instances; false if executing on a single instance, which implies partitioned on the empty set of columns
        private boolean coordinatorOnly;
        private List<LocalProperty<Symbol>> localProperties = ImmutableList.of();
        private Map<Symbol, Object> constants = ImmutableMap.of();

        public Builder unpartitioned()
        {
            partitioningColumns = Optional.of(ImmutableSet.of());
            hashingColumns = Optional.empty();
            partitioned = false;

            return this;
        }

        public Builder partitioned(ActualProperties other)
        {
            partitioningColumns = other.partitioningColumns;
            hashingColumns = other.hashingColumns;
            partitioned = other.partitioned;

            return this;
        }

        public Builder partitioned()
        {
            partitioningColumns = Optional.empty();
            hashingColumns = Optional.empty();
            partitioned = true;

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
            partitioned = true;

            return this;
        }

        public Builder hashPartitioned(List<Symbol> columns)
        {
            partitioningColumns = Optional.of(ImmutableSet.copyOf(columns));
            hashingColumns = Optional.of(ImmutableList.copyOf(columns));
            partitioned = true;

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
            this.constants = ImmutableMap.copyOf(constants);
            return this;
        }

        public Builder constants(ActualProperties other)
        {
            this.constants = ImmutableMap.copyOf(other.constants);
            return this;
        }

        public ActualProperties build()
        {
            return new ActualProperties(partitioningColumns, hashingColumns, localProperties, partitioned, coordinatorOnly, constants);
        }
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("partitioningColumns", partitioningColumns)
                .add("hashingColumns", hashingColumns)
                .add("partitioned", partitioned)
                .add("coordinatorOnly", coordinatorOnly)
                .add("localProperties", localProperties)
                .add("constants", constants)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioningColumns, hashingColumns, partitioned, coordinatorOnly, localProperties, constants.keySet());
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
        final ActualProperties other = (ActualProperties) obj;
        return Objects.equals(this.partitioningColumns, other.partitioningColumns)
                && Objects.equals(this.hashingColumns, other.hashingColumns)
                && Objects.equals(this.partitioned, other.partitioned)
                && Objects.equals(this.coordinatorOnly, other.coordinatorOnly)
                && Objects.equals(this.localProperties, other.localProperties)
                && Objects.equals(this.constants.keySet(), other.constants.keySet());
    }
}
