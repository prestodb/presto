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

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

class ActualProperties
{
    private final Global global;
    private final List<LocalProperty<Symbol>> localProperties;
    private final Map<Symbol, Object> constants;

    private ActualProperties(
            Global global,
            List<? extends LocalProperty<Symbol>> localProperties,
            Map<Symbol, Object> constants)
    {
        requireNonNull(global, "globalProperties is null");
        requireNonNull(localProperties, "localProperties is null");
        requireNonNull(constants, "constants is null");

        this.global = global;

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

    public static ActualProperties distributed()
    {
        return builder()
                .global(Global.distributed())
                .build();
    }

    public static ActualProperties undistributed()
    {
        return builder()
                .global(Global.undistributed())
                .build();
    }

    public static ActualProperties partitioned(Set<Symbol> columns)
    {
        return builder()
                .global(Global.distributed(Partitioning.partitioned(columns)))
                .build();
    }

    public static ActualProperties hashPartitioned(List<Symbol> columns)
    {
        return builder()
                .global(Global.distributed(Partitioning.hashPartitioned(columns)))
                .build();
    }

    public boolean isCoordinatorOnly()
    {
        return global.isCoordinatorOnly();
    }

    public boolean isDistributed()
    {
        return global.isDistributed();
    }

    public boolean isNullReplication()
    {
        checkState(global.getPartitioningProperties().isPresent());
        return global.getPartitioningProperties().get().isReplicateNulls();
    }

    public boolean isPartitionedOn(Collection<Symbol> columns)
    {
        return global.getPartitioningProperties().isPresent() && global.getPartitioningProperties().get().isPartitionedOn(columns, constants.keySet());
    }

    /**
     * @return true if all the data will effectively land in a single stream
     */
    public boolean isEffectivelySinglePartition()
    {
        return global.getPartitioningProperties().isPresent() && global.getPartitioningProperties().get().isEffectivelySinglePartition(constants.keySet());
    }

    /**
     * @return true if repartitioning on the keys will yield some difference
     */
    public boolean isRepartitionEffective(Collection<Symbol> keys)
    {
        return !global.getPartitioningProperties().isPresent() || global.getPartitioningProperties().get().isRepartitionEffective(keys, constants.keySet());
    }

    public ActualProperties translate(Function<Symbol, Optional<Symbol>> translator)
    {
        Map<Symbol, Object> translatedConstants = new HashMap<>();
        for (Map.Entry<Symbol, Object> entry : constants.entrySet()) {
            Optional<Symbol> translatedKey = translator.apply(entry.getKey());
            if (translatedKey.isPresent()) {
                translatedConstants.put(translatedKey.get(), entry.getValue());
            }
        }
        return builder()
                .global(global.translate(translator))
                .local(LocalProperties.translate(localProperties, translator))
                .constants(translatedConstants)
                .build();
    }

    public boolean isHashPartitionedOn(List<Symbol> columns)
    {
        return global.getPartitioningProperties().isPresent() && global.getPartitioningProperties().get().isHashPartitionedOn(columns);
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

    public static Builder builderFrom(ActualProperties properties)
    {
        return new Builder(properties.global, properties.localProperties, properties.constants);
    }

    public static class Builder
    {
        private Global global;
        private List<LocalProperty<Symbol>> localProperties;
        private Map<Symbol, Object> constants;

        public Builder(Global global, List<LocalProperty<Symbol>> localProperties, Map<Symbol, Object> constants)
        {
            this.global = global;
            this.localProperties = localProperties;
            this.constants = constants;
        }

        public Builder()
        {
            this.global = null;
            this.localProperties = ImmutableList.of();
            this.constants = ImmutableMap.of();
        }

        public Builder global(Global global)
        {
            this.global = global;
            return this;
        }

        public Builder global(ActualProperties other)
        {
            this.global = other.global;
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
            return new ActualProperties(global, localProperties, constants);
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(global, localProperties, constants.keySet());
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
        return Objects.equals(this.global, other.global)
                && Objects.equals(this.localProperties, other.localProperties)
                && Objects.equals(this.constants.keySet(), other.constants.keySet());
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("globalProperties", global)
                .add("localProperties", localProperties)
                .add("constants", constants)
                .toString();
    }

    @Immutable
    public static final class Global
    {
        private final boolean distributed; // true => plan will be distributed to multiple servers, false => plan is locked to a single server
        private final boolean coordinatorOnly;
        private final Optional<Partitioning> partitioningProperties; // if missing => partitioned with some unknown scheme

        // NOTE: Partitioning on zero columns (or effectively zero columns if the columns are constant) indicates that all
        // the rows will be partitioned into a single stream. However, this can still be a distributed plan in that the plan
        // will be distributed to multiple servers, but only one server will get all the data.

        private Global(boolean distributed, boolean coordinatorOnly, Optional<Partitioning> partitioningProperties)
        {
            this.distributed = distributed;
            this.coordinatorOnly = coordinatorOnly;
            this.partitioningProperties = Objects.requireNonNull(partitioningProperties, "partitioningProperties is null");
        }

        public static Global coordinatorOnly()
        {
            return new Global(false, true, Optional.of(Partitioning.singlePartition()));
        }

        public static Global undistributed()
        {
            return new Global(false, false, Optional.of(Partitioning.singlePartition()));
        }

        public static Global distributed(Optional<Partitioning> partitioningProperties)
        {
            return new Global(true, false, partitioningProperties);
        }

        public static Global distributed()
        {
            return distributed(Optional.<Partitioning>empty());
        }

        public static Global distributed(Partitioning partitioning)
        {
            return distributed(Optional.of(partitioning));
        }

        public boolean isDistributed()
        {
            return distributed;
        }

        public boolean isCoordinatorOnly()
        {
            return coordinatorOnly;
        }

        public Optional<Partitioning> getPartitioningProperties()
        {
            return partitioningProperties;
        }

        public Global translate(Function<Symbol, Optional<Symbol>> translator)
        {
            return new Global(distributed, coordinatorOnly, partitioningProperties.flatMap(properties -> properties.translate(translator)));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(distributed, coordinatorOnly, partitioningProperties);
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
            final Global other = (Global) obj;
            return Objects.equals(this.distributed, other.distributed)
                    && Objects.equals(this.coordinatorOnly, other.coordinatorOnly)
                    && Objects.equals(this.partitioningProperties, other.partitioningProperties);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("distributed", distributed)
                    .add("coordinatorOnly", coordinatorOnly)
                    .add("partitioningProperties", partitioningProperties)
                    .toString();
        }
    }

    @Immutable
    public static final class Partitioning
    {
        private final Set<Symbol> partitioningColumns;
        private final Optional<List<Symbol>> hashingOrder; // If populated, this list will contain the same symbols as partitioningColumns
        private final boolean replicateNulls;

        private Partitioning(Set<Symbol> partitioningColumns, Optional<List<Symbol>> hashingOrder, boolean replicateNulls)
        {
            this.partitioningColumns = ImmutableSet.copyOf(Objects.requireNonNull(partitioningColumns, "partitioningColumns is null"));
            this.hashingOrder = Objects.requireNonNull(hashingOrder, "hashingOrder is null").map(ImmutableList::copyOf);
            this.replicateNulls = replicateNulls;
            checkArgument(!replicateNulls || partitioningColumns.size() == 1, "replicateNulls can only be set for partitioning of exactly 1 column");
        }

        public static Partitioning hashPartitioned(List<Symbol> columns)
        {
            return new Partitioning(ImmutableSet.copyOf(columns), Optional.of(columns), false);
        }

        public static Partitioning hashPartitionedWithReplicatedNulls(List<Symbol> columns)
        {
            return new Partitioning(ImmutableSet.copyOf(columns), Optional.of(columns), true);
        }

        public static Partitioning partitioned(Set<Symbol> columns)
        {
            return new Partitioning(columns, Optional.<List<Symbol>>empty(), false);
        }

        public static Partitioning singlePartition()
        {
            return partitioned(ImmutableSet.of());
        }

        public boolean isReplicateNulls()
        {
            return replicateNulls;
        }

        public boolean isPartitionedOn(Collection<Symbol> columns, Set<Symbol> knownConstants)
        {
            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            return partitioningColumns.stream()
                    .filter(symbol -> !knownConstants.contains(symbol))
                    .allMatch(columns::contains);
        }

        public boolean isHashPartitionedOn(List<Symbol> columns)
        {
            return hashingOrder.isPresent() && hashingOrder.get().equals(columns);
        }

        public boolean isEffectivelySinglePartition(Set<Symbol> knownConstants)
        {
            return isPartitionedOn(ImmutableSet.of(), knownConstants);
        }

        public boolean isRepartitionEffective(Collection<Symbol> keys, Set<Symbol> knownConstants)
        {
            Set<Symbol> keysWithoutConstants = keys.stream()
                    .filter(symbol -> !knownConstants.contains(symbol))
                    .collect(toImmutableSet());
            return !partitioningColumns.stream()
                    .filter(symbol -> !knownConstants.contains(symbol))
                    .collect(toImmutableSet())
                    .equals(keysWithoutConstants);
        }

        public Optional<Partitioning> translate(Function<Symbol, Optional<Symbol>> translator)
        {
            ImmutableSet.Builder<Symbol> newPartitioningColumns = ImmutableSet.builder();
            for (Symbol partitioningColumn : partitioningColumns) {
                Optional<Symbol> translated = translator.apply(partitioningColumn);
                if (!translated.isPresent()) {
                    return Optional.empty();
                }
                newPartitioningColumns.add(translated.get());
            }

            // If all partitioningColumns can be translated, then shouldn't have any problems with hashingOrder
            Optional<List<Symbol>> newHashingOrder = hashingOrder.map(columns -> columns.stream()
                    .map(translator::apply)
                    .map(Optional::get)
                    .collect(toImmutableList()));

            return Optional.of(new Partitioning(newPartitioningColumns.build(), newHashingOrder, replicateNulls));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitioningColumns, hashingOrder);
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
            final Partitioning other = (Partitioning) obj;
            return Objects.equals(this.partitioningColumns, other.partitioningColumns)
                    && Objects.equals(this.hashingOrder, other.hashingOrder);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("partitioningColumns", partitioningColumns)
                    .add("hashingOrder", hashingOrder)
                    .add("replicateNulls", replicateNulls)
                    .toString();
        }
    }
}
