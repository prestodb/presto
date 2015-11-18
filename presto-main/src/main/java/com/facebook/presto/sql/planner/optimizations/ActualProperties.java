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
import com.facebook.presto.sql.planner.PartitioningHandle;
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

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
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

        // The constants field implies a ConstantProperty in localProperties (but not vice versa).
        // Let's make sure to include the constants into the local constant properties.
        Set<Symbol> localConstants = LocalProperties.extractLeadingConstants(localProperties);
        localProperties = LocalProperties.stripLeadingConstants(localProperties);

        Set<Symbol> updatedLocalConstants = ImmutableSet.<Symbol>builder()
                .addAll(localConstants)
                .addAll(constants.keySet())
                .build();

        List<LocalProperty<Symbol>> updatedLocalProperties = LocalProperties.normalizeAndPrune(ImmutableList.<LocalProperty<Symbol>>builder()
                .addAll(transform(updatedLocalConstants, ConstantProperty::new))
                .addAll(localProperties)
                .build());

        this.localProperties = ImmutableList.copyOf(updatedLocalProperties);
        this.constants = ImmutableMap.copyOf(constants);
    }

    public boolean isCoordinatorOnly()
    {
        return global.isCoordinatorOnly();
    }

    /**
     * @returns true if the plan will only execute on a single node
     */
    public boolean isSingleNode()
    {
        return global.isSingleNode();
    }

    public boolean isStreamPartitionedOn(Collection<Symbol> columns)
    {
        return global.isStreamPartitionedOn(columns, constants.keySet());
    }

    public boolean isNodePartitionedOn(Collection<Symbol> columns)
    {
        return global.isNodePartitionedOn(columns, constants.keySet());
    }

    public boolean isNodePartitionedOn(PartitioningHandle partitioning, List<Symbol> columns)
    {
        return global.isNodePartitionedOn(partitioning, columns);
    }

    /**
     * @return true if all the data will effectively land in a single stream
     */
    public boolean isEffectivelySingleStream()
    {
        return global.isEffectivelySingleStream(constants.keySet());
    }

    /**
     * @return true if repartitioning on the keys will yield some difference
     */
    public boolean isStreamRepartitionEffective(Collection<Symbol> keys)
    {
        return global.isStreamRepartitionEffective(keys, constants.keySet());
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

    public Optional<PartitioningHandle> getNodePartitioningHandle()
    {
        return global.getNodePartitioningHandle();
    }

    public Optional<List<Symbol>> getNodePartitioningColumns()
    {
        return global.getNodePartitioningColumns();
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
            this.global = Global.arbitraryPartition();
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

        public Builder constants(Map<Symbol, Object> constants)
        {
            this.constants = ImmutableMap.copyOf(constants);
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
        // Description of the partitioning of the data across nodes
        private final Optional<Partitioning> nodePartitioning; // if missing => partitioned with some unknown scheme
        // Description of the partitioning of the data across streams (splits)
        private final Optional<Partitioning> streamPartitioning; // if missing => partitioned with some unknown scheme

        // NOTE: Partitioning on zero columns (or effectively zero columns if the columns are constant) indicates that all
        // the rows will be partitioned into a single node or stream. However, this can still be a partitioned plan in that the plan
        // will be executed on multiple servers, but only one server will get all the data.

        private Global(Optional<Partitioning> nodePartitioning, Optional<Partitioning> streamPartitioning)
        {
            this.nodePartitioning = requireNonNull(nodePartitioning, "nodePartitioning is null");
            this.streamPartitioning = requireNonNull(streamPartitioning, "streamPartitioning is null");
        }

        public static Global coordinatorSingleStreamPartition()
        {
            return partitionedOn(
                    COORDINATOR_DISTRIBUTION,
                    ImmutableList.of(),
                    Optional.of(ImmutableList.of()));
        }

        public static Global singleStreamPartition()
        {
            return partitionedOn(
                    SINGLE_DISTRIBUTION,
                    ImmutableList.of(),
                    Optional.of(ImmutableList.of()));
        }

        public static Global arbitraryPartition()
        {
            return new Global(Optional.empty(), Optional.empty());
        }

        public static Global partitionedOn(PartitioningHandle nodePartitioningHandle, List<Symbol> nodePartitioning, Optional<List<Symbol>> streamPartitioning)
        {
            return new Global(
                    Optional.of(new Partitioning(nodePartitioningHandle, nodePartitioning)),
                    streamPartitioning.map(columns -> new Partitioning(SOURCE_DISTRIBUTION, columns)));
        }

        public static Global streamPartitionedOn(List<Symbol> streamPartitioning)
        {
            return new Global(
                    Optional.empty(),
                    Optional.of(new Partitioning(SOURCE_DISTRIBUTION, streamPartitioning)));
        }

        /**
         * @returns true if the plan will only execute on a single node
         */
        private boolean isSingleNode()
        {
            if (!nodePartitioning.isPresent()) {
                return false;
            }

            return nodePartitioning.get().getPartitioningHandle().isSingleNode();
        }

        private boolean isCoordinatorOnly()
        {
            if (!nodePartitioning.isPresent()) {
                return false;
            }

            return nodePartitioning.get().getPartitioningHandle().isCoordinatorOnly();
        }

        private boolean isNodePartitionedOn(Collection<Symbol> columns, Set<Symbol> constants)
        {
            return nodePartitioning.isPresent() && nodePartitioning.get().isPartitionedOn(columns, constants);
        }

        private boolean isNodePartitionedOn(PartitioningHandle partitioning, List<Symbol> columns)
        {
            return nodePartitioning.isPresent() && nodePartitioning.get().isPartitionedOn(partitioning, columns);
        }

        private Optional<PartitioningHandle> getNodePartitioningHandle()
        {
            return nodePartitioning.map(Partitioning::getPartitioningHandle);
        }

        private Optional<List<Symbol>> getNodePartitioningColumns()
        {
            return nodePartitioning.map(Partitioning::getPartitioningColumns);
        }

        private boolean isStreamPartitionedOn(Collection<Symbol> columns, Set<Symbol> constants)
        {
            return streamPartitioning.isPresent() && streamPartitioning.get().isPartitionedOn(columns, constants);
        }

        /**
         * @return true if all the data will effectively land in a single stream
         */
        private boolean isEffectivelySingleStream(Set<Symbol> constants)
        {
            return streamPartitioning.isPresent() && streamPartitioning.get().isEffectivelySinglePartition(constants);
        }

        /**
         * @return true if repartitioning on the keys will yield some difference
         */
        private boolean isStreamRepartitionEffective(Collection<Symbol> keys, Set<Symbol> constants)
        {
            return !streamPartitioning.isPresent() || streamPartitioning.get().isRepartitionEffective(keys, constants);
        }

        private Global translate(Function<Symbol, Optional<Symbol>> translator)
        {
            return new Global(
                    nodePartitioning.flatMap(partitioning -> partitioning.translate(translator)),
                    streamPartitioning.flatMap(partitioning -> partitioning.translate(translator)));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodePartitioning, streamPartitioning);
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
            return Objects.equals(this.nodePartitioning, other.nodePartitioning) &&
                    Objects.equals(this.streamPartitioning, other.streamPartitioning);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("nodePartitioning", nodePartitioning)
                    .add("streamPartitioning", streamPartitioning)
                    .toString();
        }
    }

    @Immutable
    public static final class Partitioning
    {
        private final PartitioningHandle partitioningHandle;
        private final List<Symbol> partitioningColumns;

        public Partitioning(PartitioningHandle partitioningHandle, List<Symbol> partitioningColumns)
        {
            this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
            this.partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
        }

        public PartitioningHandle getPartitioningHandle()
        {
            return partitioningHandle;
        }

        public List<Symbol> getPartitioningColumns()
        {
            return partitioningColumns;
        }

        public boolean isPartitionedOn(PartitioningHandle partitioning, List<Symbol> columns)
        {
            if (!partitioningHandle.equals(partitioning)) {
                return false;
            }

            return columns.equals(partitioningColumns);
        }

        public boolean isPartitionedOn(Collection<Symbol> columns, Set<Symbol> knownConstants)
        {
            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            return partitioningColumns.stream()
                    .filter(symbol -> !knownConstants.contains(symbol))
                    .allMatch(columns::contains);
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
            ImmutableList.Builder<Symbol> newPartitioningColumns = ImmutableList.builder();
            for (Symbol partitioningColumn : partitioningColumns) {
                Optional<Symbol> translated = translator.apply(partitioningColumn);
                if (!translated.isPresent()) {
                    // there is no symbol for this parameter so we can't
                    // say anything about this partitioning
                    return Optional.empty();
                }
                newPartitioningColumns.add(translated.get());
            }

            return Optional.of(new Partitioning(partitioningHandle, newPartitioningColumns.build()));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitioningHandle, partitioningColumns);
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
            return Objects.equals(this.partitioningHandle, other.partitioningHandle) &&
                    Objects.equals(this.partitioningColumns, other.partitioningColumns);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("partitioningHandle", partitioningHandle)
                    .add("partitioningColumns", partitioningColumns)
                    .toString();
        }
    }
}
