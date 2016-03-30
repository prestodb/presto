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
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.sql.planner.PartitionFunctionBinding.PartitionFunctionArgumentBinding;
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
    private final Map<Symbol, NullableValue> constants;

    private ActualProperties(
            Global global,
            List<? extends LocalProperty<Symbol>> localProperties,
            Map<Symbol, NullableValue> constants)
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

    public boolean isNodePartitionedWith(ActualProperties other, Function<Symbol, Set<Symbol>> symbolMappings)
    {
        return global.isNodePartitionedWith(
                other.global,
                symbolMappings,
                constants::get,
                other.constants::get);
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
        Map<Symbol, NullableValue> translatedConstants = new HashMap<>();
        for (Map.Entry<Symbol, NullableValue> entry : constants.entrySet()) {
            Optional<Symbol> translatedKey = translator.apply(entry.getKey());
            if (translatedKey.isPresent()) {
                translatedConstants.put(translatedKey.get(), entry.getValue());
            }
        }
        return builder()
                .global(global.translate(translator, symbol -> Optional.ofNullable(constants.get(symbol))))
                .local(LocalProperties.translate(localProperties, translator))
                .constants(translatedConstants)
                .build();
    }

    public Optional<PartitioningHandle> getNodePartitioningHandle()
    {
        return global.getNodePartitioningHandle();
    }

    public Optional<List<PartitionFunctionArgumentBinding>> getNodePartitioningColumns()
    {
        return global.getNodePartitioningColumns();
    }

    public Map<Symbol, NullableValue> getConstants()
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
        private Map<Symbol, NullableValue> constants;

        public Builder(Global global, List<LocalProperty<Symbol>> localProperties, Map<Symbol, NullableValue> constants)
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

        public Builder constants(Map<Symbol, NullableValue> constants)
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

        public static Global partitionedOn(PartitioningHandle nodePartitioningHandle, List<PartitionFunctionArgumentBinding> nodePartitioning, Optional<List<PartitionFunctionArgumentBinding>> streamPartitioning)
        {
            return new Global(
                    Optional.of(new Partitioning(nodePartitioningHandle, nodePartitioning)),
                    streamPartitioning.map(columns -> new Partitioning(SOURCE_DISTRIBUTION, columns)));
        }

        public static Global streamPartitionedOn(List<PartitionFunctionArgumentBinding> streamPartitioning)
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

        private boolean isNodePartitionedWith(
                Global other,
                Function<Symbol, Set<Symbol>> symbolMappings,
                Function<Symbol, NullableValue> leftConstantMapping,
                Function<Symbol, NullableValue> rightConstantMapping)
        {
            return nodePartitioning.isPresent() &&
                    other.nodePartitioning.isPresent() &&
                    nodePartitioning.get().isPartitionedWith(
                            other.nodePartitioning.get(),
                            symbolMappings,
                            leftConstantMapping,
                            rightConstantMapping);
        }

        private Optional<PartitioningHandle> getNodePartitioningHandle()
        {
            return nodePartitioning.map(Partitioning::getPartitioningHandle);
        }

        private Optional<List<PartitionFunctionArgumentBinding>> getNodePartitioningColumns()
        {
            return nodePartitioning.map(Partitioning::getPartitioningArguments);
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

        private Global translate(Function<Symbol, Optional<Symbol>> translator, Function<Symbol, Optional<NullableValue>> constants)
        {
            return new Global(
                    nodePartitioning.flatMap(partitioning -> partitioning.translate(translator, constants)),
                    streamPartitioning.flatMap(partitioning -> partitioning.translate(translator, constants)));
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
        private final List<PartitionFunctionArgumentBinding> partitioningArguments;

        public Partitioning(PartitioningHandle partitioningHandle, List<PartitionFunctionArgumentBinding> partitioningArguments)
        {
            this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
            this.partitioningArguments = ImmutableList.copyOf(requireNonNull(partitioningArguments, "partitioningArguments is null"));
        }

        public PartitioningHandle getPartitioningHandle()
        {
            return partitioningHandle;
        }

        public List<PartitionFunctionArgumentBinding> getPartitioningArguments()
        {
            return partitioningArguments;
        }

        public boolean isPartitionedOn(PartitioningHandle partitioning, List<Symbol> columns)
        {
            if (!partitioningHandle.equals(partitioning)) {
                return false;
            }

            if (partitioningArguments.size() != columns.size()) {
                return false;
            }

            for (int i = 0; i < partitioningArguments.size(); i++) {
                PartitionFunctionArgumentBinding argument = partitioningArguments.get(i);
                if (!argument.getColumn().equals(columns.get(i))) {
                    return false;
                }
            }
            return true;
        }

        public boolean isPartitionedWith(Partitioning right,
                Function<Symbol, Set<Symbol>> leftToRightMappings,
                Function<Symbol, NullableValue> leftConstantMapping,
                Function<Symbol, NullableValue> rightConstantMapping)
        {
            if (!partitioningHandle.equals(right.partitioningHandle)) {
                return false;
            }

            if (partitioningArguments.size() != right.partitioningArguments.size()) {
                return false;
            }

            for (int i = 0; i < partitioningArguments.size(); i++) {
                PartitionFunctionArgumentBinding leftArgument = partitioningArguments.get(i);
                PartitionFunctionArgumentBinding rightArgument = right.partitioningArguments.get(i);

                if (!isPartitionedWith(leftArgument, leftConstantMapping, rightArgument, rightConstantMapping, leftToRightMappings)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean isPartitionedWith(
                PartitionFunctionArgumentBinding leftArgument,
                Function<Symbol, NullableValue> leftConstantMapping,
                PartitionFunctionArgumentBinding rightArgument,
                Function<Symbol, NullableValue> rightConstantMapping,
                Function<Symbol, Set<Symbol>> leftToRightMappings)
        {
            if (leftArgument.isVariable()) {
                if (rightArgument.isVariable()) {
                    // variable == variable
                    Set<Symbol> mappedColumns = leftToRightMappings.apply(leftArgument.getColumn());
                    return mappedColumns.contains(rightArgument.getColumn());
                }
                else {
                    // variable == constant
                    // Normally, this would be a false condition, but if we happen to have an external
                    // mapping from the symbol to a constant value and that constant value matches the
                    // right value, then we are co-partitioned.
                    NullableValue leftConstant = leftConstantMapping.apply(leftArgument.getColumn());
                    return leftConstant != null && leftConstant.equals(rightArgument.getConstant());
                }
            }
            else {
                if (rightArgument.isConstant()) {
                    // constant == constant
                    return leftArgument.getConstant().equals(rightArgument.getConstant());
                }
                else {
                    // constant == variable
                    NullableValue rightConstant = rightConstantMapping.apply(rightArgument.getColumn());
                    return leftArgument.getConstant().equals(rightConstant);
                }
            }
        }

        public boolean isPartitionedOn(Collection<Symbol> columns, Set<Symbol> knownConstants)
        {
            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            return partitioningArguments.stream()
                    .filter(PartitionFunctionArgumentBinding::isVariable)
                    .map(PartitionFunctionArgumentBinding::getColumn)
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
            Set<Symbol> nonConstantArgs = partitioningArguments.stream()
                    .filter(PartitionFunctionArgumentBinding::isVariable)
                    .map(PartitionFunctionArgumentBinding::getColumn)
                    .filter(symbol -> !knownConstants.contains(symbol))
                    .collect(toImmutableSet());
            return !nonConstantArgs.equals(keysWithoutConstants);
        }

        public Optional<Partitioning> translate(Function<Symbol, Optional<Symbol>> translator, Function<Symbol, Optional<NullableValue>> constants)
        {
            ImmutableList.Builder<PartitionFunctionArgumentBinding> newArguments = ImmutableList.builder();
            for (PartitionFunctionArgumentBinding argument : partitioningArguments) {
                Optional<PartitionFunctionArgumentBinding> newArgument = translate(argument, translator, constants);
                if (!newArgument.isPresent()) {
                    return Optional.empty();
                }
                newArguments.add(newArgument.get());
            }

            return Optional.of(new Partitioning(partitioningHandle, newArguments.build()));
        }

        private static Optional<PartitionFunctionArgumentBinding> translate(
                PartitionFunctionArgumentBinding argument,
                Function<Symbol, Optional<Symbol>> translator,
                Function<Symbol, Optional<NullableValue>> constants)
        {
            // pass through constant arguments
            if (argument.isConstant()) {
                return Optional.of(argument);
            }

            // attempt to translate the symbol to a new symbol
            Optional<Symbol> newSymbol = translator.apply(argument.getColumn());
            if (newSymbol.isPresent()) {
                return Optional.of(new PartitionFunctionArgumentBinding(newSymbol.get()));
            }

            // As a last resort, check for a constant mapping for the symbol
            // Note: this MUST be last because we want to favor the symbol representation
            // as it makes further optimizations possible.
            Optional<NullableValue> constant = constants.apply(argument.getColumn());
            if (constant.isPresent()) {
                return Optional.of(new PartitionFunctionArgumentBinding(argument.getColumn(), constant.get()));
            }

            return Optional.empty();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitioningHandle, partitioningArguments);
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
                    Objects.equals(this.partitioningArguments, other.partitioningArguments);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("partitioningHandle", partitioningHandle)
                    .add("partitioningColumns", partitioningArguments)
                    .toString();
        }
    }
}
