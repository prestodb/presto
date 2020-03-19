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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.SymbolReference;
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

import static com.facebook.presto.sql.planner.PlannerUtils.toVariableReference;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.facebook.presto.util.MoreLists.filteredCopy;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

public class ActualProperties
{
    private final Global global;
    private final List<LocalProperty<VariableReferenceExpression>> localProperties;
    private final Map<VariableReferenceExpression, ConstantExpression> constants;

    private ActualProperties(
            Global global,
            List<? extends LocalProperty<VariableReferenceExpression>> localProperties,
            Map<VariableReferenceExpression, ConstantExpression> constants)
    {
        requireNonNull(global, "globalProperties is null");
        requireNonNull(localProperties, "localProperties is null");
        requireNonNull(constants, "constants is null");

        this.global = global;

        // The constants field implies a ConstantProperty in localProperties (but not vice versa).
        // Let's make sure to include the constants into the local constant properties.
        Set<VariableReferenceExpression> localConstants = LocalProperties.extractLeadingConstants(localProperties);
        localProperties = LocalProperties.stripLeadingConstants(localProperties);

        Set<VariableReferenceExpression> updatedLocalConstants = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(localConstants)
                .addAll(constants.keySet())
                .build();

        List<LocalProperty<VariableReferenceExpression>> updatedLocalProperties = LocalProperties.normalizeAndPrune(ImmutableList.<LocalProperty<VariableReferenceExpression>>builder()
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
     * @return true if the plan will only execute on a single node
     */
    public boolean isSingleNode()
    {
        return global.isSingleNode();
    }

    public boolean isNullsAndAnyReplicated()
    {
        return global.isNullsAndAnyReplicated();
    }

    public boolean isStreamPartitionedOn(Collection<VariableReferenceExpression> columns, boolean exactly)
    {
        return isStreamPartitionedOn(columns, false, exactly);
    }

    public boolean isStreamPartitionedOn(Collection<VariableReferenceExpression> columns, boolean nullsAndAnyReplicated, boolean exactly)
    {
        if (exactly) {
            return global.isStreamPartitionedOnExactly(columns, constants.keySet(), nullsAndAnyReplicated);
        }
        else {
            return global.isStreamPartitionedOn(columns, constants.keySet(), nullsAndAnyReplicated);
        }
    }

    public boolean isNodePartitionedOn(Collection<VariableReferenceExpression> columns, boolean exactly)
    {
        return isNodePartitionedOn(columns, false, exactly);
    }

    public boolean isNodePartitionedOn(Collection<VariableReferenceExpression> columns, boolean nullsAndAnyReplicated, boolean exactly)
    {
        if (exactly) {
            return global.isNodePartitionedOnExactly(columns, constants.keySet(), nullsAndAnyReplicated);
        }
        else {
            return global.isNodePartitionedOn(columns, constants.keySet(), nullsAndAnyReplicated);
        }
    }

    @Deprecated
    public boolean isCompatibleTablePartitioningWith(Partitioning partitioning, boolean nullsAndAnyReplicated, Metadata metadata, Session session)
    {
        return global.isCompatibleTablePartitioningWith(partitioning, nullsAndAnyReplicated, metadata, session);
    }

    @Deprecated
    public boolean isCompatibleTablePartitioningWith(ActualProperties other, Function<VariableReferenceExpression, Set<VariableReferenceExpression>> symbolMappings, Metadata metadata, Session session)
    {
        return global.isCompatibleTablePartitioningWith(
                other.global,
                symbolMappings,
                variable -> Optional.ofNullable(constants.get(variable)),
                variable -> Optional.ofNullable(other.constants.get(variable)),
                metadata,
                session);
    }

    public boolean isRefinedPartitioningOver(Partitioning partitioning, boolean nullsAndAnyReplicated, Metadata metadata, Session session)
    {
        return global.isRefinedPartitioningOver(partitioning, nullsAndAnyReplicated, metadata, session);
    }

    public boolean isRefinedPartitioningOver(ActualProperties other, Function<VariableReferenceExpression, Set<VariableReferenceExpression>> symbolMappings, Metadata metadata, Session session)
    {
        return global.isRefinedPartitioningOver(
                other.global,
                symbolMappings,
                variable -> Optional.ofNullable(constants.get(variable)),
                variable -> Optional.ofNullable(other.constants.get(variable)),
                metadata,
                session);
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
    public boolean isStreamRepartitionEffective(Collection<VariableReferenceExpression> keys)
    {
        return global.isStreamRepartitionEffective(keys, constants.keySet());
    }

    public ActualProperties translateVariable(Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> translator)
    {
        Map<VariableReferenceExpression, ConstantExpression> translatedConstants = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, ConstantExpression> entry : constants.entrySet()) {
            Optional<VariableReferenceExpression> translatedKey = translator.apply(entry.getKey());
            if (translatedKey.isPresent()) {
                translatedConstants.put(translatedKey.get(), entry.getValue());
            }
        }
        return builder()
                .global(global.translateVariableToRowExpression(variable -> {
                    Optional<RowExpression> translated = translator.apply(variable).map(RowExpression.class::cast);
                    if (!translated.isPresent()) {
                        translated = Optional.ofNullable(constants.get(variable));
                    }
                    return translated;
                }))
                .local(LocalProperties.translate(localProperties, translator))
                .constants(translatedConstants)
                .build();
    }

    public ActualProperties translateRowExpression(Map<VariableReferenceExpression, RowExpression> assignments, TypeProvider types)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> inputToOutputVariables = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : assignments.entrySet()) {
            RowExpression expression = assignment.getValue();
            if (isExpression(expression)) {
                if (castToExpression(expression) instanceof SymbolReference) {
                    inputToOutputVariables.put(toVariableReference(castToExpression(expression), types), assignment.getKey());
                }
            }
            else {
                if (expression instanceof VariableReferenceExpression) {
                    inputToOutputVariables.put((VariableReferenceExpression) expression, assignment.getKey());
                }
            }
        }

        Map<VariableReferenceExpression, ConstantExpression> translatedConstants = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, ConstantExpression> entry : constants.entrySet()) {
            if (inputToOutputVariables.containsKey(entry.getKey())) {
                translatedConstants.put(inputToOutputVariables.get(entry.getKey()), entry.getValue());
            }
        }

        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> inputToOutputMappings = ImmutableMap.builder();
        inputToOutputMappings.putAll(inputToOutputVariables);
        constants.entrySet().stream()
                .filter(entry -> !inputToOutputVariables.containsKey(entry.getKey()))
                .forEach(inputToOutputMappings::put);
        return builder()
                .global(global.translateRowExpression(inputToOutputMappings.build(), assignments, types))
                .local(LocalProperties.translate(localProperties, variable -> Optional.ofNullable(inputToOutputVariables.get(variable))))
                .constants(translatedConstants)
                .build();
    }

    public Optional<Partitioning> getNodePartitioning()
    {
        return global.getNodePartitioning();
    }

    public Map<VariableReferenceExpression, ConstantExpression> getConstants()
    {
        return constants;
    }

    public List<LocalProperty<VariableReferenceExpression>> getLocalProperties()
    {
        return localProperties;
    }

    public ActualProperties withReplicatedNulls(boolean replicatedNulls)
    {
        return builderFrom(this)
                .global(global.withReplicatedNulls(replicatedNulls))
                .build();
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
        private List<LocalProperty<VariableReferenceExpression>> localProperties;
        private Map<VariableReferenceExpression, ConstantExpression> constants;
        private boolean unordered;

        public Builder()
        {
            this(Global.arbitraryPartition(), ImmutableList.of(), ImmutableMap.of());
        }

        public Builder(Global global, List<LocalProperty<VariableReferenceExpression>> localProperties, Map<VariableReferenceExpression, ConstantExpression> constants)
        {
            this.global = requireNonNull(global, "global is null");
            this.localProperties = ImmutableList.copyOf(localProperties);
            this.constants = ImmutableMap.copyOf(constants);
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

        public Builder local(List<? extends LocalProperty<VariableReferenceExpression>> localProperties)
        {
            this.localProperties = ImmutableList.copyOf(localProperties);
            return this;
        }

        public Builder constants(Map<VariableReferenceExpression, ConstantExpression> constants)
        {
            this.constants = ImmutableMap.copyOf(constants);
            return this;
        }

        public Builder unordered(boolean unordered)
        {
            this.unordered = unordered;
            return this;
        }

        public ActualProperties build()
        {
            List<LocalProperty<VariableReferenceExpression>> localProperties = this.localProperties;
            if (unordered) {
                localProperties = filteredCopy(this.localProperties, property -> !property.isOrderSensitive());
            }
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
        return toStringHelper(this)
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

        // Description of whether rows with nulls in partitioning columns or some arbitrary rows have been replicated to all *nodes*
        // When doing an IN query NULL in empty set is false, NULL in non-empty set is NULL. Say non-NULL element A (number 1) in
        // a set that is missing A ( say 2, 3) is false, but A in (2, 3, NULL) is NULL.
        // IN is equivalent to "a = b OR a = c OR a = d...).
        private final boolean nullsAndAnyReplicated;

        private Global(Optional<Partitioning> nodePartitioning, Optional<Partitioning> streamPartitioning, boolean nullsAndAnyReplicated)
        {
            checkArgument(!nodePartitioning.isPresent()
                            || !streamPartitioning.isPresent()
                            || nodePartitioning.get().getVariableReferences().containsAll(streamPartitioning.get().getVariableReferences())
                            || streamPartitioning.get().getVariableReferences().containsAll(nodePartitioning.get().getVariableReferences()),
                    "Global stream partitioning columns should match node partitioning columns");
            this.nodePartitioning = requireNonNull(nodePartitioning, "nodePartitioning is null");
            this.streamPartitioning = requireNonNull(streamPartitioning, "streamPartitioning is null");
            this.nullsAndAnyReplicated = nullsAndAnyReplicated;
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
            return new Global(Optional.empty(), Optional.empty(), false);
        }

        public static <T extends RowExpression, U extends RowExpression> Global partitionedOn(
                PartitioningHandle nodePartitioningHandle,
                List<T> nodePartitioning,
                Optional<List<U>> streamPartitioning)
        {
            return new Global(
                    Optional.of(Partitioning.create(nodePartitioningHandle, nodePartitioning)),
                    streamPartitioning.map(columns -> Partitioning.create(SOURCE_DISTRIBUTION, columns)),
                    false);
        }

        public static Global partitionedOn(Partitioning nodePartitioning, Optional<Partitioning> streamPartitioning)
        {
            return new Global(
                    Optional.of(nodePartitioning),
                    streamPartitioning,
                    false);
        }

        public static Global streamPartitionedOn(List<VariableReferenceExpression> streamPartitioning)
        {
            return new Global(
                    Optional.empty(),
                    Optional.of(Partitioning.create(SOURCE_DISTRIBUTION, streamPartitioning)),
                    false);
        }

        public static Global partitionedOnCoalesce(Partitioning one, Partitioning other, Metadata metadata, Session session)
        {
            return new Global(one.translateToCoalesce(other, metadata, session), Optional.empty(), false);
        }

        public Global withReplicatedNulls(boolean replicatedNulls)
        {
            return new Global(nodePartitioning, streamPartitioning, replicatedNulls);
        }

        private boolean isNullsAndAnyReplicated()
        {
            return nullsAndAnyReplicated;
        }

        /**
         * @return true if the plan will only execute on a single node
         */
        private boolean isSingleNode()
        {
            if (!nodePartitioning.isPresent()) {
                return false;
            }

            return nodePartitioning.get().getHandle().isSingleNode();
        }

        private boolean isCoordinatorOnly()
        {
            if (!nodePartitioning.isPresent()) {
                return false;
            }

            return nodePartitioning.get().getHandle().isCoordinatorOnly();
        }

        private boolean isNodePartitionedOn(Collection<VariableReferenceExpression> columns, Set<VariableReferenceExpression> constants, boolean nullsAndAnyReplicated)
        {
            return nodePartitioning.isPresent() && nodePartitioning.get().isPartitionedOn(columns, constants) && this.nullsAndAnyReplicated == nullsAndAnyReplicated;
        }

        private boolean isNodePartitionedOnExactly(Collection<VariableReferenceExpression> columns, Set<VariableReferenceExpression> constants, boolean nullsAndAnyReplicated)
        {
            return nodePartitioning.isPresent() && nodePartitioning.get().isPartitionedOnExactly(columns, constants) && this.nullsAndAnyReplicated == nullsAndAnyReplicated;
        }

        private boolean isCompatibleTablePartitioningWith(Partitioning partitioning, boolean nullsAndAnyReplicated, Metadata metadata, Session session)
        {
            return nodePartitioning.isPresent() && nodePartitioning.get().isCompatibleWith(partitioning, metadata, session) && this.nullsAndAnyReplicated == nullsAndAnyReplicated;
        }

        private boolean isCompatibleTablePartitioningWith(
                Global other,
                Function<VariableReferenceExpression, Set<VariableReferenceExpression>> symbolMappings,
                Function<VariableReferenceExpression, Optional<ConstantExpression>> leftConstantMapping,
                Function<VariableReferenceExpression, Optional<ConstantExpression>> rightConstantMapping,
                Metadata metadata,
                Session session)
        {
            return nodePartitioning.isPresent() &&
                    other.nodePartitioning.isPresent() &&
                    nodePartitioning.get().isCompatibleWith(
                            other.nodePartitioning.get(),
                            symbolMappings,
                            leftConstantMapping,
                            rightConstantMapping,
                            metadata,
                            session) &&
                    nullsAndAnyReplicated == other.nullsAndAnyReplicated;
        }

        private boolean isRefinedPartitioningOver(Partitioning partitioning, boolean nullsAndAnyReplicated, Metadata metadata, Session session)
        {
            return nodePartitioning.isPresent() && nodePartitioning.get().isRefinedPartitioningOver(partitioning, metadata, session) && this.nullsAndAnyReplicated == nullsAndAnyReplicated;
        }

        private boolean isRefinedPartitioningOver(
                Global other,
                Function<VariableReferenceExpression, Set<VariableReferenceExpression>> symbolMappings,
                Function<VariableReferenceExpression, Optional<ConstantExpression>> leftConstantMapping,
                Function<VariableReferenceExpression, Optional<ConstantExpression>> rightConstantMapping,
                Metadata metadata,
                Session session)
        {
            return nodePartitioning.isPresent() &&
                    other.nodePartitioning.isPresent() &&
                    nodePartitioning.get().isRefinedPartitioningOver(
                            other.nodePartitioning.get(),
                            symbolMappings,
                            leftConstantMapping,
                            rightConstantMapping,
                            metadata,
                            session) &&
                    nullsAndAnyReplicated == other.nullsAndAnyReplicated;
        }

        private Optional<Partitioning> getNodePartitioning()
        {
            return nodePartitioning;
        }

        private boolean isStreamPartitionedOn(Collection<VariableReferenceExpression> columns, Set<VariableReferenceExpression> constants, boolean nullsAndAnyReplicated)
        {
            return streamPartitioning.isPresent() && streamPartitioning.get().isPartitionedOn(columns, constants) && this.nullsAndAnyReplicated == nullsAndAnyReplicated;
        }

        private boolean isStreamPartitionedOnExactly(Collection<VariableReferenceExpression> columns, Set<VariableReferenceExpression> constants, boolean nullsAndAnyReplicated)
        {
            return streamPartitioning.isPresent() && streamPartitioning.get().isPartitionedOnExactly(columns, constants) && this.nullsAndAnyReplicated == nullsAndAnyReplicated;
        }

        /**
         * @return true if all the data will effectively land in a single stream
         */
        private boolean isEffectivelySingleStream(Set<VariableReferenceExpression> constants)
        {
            return streamPartitioning.isPresent() && streamPartitioning.get().isEffectivelySinglePartition(constants) && !nullsAndAnyReplicated;
        }

        /**
         * @return true if repartitioning on the keys will yield some difference
         */
        private boolean isStreamRepartitionEffective(Collection<VariableReferenceExpression> keys, Set<VariableReferenceExpression> constants)
        {
            return (!streamPartitioning.isPresent() || streamPartitioning.get().isRepartitionEffective(keys, constants)) && !nullsAndAnyReplicated;
        }

        private Global translateVariableToRowExpression(
                Function<VariableReferenceExpression, Optional<RowExpression>> translator)
        {
            return new Global(
                    nodePartitioning.flatMap(partitioning -> partitioning.translateVariableToRowExpression(translator)),
                    streamPartitioning.flatMap(partitioning -> partitioning.translateVariableToRowExpression(translator)),
                    nullsAndAnyReplicated);
        }

        private Global translateRowExpression(Map<VariableReferenceExpression, RowExpression> inputToOutputMappings, Map<VariableReferenceExpression, RowExpression> assignments, TypeProvider types)
        {
            return new Global(
                    nodePartitioning.flatMap(partitioning -> partitioning.translateRowExpression(inputToOutputMappings, assignments, types)),
                    streamPartitioning.flatMap(partitioning -> partitioning.translateRowExpression(inputToOutputMappings, assignments, types)),
                    nullsAndAnyReplicated);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodePartitioning, streamPartitioning, nullsAndAnyReplicated);
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
                    Objects.equals(this.streamPartitioning, other.streamPartitioning) &&
                    this.nullsAndAnyReplicated == other.nullsAndAnyReplicated;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("nodePartitioning", nodePartitioning)
                    .add("streamPartitioning", streamPartitioning)
                    .add("nullsAndAnyReplicated", nullsAndAnyReplicated)
                    .toString();
        }
    }
}
