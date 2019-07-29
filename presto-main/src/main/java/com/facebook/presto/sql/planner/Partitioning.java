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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

@Immutable
public final class Partitioning
{
    private final PartitioningHandle handle;
    private final List<ArgumentBinding> arguments;

    private Partitioning(PartitioningHandle handle, List<ArgumentBinding> arguments)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    public static Partitioning create(PartitioningHandle handle, List<VariableReferenceExpression> columns)
    {
        return new Partitioning(handle, columns.stream()
                .map(ArgumentBinding::new)
                .collect(toImmutableList()));
    }

    // Factory method for JSON serde only!
    @JsonCreator
    public static Partitioning jsonCreate(
            @JsonProperty("handle") PartitioningHandle handle,
            @JsonProperty("arguments") List<ArgumentBinding> arguments)
    {
        return new Partitioning(handle, arguments);
    }

    @JsonProperty
    public PartitioningHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    public List<ArgumentBinding> getArguments()
    {
        return arguments;
    }

    public Set<VariableReferenceExpression> getVariableReferences()
    {
        return arguments.stream()
                .filter(ArgumentBinding::isVariable)
                .map(ArgumentBinding::getVariableReference)
                .collect(toImmutableSet());
    }

    @Deprecated
    public boolean isCompatibleWith(
            Partitioning right,
            Metadata metadata,
            Session session)
    {
        if (!handle.equals(right.handle) && !metadata.getCommonPartitioning(session, handle, right.handle).isPresent()) {
            return false;
        }

        return arguments.equals(right.arguments);
    }

    @Deprecated
    public boolean isCompatibleWith(
            Partitioning right,
            Function<VariableReferenceExpression, Set<VariableReferenceExpression>> leftToRightMappings,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> leftConstantMapping,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> rightConstantMapping,
            Metadata metadata,
            Session session)
    {
        if (!handle.equals(right.handle) && !metadata.getCommonPartitioning(session, handle, right.handle).isPresent()) {
            return false;
        }

        if (arguments.size() != right.arguments.size()) {
            return false;
        }

        for (int i = 0; i < arguments.size(); i++) {
            ArgumentBinding leftArgument = arguments.get(i);
            ArgumentBinding rightArgument = right.arguments.get(i);

            if (!isPartitionedWith(leftArgument, leftConstantMapping, rightArgument, rightConstantMapping, leftToRightMappings)) {
                return false;
            }
        }
        return true;
    }

    //  Refined-over relation is reflexive.
    public boolean isRefinedPartitioningOver(
            Partitioning right,
            Metadata metadata,
            Session session)
    {
        if (!handle.equals(right.handle) && !metadata.isRefinedPartitioningOver(session, handle, right.handle)) {
            return false;
        }

        return arguments.equals(right.arguments);
    }

    //  Refined-over relation is reflexive.
    public boolean isRefinedPartitioningOver(
            Partitioning right,
            Function<VariableReferenceExpression, Set<VariableReferenceExpression>> leftToRightMappings,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> leftConstantMapping,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> rightConstantMapping,
            Metadata metadata,
            Session session)
    {
        if (!metadata.isRefinedPartitioningOver(session, handle, right.handle)) {
            return false;
        }
        if (arguments.size() != right.arguments.size()) {
            return false;
        }

        for (int i = 0; i < arguments.size(); i++) {
            ArgumentBinding leftArgument = arguments.get(i);
            ArgumentBinding rightArgument = right.arguments.get(i);

            if (!isPartitionedWith(leftArgument, leftConstantMapping, rightArgument, rightConstantMapping, leftToRightMappings)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPartitionedWith(
            ArgumentBinding leftArgument,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> leftConstantMapping,
            ArgumentBinding rightArgument,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> rightConstantMapping,
            Function<VariableReferenceExpression, Set<VariableReferenceExpression>> leftToRightMappings)
    {
        if (leftArgument.isVariable()) {
            if (rightArgument.isVariable()) {
                // variable == variable
                Set<VariableReferenceExpression> mappedColumns = leftToRightMappings.apply(leftArgument.getVariableReference());
                return mappedColumns.contains(rightArgument.getVariableReference());
            }
            else {
                // variable == constant
                // Normally, this would be a false condition, but if we happen to have an external
                // mapping from the variable to a constant value and that constant value matches the
                // right value, then we are co-partitioned.
                Optional<ConstantExpression> leftConstant = leftConstantMapping.apply(leftArgument.getVariableReference());
                return leftConstant.isPresent() && leftConstant.get().equals(rightArgument.getConstant());
            }
        }
        else {
            if (rightArgument.isConstant()) {
                // constant == constant
                return leftArgument.getConstant().equals(rightArgument.getConstant());
            }
            else {
                // constant == variable
                Optional<ConstantExpression> rightConstant = rightConstantMapping.apply(rightArgument.getVariableReference());
                return rightConstant.isPresent() && rightConstant.get().equals(leftArgument.getConstant());
            }
        }
    }

    public boolean isPartitionedOn(Collection<VariableReferenceExpression> columns, Set<VariableReferenceExpression> knownConstants)
    {
        // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
        // can safely ignore all constant columns when comparing partition properties
        return arguments.stream()
                .filter(ArgumentBinding::isVariable)
                .map(ArgumentBinding::getVariableReference)
                .filter(variable -> !knownConstants.contains(variable))
                .allMatch(columns::contains);
    }

    public boolean isEffectivelySinglePartition(Set<VariableReferenceExpression> knownConstants)
    {
        return isPartitionedOn(ImmutableSet.of(), knownConstants);
    }

    public boolean isRepartitionEffective(Collection<VariableReferenceExpression> keys, Set<VariableReferenceExpression> knownConstants)
    {
        Set<VariableReferenceExpression> keysWithoutConstants = keys.stream()
                .filter(variable -> !knownConstants.contains(variable))
                .collect(toImmutableSet());
        Set<VariableReferenceExpression> nonConstantArgs = arguments.stream()
                .filter(ArgumentBinding::isVariable)
                .map(ArgumentBinding::getVariableReference)
                .filter(variable -> !knownConstants.contains(variable))
                .collect(toImmutableSet());
        return !nonConstantArgs.equals(keysWithoutConstants);
    }

    public Partitioning translate(Function<VariableReferenceExpression, VariableReferenceExpression> translator)
    {
        return new Partitioning(handle, arguments.stream()
                .map(argument -> argument.translate(translator))
                .collect(toImmutableList()));
    }

    public Optional<Partitioning> translate(Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> translator, Function<VariableReferenceExpression, Optional<ConstantExpression>> constants)
    {
        ImmutableList.Builder<ArgumentBinding> newArguments = ImmutableList.builder();
        for (ArgumentBinding argument : arguments) {
            Optional<ArgumentBinding> newArgument = argument.translate(translator, constants);
            if (!newArgument.isPresent()) {
                return Optional.empty();
            }
            newArguments.add(newArgument.get());
        }

        return Optional.of(new Partitioning(handle, newArguments.build()));
    }

    public Partitioning withAlternativePartitiongingHandle(PartitioningHandle partitiongingHandle)
    {
        return new Partitioning(partitiongingHandle, this.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(handle, arguments);
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
        return Objects.equals(this.handle, other.handle) &&
                Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("handle", handle)
                .add("arguments", arguments)
                .toString();
    }

    @Immutable
    public static final class ArgumentBinding
    {
        private final RowExpression rowExpression;

        @JsonCreator
        public ArgumentBinding(@JsonProperty("rowExpression") RowExpression rowExpression)
        {
            checkArgument(rowExpression instanceof VariableReferenceExpression || rowExpression instanceof ConstantExpression, "Expect either VariableReferenceExpression or ConstantExpression");
            this.rowExpression = requireNonNull(rowExpression, "rowExpression is null");
        }

        @JsonProperty
        public RowExpression getRowExpression()
        {
            return rowExpression;
        }

        public boolean isConstant()
        {
            return rowExpression instanceof ConstantExpression;
        }

        public boolean isVariable()
        {
            return rowExpression instanceof VariableReferenceExpression;
        }

        public VariableReferenceExpression getVariableReference()
        {
            verify(rowExpression instanceof VariableReferenceExpression, "Expect the rowExpression to be a VariableReferenceExpression");
            return (VariableReferenceExpression) rowExpression;
        }

        public ConstantExpression getConstant()
        {
            verify(rowExpression instanceof ConstantExpression, "Expect the rowExpression to be a ConstantExpression");
            return (ConstantExpression) rowExpression;
        }

        public ArgumentBinding translate(Function<VariableReferenceExpression, VariableReferenceExpression> translator)
        {
            if (isConstant()) {
                return this;
            }
            return new ArgumentBinding(translator.apply((VariableReferenceExpression) rowExpression));
        }

        public Optional<ArgumentBinding> translate(Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> translator, Function<VariableReferenceExpression, Optional<ConstantExpression>> constants)
        {
            if (isConstant()) {
                return Optional.of(this);
            }

            Optional<ArgumentBinding> newColumn = translator.apply((VariableReferenceExpression) rowExpression)
                    .map(ArgumentBinding::new);
            if (newColumn.isPresent()) {
                return newColumn;
            }

            // As a last resort, check for a constant mapping for the variable
            // Note: this MUST be last because we want to favor the variable representation
            // as it makes further optimizations possible.
            return constants.apply((VariableReferenceExpression) rowExpression)
                    .map(ArgumentBinding::new);
        }

        @Override
        public String toString()
        {
            if (rowExpression instanceof ConstantExpression) {
                return rowExpression.toString();
            }
            return "\"" + rowExpression.toString() + "\"";
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ArgumentBinding that = (ArgumentBinding) o;
            return Objects.equals(rowExpression, that.rowExpression);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rowExpression);
        }
    }
}
