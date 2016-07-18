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

import com.facebook.presto.spi.predicate.NullableValue;
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

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
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

    public static Partitioning create(PartitioningHandle handle, List<Symbol> columns)
    {
        return new Partitioning(handle, columns.stream()
                .map(ArgumentBinding::columnBinding)
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

    public Set<Symbol> getColumns()
    {
        return arguments.stream()
                .filter(ArgumentBinding::isVariable)
                .map(ArgumentBinding::getColumn)
                .collect(toImmutableSet());
    }

    public boolean isPartitionedWith(Partitioning right,
            Function<Symbol, Set<Symbol>> leftToRightMappings,
            Function<Symbol, Optional<NullableValue>> leftConstantMapping,
            Function<Symbol, Optional<NullableValue>> rightConstantMapping)
    {
        if (!handle.equals(right.handle)) {
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
            Function<Symbol, Optional<NullableValue>> leftConstantMapping,
            ArgumentBinding rightArgument,
            Function<Symbol, Optional<NullableValue>> rightConstantMapping,
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
                Optional<NullableValue> leftConstant = leftConstantMapping.apply(leftArgument.getColumn());
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
                Optional<NullableValue> rightConstant = rightConstantMapping.apply(rightArgument.getColumn());
                return rightConstant.isPresent() && rightConstant.get().equals(leftArgument.getConstant());
            }
        }
    }

    public boolean isPartitionedOn(Collection<Symbol> columns, Set<Symbol> knownConstants)
    {
        // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
        // can safely ignore all constant columns when comparing partition properties
        return arguments.stream()
                .filter(ArgumentBinding::isVariable)
                .map(ArgumentBinding::getColumn)
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
        Set<Symbol> nonConstantArgs = arguments.stream()
                .filter(ArgumentBinding::isVariable)
                .map(ArgumentBinding::getColumn)
                .filter(symbol -> !knownConstants.contains(symbol))
                .collect(toImmutableSet());
        return !nonConstantArgs.equals(keysWithoutConstants);
    }

    public Partitioning translate(Function<Symbol, Symbol> translator)
    {
        return new Partitioning(handle, arguments.stream()
                .map(argument -> argument.translate(translator))
                .collect(toImmutableList()));
    }

    public Optional<Partitioning> translate(Function<Symbol, Optional<Symbol>> translator, Function<Symbol, Optional<NullableValue>> constants)
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
        private final Symbol column;
        private final NullableValue constant;

        @JsonCreator
        public ArgumentBinding(
                @JsonProperty("column") Symbol column,
                @JsonProperty("constant") NullableValue constant)
        {
            this.column = column;
            this.constant = constant;
            checkArgument((column == null) != (constant == null), "Either column or constant must be set");
        }

        public static ArgumentBinding columnBinding(Symbol column)
        {
            return new ArgumentBinding(requireNonNull(column, "column is null"), null);
        }

        public static ArgumentBinding constantBinding(NullableValue constant)
        {
            return new ArgumentBinding(null, requireNonNull(constant, "constant is null"));
        }

        public boolean isConstant()
        {
            return constant != null;
        }

        public boolean isVariable()
        {
            return column != null;
        }

        @JsonProperty
        public Symbol getColumn()
        {
            return column;
        }

        @JsonProperty
        public NullableValue getConstant()
        {
            return constant;
        }

        public ArgumentBinding translate(Function<Symbol, Symbol> translator)
        {
            if (isConstant()) {
                return this;
            }
            return columnBinding(translator.apply(column));
        }

        public Optional<ArgumentBinding> translate(Function<Symbol, Optional<Symbol>> translator, Function<Symbol, Optional<NullableValue>> constants)
        {
            if (isConstant()) {
                return Optional.of(this);
            }

            Optional<ArgumentBinding> newColumn = translator.apply(column)
                    .map(ArgumentBinding::columnBinding);
            if (newColumn.isPresent()) {
                return newColumn;
            }

            // As a last resort, check for a constant mapping for the symbol
            // Note: this MUST be last because we want to favor the symbol representation
            // as it makes further optimizations possible.
            return constants.apply(column)
                    .map(ArgumentBinding::constantBinding);
        }

        @Override
        public String toString()
        {
            if (constant != null) {
                return constant.toString();
            }
            return "\"" + column + "\"";
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
            return Objects.equals(column, that.column) &&
                    Objects.equals(constant, that.constant);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(column, constant);
        }
    }
}
