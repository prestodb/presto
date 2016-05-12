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
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public final class Partitioning
{
    private final PartitioningHandle handle;
    private final List<PartitionFunctionArgumentBinding> arguments;

    public Partitioning(PartitioningHandle handle, List<PartitionFunctionArgumentBinding> arguments)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    public PartitioningHandle getHandle()
    {
        return handle;
    }

    public List<PartitionFunctionArgumentBinding> getArguments()
    {
        return arguments;
    }

    public boolean isPartitionedOn(PartitioningHandle partitioning, List<Symbol> columns)
    {
        if (!handle.equals(partitioning)) {
            return false;
        }

        if (arguments.size() != columns.size()) {
            return false;
        }

        for (int i = 0; i < arguments.size(); i++) {
            PartitionFunctionArgumentBinding argument = arguments.get(i);
            if (argument.isVariable() && !argument.getColumn().equals(columns.get(i))) {
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
        if (!handle.equals(right.handle)) {
            return false;
        }

        if (arguments.size() != right.arguments.size()) {
            return false;
        }

        for (int i = 0; i < arguments.size(); i++) {
            PartitionFunctionArgumentBinding leftArgument = arguments.get(i);
            PartitionFunctionArgumentBinding rightArgument = right.arguments.get(i);

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
        return arguments.stream()
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
        Set<Symbol> nonConstantArgs = arguments.stream()
                .filter(PartitionFunctionArgumentBinding::isVariable)
                .map(PartitionFunctionArgumentBinding::getColumn)
                .filter(symbol -> !knownConstants.contains(symbol))
                .collect(toImmutableSet());
        return !nonConstantArgs.equals(keysWithoutConstants);
    }

    public Optional<Partitioning> translate(Function<Symbol, Optional<Symbol>> translator, Function<Symbol, Optional<NullableValue>> constants)
    {
        ImmutableList.Builder<PartitionFunctionArgumentBinding> newArguments = ImmutableList.builder();
        for (PartitionFunctionArgumentBinding argument : arguments) {
            Optional<PartitionFunctionArgumentBinding> newArgument = translate(argument, translator, constants);
            if (!newArgument.isPresent()) {
                return Optional.empty();
            }
            newArguments.add(newArgument.get());
        }

        return Optional.of(new Partitioning(handle, newArguments.build()));
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
            return Optional.of(new PartitionFunctionArgumentBinding(constant.get()));
        }

        return Optional.empty();
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
        return MoreObjects.toStringHelper(this)
                .add("handle", handle)
                .add("arguments", arguments)
                .toString();
    }

    @Immutable
    public static final class PartitionFunctionArgumentBinding
    {
        private final Symbol column;
        private final NullableValue constant;

        public PartitionFunctionArgumentBinding(Symbol column)
        {
            this.column = requireNonNull(column, "column is null");
            this.constant = null;
        }

        public PartitionFunctionArgumentBinding(NullableValue constant)
        {
            this.constant = requireNonNull(constant, "constant is null");
            this.column = null;
        }

        @JsonCreator
        public PartitionFunctionArgumentBinding(
                @JsonProperty("column") Symbol column,
                @JsonProperty("constant") NullableValue constant)
        {
            this.column = column;
            this.constant = constant;
            checkArgument((column == null) != (constant == null), "Column or constant be set");
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
            PartitionFunctionArgumentBinding that = (PartitionFunctionArgumentBinding) o;
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
