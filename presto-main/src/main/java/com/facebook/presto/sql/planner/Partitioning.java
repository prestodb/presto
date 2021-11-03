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
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.planner.optimizations.PropertyDerivations.arePartitionHandlesCompatibleForCoalesce;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Immutable
public final class Partitioning
{
    private final PartitioningHandle handle;
    private final List<RowExpression> arguments;

    private Partitioning(PartitioningHandle handle, List<RowExpression> arguments)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    public static <T extends RowExpression> Partitioning create(PartitioningHandle handle, Collection<T> columns)
    {
        return new Partitioning(handle, columns.stream()
                .map(RowExpression.class::cast)
                .collect(toImmutableList()));
    }

    // Factory method for JSON serde only!
    @JsonCreator
    public static Partitioning jsonCreate(
            @JsonProperty("handle") PartitioningHandle handle,
            @JsonProperty("arguments") List<RowExpression> arguments)
    {
        return new Partitioning(handle, arguments);
    }

    @JsonProperty
    public PartitioningHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    public Set<VariableReferenceExpression> getVariableReferences()
    {
        return arguments.stream()
                .filter(VariableReferenceExpression.class::isInstance)
                .map(VariableReferenceExpression.class::cast)
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
            RowExpression leftArgument = arguments.get(i);
            RowExpression rightArgument = right.arguments.get(i);

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
            RowExpression leftArgument = arguments.get(i);
            RowExpression rightArgument = right.arguments.get(i);

            if (!isPartitionedWith(leftArgument, leftConstantMapping, rightArgument, rightConstantMapping, leftToRightMappings)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPartitionedWith(
            RowExpression leftArgument,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> leftConstantMapping,
            RowExpression rightArgument,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> rightConstantMapping,
            Function<VariableReferenceExpression, Set<VariableReferenceExpression>> leftToRightMappings)
    {
        if (leftArgument instanceof VariableReferenceExpression) {
            if (rightArgument instanceof VariableReferenceExpression) {
                // variable == variable
                Set<VariableReferenceExpression> mappedColumns = leftToRightMappings.apply((VariableReferenceExpression) leftArgument);
                return mappedColumns.contains(rightArgument);
            }
            else if (rightArgument instanceof ConstantExpression) {
                // variable == constant
                // Normally, this would be a false condition, but if we happen to have an external
                // mapping from the variable to a constant value and that constant value matches the
                // right value, then we are co-partitioned.
                Optional<ConstantExpression> leftConstant = leftConstantMapping.apply((VariableReferenceExpression) leftArgument);
                return leftConstant.isPresent() && leftConstant.get().equals(rightArgument);
            }
            else {
                // variable == coalesce
                return false;
            }
        }
        else if (leftArgument instanceof ConstantExpression) {
            if (rightArgument instanceof ConstantExpression) {
                // constant == constant
                return leftArgument.equals(rightArgument);
            }
            else if (rightArgument instanceof VariableReferenceExpression) {
                // constant == variable
                Optional<ConstantExpression> rightConstant = rightConstantMapping.apply((VariableReferenceExpression) rightArgument);
                return rightConstant.isPresent() && rightConstant.get().equals(leftArgument);
            }
            else {
                // constant == coalesce
                return false;
            }
        }
        else {
            // coalesce == ?
            return false;
        }
    }

    public boolean isPartitionedOn(Collection<VariableReferenceExpression> columns, Set<VariableReferenceExpression> knownConstants)
    {
        for (RowExpression argument : arguments) {
            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            if (argument instanceof ConstantExpression) {
                continue;
            }
            if (!(argument instanceof VariableReferenceExpression)) {
                return false;
            }
            if (!knownConstants.contains(argument) && !columns.contains(argument)) {
                return false;
            }
        }
        return true;
    }

    public boolean isPartitionedOnExactly(Collection<VariableReferenceExpression> columns, Set<VariableReferenceExpression> knownConstants)
    {
        Set<VariableReferenceExpression> toCheck = new HashSet<>();
        for (RowExpression argument : arguments) {
            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            if (argument instanceof ConstantExpression) {
                continue;
            }
            if (!(argument instanceof VariableReferenceExpression)) {
                return false;
            }
            if (knownConstants.contains(argument)) {
                continue;
            }
            toCheck.add((VariableReferenceExpression) argument);
        }
        return ImmutableSet.copyOf(columns).equals(toCheck);
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
                .filter(VariableReferenceExpression.class::isInstance)
                .map(VariableReferenceExpression.class::cast)
                .filter(variable -> !knownConstants.contains(variable))
                .collect(toImmutableSet());
        return !nonConstantArgs.equals(keysWithoutConstants);
    }

    // Translates VariableReferenceExpression in arguments according to translator, keeps other arguments unchanged.
    public Partitioning translateVariable(Function<VariableReferenceExpression, VariableReferenceExpression> translator)
    {
        return new Partitioning(handle, arguments.stream()
                .map(argument -> {
                    if (argument instanceof VariableReferenceExpression) {
                        return translator.apply((VariableReferenceExpression) argument);
                    }
                    return argument;
                })
                .collect(toImmutableList()));
    }

    // Tries to translate VariableReferenceExpression in arguments according to translator, keeps constant arguments unchanged. If any arguments failed to translate, return empty partitioning.
    public Optional<Partitioning> translateVariableToRowExpression(Function<VariableReferenceExpression, Optional<RowExpression>> translator)
    {
        ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
        for (RowExpression argument : arguments) {
            if (argument instanceof ConstantExpression) {
                newArguments.add(argument);
            }
            else if (argument instanceof VariableReferenceExpression) {
                Optional<RowExpression> newArgument = translator.apply((VariableReferenceExpression) argument);
                if (!newArgument.isPresent()) {
                    return Optional.empty();
                }
                newArguments.add(newArgument.get());
            }
            else {
                return Optional.empty();
            }
        }

        return Optional.of(new Partitioning(handle, newArguments.build()));
    }

    // Maps VariableReferenceExpression in both partitions to an COALESCE expression, keeps constant arguments unchanged.
    public Optional<Partitioning> translateToCoalesce(Partitioning other, Metadata metadata, Session session)
    {
        checkArgument(arePartitionHandlesCompatibleForCoalesce(this.handle, other.handle, metadata, session), "incompatible partitioning handles: cannot coalesce %s and %s", this.handle, other.handle);
        checkArgument(this.arguments.size() == other.arguments.size(), "incompatible number of partitioning arguments: %s != %s", this.arguments.size(), other.arguments.size());
        ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
        for (int i = 0; i < this.arguments.size(); i++) {
            RowExpression leftArgument = this.arguments.get(i);
            RowExpression rightArgument = other.arguments.get(i);
            if (leftArgument instanceof ConstantExpression) {
                arguments.add(leftArgument);
            }
            else if (rightArgument instanceof ConstantExpression) {
                arguments.add(rightArgument);
            }
            else if (leftArgument instanceof VariableReferenceExpression && rightArgument instanceof VariableReferenceExpression) {
                VariableReferenceExpression leftVariable = (VariableReferenceExpression) leftArgument;
                VariableReferenceExpression rightVariable = (VariableReferenceExpression) rightArgument;
                checkArgument(leftVariable.getType().equals(rightVariable.getType()), "incompatible types: %s != %s", leftVariable.getType(), rightVariable.getType());
                arguments.add(new SpecialFormExpression(COALESCE, leftVariable.getType(), ImmutableList.of(leftVariable, rightVariable)));
            }
            else {
                return Optional.empty();
            }
        }
        return Optional.of(new Partitioning(metadata.isRefinedPartitioningOver(session, other.handle, this.handle) ? this.handle : other.handle, arguments.build()));
    }

    public Optional<Partitioning> translateRowExpression(Map<VariableReferenceExpression, RowExpression> inputToOutputMappings, Map<VariableReferenceExpression, RowExpression> assignments, TypeProvider types)
    {
        ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
        for (RowExpression argument : arguments) {
            if (argument instanceof ConstantExpression) {
                newArguments.add(argument);
            }
            else if (argument instanceof VariableReferenceExpression) {
                if (!inputToOutputMappings.containsKey(argument)) {
                    return Optional.empty();
                }
                newArguments.add(inputToOutputMappings.get(argument));
            }
            else {
                checkArgument(argument instanceof SpecialFormExpression && ((SpecialFormExpression) argument).getForm().equals(COALESCE), format("Expect argument to be COALESCE but get %s", argument));
                Set<RowExpression> coalesceArguments = ImmutableSet.copyOf(((SpecialFormExpression) argument).getArguments());
                checkArgument(coalesceArguments.stream().allMatch(VariableReferenceExpression.class::isInstance), format("Expect arguments of COALESCE to be VariableReferenceExpression but get %s", coalesceArguments));
                // We are using the property that the result of coalesce from full outer join keys would not be null despite of the order
                // of the arguments. Thus we extract and compare the variables of the COALESCE as a set rather than compare COALESCE directly.
                VariableReferenceExpression translated = null;
                for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignments.entrySet()) {
                    if (isExpression(entry.getValue())) {
                        if (castToExpression(entry.getValue()) instanceof CoalesceExpression) {
                            Set<Expression> coalesceOperands = ImmutableSet.copyOf(((CoalesceExpression) castToExpression(entry.getValue())).getOperands());
                            if (!coalesceOperands.stream().allMatch(SymbolReference.class::isInstance)) {
                                continue;
                            }

                            if (coalesceOperands.stream()
                                    .map(operand -> new VariableReferenceExpression(((SymbolReference) operand).getName(), types.get(operand)))
                                    .collect(toImmutableSet())
                                    .equals(coalesceArguments)) {
                                translated = entry.getKey();
                                break;
                            }
                        }
                    }
                    else {
                        if (entry.getValue() instanceof SpecialFormExpression && ((SpecialFormExpression) entry.getValue()).getForm().equals(COALESCE)) {
                            Set<RowExpression> assignmentArguments = ImmutableSet.copyOf(((SpecialFormExpression) entry.getValue()).getArguments());
                            if (!assignmentArguments.stream().allMatch(VariableReferenceExpression.class::isInstance)) {
                                continue;
                            }

                            if (assignmentArguments.equals(coalesceArguments)) {
                                translated = entry.getKey();
                                break;
                            }
                        }
                    }
                }
                if (translated == null) {
                    return Optional.empty();
                }
                newArguments.add(translated);
            }
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
}
