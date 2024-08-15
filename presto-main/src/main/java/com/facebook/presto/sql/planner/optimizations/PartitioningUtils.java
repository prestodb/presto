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
import com.facebook.presto.common.Utils;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartitioningUtils
{
    private PartitioningUtils() {}

    @Deprecated
    public static boolean areCompatiblePartitionings(
            Partitioning left,
            Partitioning right,
            Metadata metadata,
            Session session)
    {
        if (!left.getHandle().equals(right.getHandle()) && !metadata.getCommonPartitioning(session, left.getHandle(), right.getHandle()).isPresent()) {
            return false;
        }

        return left.getArguments().equals(right.getArguments());
    }

    @Deprecated
    public static boolean areCompatiblePartitionings(
            Partitioning left,
            Partitioning right,
            Function<VariableReferenceExpression, Set<VariableReferenceExpression>> leftToRightMappings,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> leftConstantMapping,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> rightConstantMapping,
            Metadata metadata,
            Session session)
    {
        if (!left.getHandle().equals(right.getHandle()) && !metadata.getCommonPartitioning(session, left.getHandle(), right.getHandle()).isPresent()) {
            return false;
        }

        if (left.getArguments().size() != right.getArguments().size()) {
            return false;
        }

        for (int i = 0; i < left.getArguments().size(); i++) {
            RowExpression leftArgument = left.getArguments().get(i);
            RowExpression rightArgument = right.getArguments().get(i);

            if (!isPartitionedWith(leftArgument, leftConstantMapping, rightArgument, rightConstantMapping, leftToRightMappings)) {
                return false;
            }
        }
        return true;
    }

    //  Refined-over relation is reflexive.
    public static boolean isRefinedPartitioningOver(
            Partitioning left,
            Partitioning right,
            Metadata metadata,
            Session session)
    {
        if (!left.getHandle().equals(right.getHandle()) && !metadata.isRefinedPartitioningOver(session, left.getHandle(), right.getHandle())) {
            return false;
        }

        return left.getArguments().equals(right.getArguments());
    }

    //  Refined-over relation is reflexive.
    public static boolean isRefinedPartitioningOver(
            Partitioning left,
            Partitioning right,
            Function<VariableReferenceExpression, Set<VariableReferenceExpression>> leftToRightMappings,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> leftConstantMapping,
            Function<VariableReferenceExpression, Optional<ConstantExpression>> rightConstantMapping,
            Metadata metadata,
            Session session)
    {
        if (!metadata.isRefinedPartitioningOver(session, left.getHandle(), right.getHandle())) {
            return false;
        }
        List<RowExpression> leftArguments = left.getArguments();
        List<RowExpression> rightArguments = right.getArguments();
        if (leftArguments.size() != rightArguments.size()) {
            return false;
        }

        for (int i = 0; i < leftArguments.size(); i++) {
            RowExpression leftArgument = leftArguments.get(i);
            RowExpression rightArgument = rightArguments.get(i);

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

    public static boolean isPartitionedOn(Partitioning partitioning, Collection<VariableReferenceExpression> columns, Set<VariableReferenceExpression> knownConstants)
    {
        for (RowExpression argument : partitioning.getArguments()) {
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

    public static boolean isPartitionedOnExactly(Partitioning partitioning, Collection<VariableReferenceExpression> columns, Set<VariableReferenceExpression> knownConstants)
    {
        Set<VariableReferenceExpression> toCheck = new HashSet<>();
        for (RowExpression argument : partitioning.getArguments()) {
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

    public static boolean isEffectivelySinglePartition(Partitioning partitioning, Set<VariableReferenceExpression> knownConstants)
    {
        return isPartitionedOn(partitioning, ImmutableSet.of(), knownConstants);
    }

    public static boolean isRepartitionEffective(Partitioning partitioning, Collection<VariableReferenceExpression> keys, Set<VariableReferenceExpression> knownConstants)
    {
        Set<VariableReferenceExpression> keysWithoutConstants = keys.stream()
                .filter(variable -> !knownConstants.contains(variable))
                .collect(ImmutableSet.toImmutableSet());
        Set<VariableReferenceExpression> nonConstantArgs = partitioning.getArguments().stream()
                .filter(VariableReferenceExpression.class::isInstance)
                .map(VariableReferenceExpression.class::cast)
                .filter(variable -> !knownConstants.contains(variable))
                .collect(ImmutableSet.toImmutableSet());
        return !nonConstantArgs.equals(keysWithoutConstants);
    }

    // Tries to translate VariableReferenceExpression in arguments according to translator, keeps constant arguments unchanged. If any arguments failed to translate, return empty partitioning.
    public static Optional<Partitioning> translateVariableToRowExpression(Partitioning partitioning, Function<VariableReferenceExpression, Optional<RowExpression>> translator)
    {
        ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
        for (RowExpression argument : partitioning.getArguments()) {
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

        return Optional.of(new Partitioning(partitioning.getHandle(), newArguments.build()));
    }

    // Maps VariableReferenceExpression in both partitions to an COALESCE expression, keeps constant arguments unchanged.
    public static Optional<Partitioning> translateToCoalesce(Partitioning left, Partitioning right, Metadata metadata, Session session)
    {
        checkArgument(
                PropertyDerivations.arePartitionHandlesCompatibleForCoalesce(left.getHandle(), right.getHandle(), metadata, session),
                "incompatible partitioning handles: cannot coalesce %s and %s", left.getHandle(), right.getHandle());
        List<RowExpression> leftArguments = left.getArguments();
        List<RowExpression> rightArguments = right.getArguments();
        checkArgument(left.getArguments().size() == rightArguments.size(), "incompatible number of partitioning arguments: %s != %s", leftArguments.size(), rightArguments.size());
        ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
        for (int i = 0; i < leftArguments.size(); i++) {
            RowExpression leftArgument = leftArguments.get(i);
            RowExpression rightArgument = rightArguments.get(i);
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
        return Optional.of(new Partitioning(metadata.isRefinedPartitioningOver(session, right.getHandle(), left.getHandle()) ? left.getHandle() : right.getHandle(), arguments.build()));
    }

    public static Optional<Partitioning> translatePartitioningRowExpression(Partitioning partitioning, Map<VariableReferenceExpression, RowExpression> inputToOutputMappings, Map<VariableReferenceExpression, RowExpression> assignments)
    {
        ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
        for (RowExpression argument : partitioning.getArguments()) {
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
                if (translated == null) {
                    return Optional.empty();
                }
                newArguments.add(translated);
            }
        }

        return Optional.of(new Partitioning(partitioning.getHandle(), newArguments.build()));
    }

    public static PartitioningScheme translateOutputLayout(PartitioningScheme partitioningScheme, List<VariableReferenceExpression> newOutputLayout)
    {
        requireNonNull(newOutputLayout, "newOutputLayout is null");

        Utils.checkArgument(newOutputLayout.size() == partitioningScheme.getOutputLayout().size());

        List<VariableReferenceExpression> oldOutputLayout = partitioningScheme.getOutputLayout();
        Partitioning newPartitioning = translateVariable(partitioningScheme.getPartitioning(), variable -> newOutputLayout.get(oldOutputLayout.indexOf(variable)));

        Optional<VariableReferenceExpression> newHashSymbol = partitioningScheme.getHashColumn()
                .map(oldOutputLayout::indexOf)
                .map(newOutputLayout::get);

        return new PartitioningScheme(newPartitioning, newOutputLayout, newHashSymbol, partitioningScheme.isReplicateNullsAndAny(), partitioningScheme.getBucketToPartition());
    }

    // Translates VariableReferenceExpression in arguments according to translator, keeps other arguments unchanged.
    public static Partitioning translateVariable(Partitioning partitioning, Function<VariableReferenceExpression, VariableReferenceExpression> translator)
    {
        return new Partitioning(partitioning.getHandle(), partitioning.getArguments().stream()
                .map(argument -> {
                    if (argument instanceof VariableReferenceExpression) {
                        return translator.apply((VariableReferenceExpression) argument);
                    }
                    return argument;
                })
                .collect(ImmutableList.toImmutableList()));
    }
}
