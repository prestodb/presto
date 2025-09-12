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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.SystemSessionProperties.preferStreamingOperators;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.FIXED;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.MULTIPLE;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.SINGLE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class StreamPreferredProperties
{
    private final Optional<StreamDistribution> distribution;

    private final boolean exactColumnOrder;
    private final Optional<List<VariableReferenceExpression>> partitioningColumns; // if missing => any partitioning scheme is acceptable

    private final boolean orderSensitive;

    private StreamPreferredProperties(Optional<StreamDistribution> distribution, Optional<? extends Iterable<VariableReferenceExpression>> partitioningColumns, boolean orderSensitive)
    {
        this(distribution, false, partitioningColumns, orderSensitive);
    }

    private StreamPreferredProperties(
            Optional<StreamDistribution> distribution,
            boolean exactColumnOrder,
            Optional<? extends Iterable<VariableReferenceExpression>> partitioningColumns,
            boolean orderSensitive)
    {
        this.distribution = requireNonNull(distribution, "distribution is null");
        this.partitioningColumns = requireNonNull(partitioningColumns, "partitioningColumns is null").map(ImmutableList::copyOf);
        this.exactColumnOrder = exactColumnOrder;
        this.orderSensitive = orderSensitive;

        checkArgument(!orderSensitive || !partitioningColumns.isPresent(), "An order sensitive context can not prefer partitioning");
    }

    public static StreamPreferredProperties any()
    {
        return new StreamPreferredProperties(Optional.empty(), Optional.empty(), false);
    }

    public static StreamPreferredProperties singleStream()
    {
        return new StreamPreferredProperties(Optional.of(SINGLE), Optional.empty(), false);
    }

    public static StreamPreferredProperties fixedParallelism()
    {
        return new StreamPreferredProperties(Optional.of(FIXED), Optional.empty(), false);
    }

    public static StreamPreferredProperties defaultParallelism(Session session)
    {
        if (getTaskConcurrency(session) > 1 && !preferStreamingOperators(session)) {
            return new StreamPreferredProperties(Optional.of(MULTIPLE), Optional.empty(), false);
        }
        return any();
    }

    public StreamPreferredProperties withParallelism()
    {
        // do not override an existing parallel preference
        if (isParallelPreferred()) {
            return this;
        }
        return new StreamPreferredProperties(Optional.of(MULTIPLE), Optional.empty(), orderSensitive);
    }

    public StreamPreferredProperties withFixedParallelism()
    {
        if (distribution.isPresent() && distribution.get() == FIXED) {
            return this;
        }
        return fixedParallelism();
    }

    public static StreamPreferredProperties partitionedOn(Collection<VariableReferenceExpression> partitionSymbols)
    {
        if (partitionSymbols.isEmpty()) {
            return singleStream();
        }

        // Prefer partitioning on given partitioning symbols. Partition hash can be evaluated in any order.
        return new StreamPreferredProperties(Optional.of(FIXED), false, Optional.of(ImmutableSet.copyOf(partitionSymbols)), false);
    }

    public static StreamPreferredProperties exactlyPartitionedOn(Collection<VariableReferenceExpression> partitionVariables)
    {
        if (partitionVariables.isEmpty()) {
            return singleStream();
        }

        // this must be the exact partitioning symbols, in the exact order
        return new StreamPreferredProperties(Optional.of(FIXED), true, Optional.of(ImmutableList.copyOf(partitionVariables)), false);
    }

    public StreamPreferredProperties withoutPreference()
    {
        return new StreamPreferredProperties(Optional.empty(), Optional.empty(), orderSensitive);
    }

    public StreamPreferredProperties withPartitioning(Collection<VariableReferenceExpression> partitionVariables)
    {
        if (partitionVariables.isEmpty()) {
            return singleStream();
        }

        Iterable<VariableReferenceExpression> desiredPartitioning = partitionVariables;
        if (partitioningColumns.isPresent()) {
            if (exactColumnOrder) {
                if (partitioningColumns.get().equals(desiredPartitioning)) {
                    return this;
                }
            }
            else {
                // If there are common columns between our requirements and the desired partitionSymbols, both can be satisfied in one shot
                Set<VariableReferenceExpression> common = Sets.intersection(ImmutableSet.copyOf(desiredPartitioning), ImmutableSet.copyOf(partitioningColumns.get()));

                // If we find common partitioning columns, use them, else use child's partitioning columns
                if (!common.isEmpty()) {
                    desiredPartitioning = common;
                }
            }
        }

        return new StreamPreferredProperties(distribution, Optional.of(desiredPartitioning), false);
    }

    public StreamPreferredProperties withDefaultParallelism(Session session)
    {
        if (getTaskConcurrency(session) > 1 && !preferStreamingOperators(session)) {
            return withParallelism();
        }
        return this;
    }

    public boolean isSatisfiedBy(StreamProperties actualProperties)
    {
        // is there a specific preference
        if (!distribution.isPresent() && !partitioningColumns.isPresent()) {
            return true;
        }

        if (isOrderSensitive() && actualProperties.isOrdered()) {
            // ordered is required to be a single stream, so in this ordered case SINGLE is
            // considered satisfactory for MULTIPLE and FIXED
            return true;
        }

        if (distribution.isPresent()) {
            StreamDistribution actualDistribution = actualProperties.getDistribution();
            if (distribution.get() == SINGLE && actualDistribution != SINGLE) {
                return false;
            }
            else if (distribution.get() == FIXED && actualDistribution != FIXED) {
                return false;
            }
            else if (distribution.get() == MULTIPLE && actualDistribution != FIXED && actualDistribution != MULTIPLE) {
                return false;
            }
        }
        else if (actualProperties.getDistribution() == SINGLE) {
            // when there is no explicit distribution preference, SINGLE satisfies everything
            return true;
        }

        // is there a preference for a specific partitioning scheme?
        if (partitioningColumns.isPresent()) {
            if (exactColumnOrder) {
                return actualProperties.isExactlyPartitionedOn(partitioningColumns.get());
            }
            return actualProperties.isPartitionedOn(partitioningColumns.get());
        }

        return true;
    }

    public boolean isSingleStreamPreferred()
    {
        return distribution.isPresent() && distribution.get() == SINGLE;
    }

    public boolean isParallelPreferred()
    {
        return distribution.isPresent() && distribution.get() != SINGLE;
    }

    public Optional<List<VariableReferenceExpression>> getPartitioningColumns()
    {
        return partitioningColumns;
    }

    public boolean isOrderSensitive()
    {
        return orderSensitive;
    }

    public StreamPreferredProperties translate(Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> translator)
    {
        return new StreamPreferredProperties(
                distribution,
                partitioningColumns.flatMap(partitioning -> translateVariables(partitioning, translator)),
                orderSensitive);
    }

    private static Optional<List<VariableReferenceExpression>> translateVariables(Iterable<VariableReferenceExpression> partitioning, Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> translator)
    {
        ImmutableList.Builder<VariableReferenceExpression> newPartitioningColumns = ImmutableList.builder();
        for (VariableReferenceExpression partitioningColumn : partitioning) {
            Optional<VariableReferenceExpression> translated = translator.apply(partitioningColumn);
            if (!translated.isPresent()) {
                return Optional.empty();
            }
            newPartitioningColumns.add(translated.get());
        }
        return Optional.of(newPartitioningColumns.build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("distribution", distribution.orElse(null))
                .add("partitioningColumns", partitioningColumns.orElse(null))
                .omitNullValues()
                .toString();
    }

    public StreamPreferredProperties withOrderSensitivity()
    {
        return new StreamPreferredProperties(distribution, false, Optional.empty(), true);
    }

    public StreamPreferredProperties constrainTo(Iterable<VariableReferenceExpression> variables)
    {
        if (!partitioningColumns.isPresent()) {
            return this;
        }

        ImmutableSet<VariableReferenceExpression> availableVariables = ImmutableSet.copyOf(variables);
        if (exactColumnOrder) {
            if (availableVariables.containsAll(partitioningColumns.get())) {
                return this;
            }
            return any();
        }

        List<VariableReferenceExpression> common = partitioningColumns.get().stream()
                .filter(availableVariables::contains)
                .collect(toImmutableList());
        if (common.isEmpty()) {
            return any();
        }
        return new StreamPreferredProperties(distribution, Optional.of(common), false);
    }
}
