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
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.SystemSessionProperties.preferStreamingOperators;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.singlePartition;
import static com.facebook.presto.sql.planner.optimizations.PreferredPartitioning.partitioned;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

class StreamPreferredProperties
{
    private final Optional<Boolean> parallelPreferred;

    // Specific partitioning requested
    // if set, parallelPreferred must be set to a matching value
    private final Optional<PreferredPartitioning> partitioning;

    private final boolean orderSensitive;

    private StreamPreferredProperties(PreferredPartitioning partitioning)
    {
        this(Optional.of(!partitioning.isSinglePreferred()), Optional.of(partitioning), false);
    }

    private StreamPreferredProperties(
            Optional<Boolean> parallelPreferred,
            Optional<PreferredPartitioning> partitioning,
            boolean orderSensitive)
    {
        this.parallelPreferred = requireNonNull(parallelPreferred, "parallelPreferred is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.orderSensitive = orderSensitive;

        // if there is a preference for single, the partitioning must be set to a single partitioning
        if (parallelPreferred.isPresent() && !parallelPreferred.get()) {
            checkArgument(partitioning.isPresent() && partitioning.get().isSinglePreferred(), "Single stream preference must use a single partitioning");
        }

        // if there is an explicit partitioning preference, verify the other properties agree
        if (partitioning.isPresent()) {
            checkArgument(!parallelPreferred.isPresent() || parallelPreferred.get() == !partitioning.get().isSinglePreferred(), "parallelPreferred must agree with preferred partitioning");

            // the only allowed order sensitive partitioning is single
            checkArgument(!orderSensitive || partitioning.get().isSinglePreferred(), "An order sensitive context can not prefer partitioning");
        }
    }

    public static StreamPreferredProperties any()
    {
        return new StreamPreferredProperties(Optional.empty(), Optional.empty(), false);
    }

    public static StreamPreferredProperties singleStream()
    {
        return partitionedOn(singlePartition());
    }

    public static StreamPreferredProperties defaultParallelism(Session session)
    {
        if (getTaskConcurrency(session) > 1 && !preferStreamingOperators(session)) {
            return new StreamPreferredProperties(Optional.of(true), Optional.empty(), false);
        }
        return any();
    }

    public static StreamPreferredProperties partitionedOn(Partitioning partitioning)
    {
        return new StreamPreferredProperties(partitioned(partitioning));
    }

    public StreamPreferredProperties withoutPreference()
    {
        return new StreamPreferredProperties(Optional.empty(), Optional.empty(), orderSensitive);
    }

    public StreamPreferredProperties withPartitioning(Collection<Symbol> partitionSymbols)
    {
        if (partitionSymbols.isEmpty()) {
            return singleStream();
        }

        Collection<Symbol> desiredPartitioning = partitionSymbols;
        if (partitioning.isPresent()) {
            PreferredPartitioning partitioning = this.partitioning.get();
            if (partitioning.getPartitioning().isPresent()) {
                if (partitioning.getPartitioningColumns().equals(desiredPartitioning)) {
                    return this;
                }
            }
            else {
                // If there are common columns between our requirements and the desired partitionSymbols, both can be satisfied in one shot
                Set<Symbol> common = Sets.intersection(ImmutableSet.copyOf(desiredPartitioning), partitioning.getPartitioningColumns());

                // If we find common partitioning columns, use them, else use child's partitioning columns
                if (!common.isEmpty()) {
                    desiredPartitioning = common;
                }
            }
        }

        return new StreamPreferredProperties(Optional.of(true), Optional.of(partitioned(desiredPartitioning)), false);
    }

    public StreamPreferredProperties withDefaultParallelism(Session session)
    {
        // do not override an existing parallel preference
        if (isParallelPreferred()) {
            return this;
        }

        if (getTaskConcurrency(session) > 1 && !preferStreamingOperators(session)) {
            return new StreamPreferredProperties(Optional.of(true), Optional.empty(), orderSensitive);
        }
        return this;
    }

    public StreamPreferredProperties withOrderSensitivity()
    {
        return new StreamPreferredProperties(parallelPreferred, partitioning, true);
    }

    public boolean isSatisfiedBy(Session session, StreamProperties actualProperties)
    {
        // If there is no specific preference, anything is acceptable
        if (!parallelPreferred.isPresent() && !partitioning.isPresent()) {
            return true;
        }

        if (orderSensitive && actualProperties.isOrdered()) {
            // ordered is required to be a single stream, so in this ordered case single is
            // considered satisfactory for a parallel preference
            return true;
        }

        // is there a specific partitioning preference
        if (partitioning.isPresent()) {
            PreferredPartitioning preferredPartitioning = partitioning.get();
            if (preferredPartitioning.getPartitioning().isPresent()) {
                Partitioning partitioning = preferredPartitioning.getPartitioning().get();
                return actualProperties.isPartitionedOn(partitioning);
            }

            // the system will choose a parallel partitioning only if task concurrency is enabled
            if (getTaskConcurrency(session) > 1) {
                return actualProperties.isPartitionedOn(preferredPartitioning.getPartitioningColumns());
            }
        }

        // at this point it is assured that there is not a preference for single, since single must have a specific partitioning set
        verify(!isSingleStreamPreferred());

        // even though there is a preference for parallel, the system will only choose a parallel
        // output if task concurrency is enabled; otherwise, the system will choose single
        return actualProperties.isSingleStream() == (getTaskConcurrency(session) == 1);
    }

    public boolean isSingleStreamPreferred()
    {
        return parallelPreferred.isPresent() && !parallelPreferred.get();
    }

    public boolean isParallelPreferred()
    {
        return parallelPreferred.isPresent() && parallelPreferred.get();
    }

    public Optional<PreferredPartitioning> getPartitioning()
    {
        return partitioning;
    }

    public Optional<Set<Symbol>> getPartitioningColumns()
    {
        return partitioning.map(PreferredPartitioning::getPartitioningColumns);
    }

    public StreamPreferredProperties constrainTo(Iterable<Symbol> symbols)
    {
        if (!partitioning.isPresent()) {
            return this;
        }

        Set<Symbol> availableSymbols = ImmutableSet.copyOf(symbols);
        Optional<PreferredPartitioning> newPartitioning = partitioning.get().translate(symbol -> availableSymbols.contains(symbol) ? Optional.of(symbol) : Optional.empty());
        return new StreamPreferredProperties(parallelPreferred, newPartitioning, orderSensitive);
    }
}
