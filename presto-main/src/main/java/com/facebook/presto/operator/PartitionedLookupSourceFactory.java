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
package com.facebook.presto.operator;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.MoreFutures;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.facebook.presto.operator.OuterLookupSource.createOuterLookupSourceSupplier;
import static com.facebook.presto.operator.PartitionedLookupSource.createPartitionedLookupSourceSupplier;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class PartitionedLookupSourceFactory
        implements LookupSourceFactory
{
    private final List<Type> types;
    private final List<Type> outputTypes;
    private final Map<Symbol, Integer> layout;
    private final List<Type> hashChannelTypes;
    private final Supplier<LookupSource>[] partitions;
    private final boolean outer;
    private final CompletableFuture<?> destroyed = new CompletableFuture<>();

    @GuardedBy("this")
    private int partitionsSet;

    @GuardedBy("this")
    private Supplier<LookupSource> lookupSourceSupplier;

    @GuardedBy("this")
    private final List<SettableFuture<LookupSource>> lookupSourceFutures = new ArrayList<>();

    public PartitionedLookupSourceFactory(List<Type> types, List<Type> outputTypes, List<Integer> hashChannels, int partitionCount, Map<Symbol, Integer> layout, boolean outer)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        this.layout = ImmutableMap.copyOf(layout);
        this.partitions = (Supplier<LookupSource>[]) new Supplier<?>[partitionCount];
        this.outer = outer;

        hashChannelTypes = hashChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    @Override
    public Map<Symbol, Integer> getLayout()
    {
        return layout;
    }

    @Override
    public synchronized ListenableFuture<LookupSource> createLookupSource()
    {
        if (lookupSourceSupplier != null) {
            return Futures.immediateFuture(lookupSourceSupplier.get());
        }

        SettableFuture<LookupSource> lookupSourceFuture = SettableFuture.create();
        lookupSourceFutures.add(lookupSourceFuture);
        return lookupSourceFuture;
    }

    public void setPartitionLookupSourceSupplier(int partitionIndex, Supplier<LookupSource> partitionLookupSource)
    {
        requireNonNull(partitionLookupSource, "partitionLookupSource is null");

        Supplier<LookupSource> lookupSourceSupplier = null;
        List<SettableFuture<LookupSource>> lookupSourceFutures = null;
        synchronized (this) {
            if (destroyed.isDone()) {
                return;
            }

            checkState(partitions[partitionIndex] == null, "Partition already set");
            partitions[partitionIndex] = partitionLookupSource;
            partitionsSet++;

            if (partitionsSet == partitions.length) {
                if (partitionsSet != 1) {
                    List<Supplier<LookupSource>> partitions = ImmutableList.copyOf(this.partitions);
                    this.lookupSourceSupplier = createPartitionedLookupSourceSupplier(partitions, hashChannelTypes, outer);
                }
                else if (outer) {
                    this.lookupSourceSupplier = createOuterLookupSourceSupplier(partitionLookupSource);
                }
                else {
                    this.lookupSourceSupplier = partitionLookupSource;
                }

                // store lookup source supplier and futures into local variables so they can be used outside of the lock
                lookupSourceSupplier = this.lookupSourceSupplier;
                lookupSourceFutures = ImmutableList.copyOf(this.lookupSourceFutures);
            }
        }

        if (lookupSourceSupplier != null) {
            for (SettableFuture<LookupSource> lookupSourceFuture : lookupSourceFutures) {
                lookupSourceFuture.set(lookupSourceSupplier.get());
            }
        }
    }

    @Override
    public void destroy()
    {
        destroyed.complete(null);
    }

    public CompletableFuture<?> isDestroyed()
    {
        return MoreFutures.unmodifiableFuture(destroyed);
    }
}
