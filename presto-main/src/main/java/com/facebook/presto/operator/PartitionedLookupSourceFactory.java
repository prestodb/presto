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

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static com.facebook.presto.operator.OuterLookupSource.createOuterLookupSourceSupplier;
import static com.facebook.presto.operator.PartitionedLookupSource.createPartitionedLookupSourceSupplier;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static java.util.Objects.requireNonNull;

public final class PartitionedLookupSourceFactory
        implements LookupSourceFactory
{
    private final List<Type> types;
    private final List<Type> outputTypes;
    private final Map<Symbol, Integer> layout;
    private final List<Type> hashChannelTypes;
    private final boolean outer;
    private final SettableFuture<?> destroyed = SettableFuture.create();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    @GuardedBy("rwLock")
    private final Supplier<LookupSource>[] partitions;

    @GuardedBy("rwLock")
    private int partitionsSet;

    @GuardedBy("rwLock")
    private TrackingLookupSourceSupplier lookupSourceSupplier;

    @GuardedBy("rwLock")
    private final List<SettableFuture<LookupSourceProvider>> lookupSourceFutures = new ArrayList<>();

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
    public ListenableFuture<LookupSourceProvider> createLookupSourceProvider()
    {
        rwLock.writeLock().lock();
        try {
            if (lookupSourceSupplier != null) {
                return Futures.immediateFuture(new SimpleLookupSourceProvider(lookupSourceSupplier.getLookupSource()));
            }

            SettableFuture<LookupSourceProvider> lookupSourceFuture = SettableFuture.create();
            lookupSourceFutures.add(lookupSourceFuture);
            return lookupSourceFuture;
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    public void setPartitionLookupSourceSupplier(int partitionIndex, Supplier<LookupSource> partitionLookupSource)
    {
        requireNonNull(partitionLookupSource, "partitionLookupSource is null");

        boolean completed;

        rwLock.writeLock().lock();
        try {
            if (destroyed.isDone()) {
                return;
            }

            checkState(partitions[partitionIndex] == null, "Partition already set");
            partitions[partitionIndex] = partitionLookupSource;
            partitionsSet++;
            completed = partitionsSet == partitions.length;
        }
        finally {
            rwLock.writeLock().unlock();
        }

        if (completed) {
            supplyLookupSources();
        }
    }

    private void supplyLookupSources()
    {
        checkState(!rwLock.isWriteLockedByCurrentThread());

        TrackingLookupSourceSupplier lookupSourceSupplier;
        List<SettableFuture<LookupSourceProvider>> lookupSourceFutures;

        rwLock.writeLock().lock();
        try {
            checkState(partitionsSet == partitions.length, "Not all set yet");
            checkState(this.lookupSourceSupplier == null, "Already supplied");

            if (partitionsSet != 1) {
                List<Supplier<LookupSource>> partitions = ImmutableList.copyOf(this.partitions);
                this.lookupSourceSupplier = createPartitionedLookupSourceSupplier(partitions, hashChannelTypes, outer);
            }
            else if (outer) {
                this.lookupSourceSupplier = createOuterLookupSourceSupplier(partitions[0]);
            }
            else {
                this.lookupSourceSupplier = TrackingLookupSourceSupplier.nonTracking(partitions[0]);
            }

            // store lookup source supplier and futures into local variables so they can be used outside of the lock
            lookupSourceSupplier = this.lookupSourceSupplier;
            lookupSourceFutures = ImmutableList.copyOf(this.lookupSourceFutures);
        }
        finally {
            rwLock.writeLock().unlock();
        }

        for (SettableFuture<LookupSourceProvider> lookupSourceFuture : lookupSourceFutures) {
            lookupSourceFuture.set(new SimpleLookupSourceProvider(lookupSourceSupplier.getLookupSource()));
        }
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        TrackingLookupSourceSupplier lookupSourceSupplier;

        rwLock.writeLock().lock();
        try {
            checkState(this.lookupSourceSupplier != null, "lookup source not ready yet");
            lookupSourceSupplier = this.lookupSourceSupplier;
        }
        finally {
            rwLock.writeLock().unlock();
        }

        return lookupSourceSupplier.getOuterPositionIterator();
    }

    @Override
    public void destroy()
    {
        destroyed.set(null);
    }

    public ListenableFuture<?> isDestroyed()
    {
        return nonCancellationPropagating(destroyed);
    }
}
