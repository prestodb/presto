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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class PartitionedConsumption<T>
{
    private final int consumersCount;
    private final AtomicInteger consumed = new AtomicInteger();
    @Nullable
    private List<Partition<T>> partitions;

    public PartitionedConsumption(int consumersCount, Iterable<Integer> partitionNumbers, IntFunction<ListenableFuture<T>> loader, IntConsumer disposer)
    {
        this(consumersCount, immediateFuture(null), partitionNumbers, loader, disposer);
    }

    public PartitionedConsumption(
            int consumersCount,
            ListenableFuture<?> activator,
            Iterable<Integer> partitionNumbers,
            IntFunction<ListenableFuture<T>> loader,
            IntConsumer disposer)
    {
        checkArgument(consumersCount > 0, "consumersCount must be positive");
        this.consumersCount = consumersCount;
        this.partitions = createPartitions(activator, partitionNumbers, loader, disposer);
    }

    private List<Partition<T>> createPartitions(
            ListenableFuture<?> activator,
            Iterable<Integer> partitionNumbers,
            IntFunction<ListenableFuture<T>> loader,
            IntConsumer disposer)
    {
        requireNonNull(partitionNumbers, "partitionNumbers is null");
        requireNonNull(loader, "loader is null");
        requireNonNull(disposer, "disposer is null");

        ImmutableList.Builder<Partition<T>> partitions = ImmutableList.builder();
        ListenableFuture<?> partitionActivator = activator;
        for (Integer partitionNumber : partitionNumbers) {
            Partition<T> partition = new Partition<>(consumersCount, partitionNumber, loader, partitionActivator, disposer);
            partitions.add(partition);
            partitionActivator = partition.released;
        }
        return partitions.build();
    }

    public int getConsumersCount()
    {
        return consumersCount;
    }

    public Iterator<Partition<T>> beginConsumption()
    {
        Queue<Partition<T>> partitions = new ArrayDeque<>(requireNonNull(this.partitions, "partitions is already null"));
        if (consumed.incrementAndGet() >= consumersCount) {
            // Unreference futures to allow GC
            this.partitions = null;
        }
        return new AbstractIterator<Partition<T>>()
        {
            @Override
            protected Partition<T> computeNext()
            {
                Partition<T> next = partitions.poll();
                if (next != null) {
                    return next;
                }
                else {
                    return endOfData();
                }
            }
        };
    }

    public static class Partition<T>
    {
        private final int partitionNumber;
        private final SettableFuture<?> requested;
        private final ListenableFuture<T> loaded;
        private final SettableFuture<?> released;

        @GuardedBy("this")
        private int pendingReleases;

        public Partition(
                int consumersCount,
                int partitionNumber,
                IntFunction<ListenableFuture<T>> loader,
                ListenableFuture<?> previousReleased,
                IntConsumer disposer)
        {
            this.partitionNumber = partitionNumber;
            this.requested = SettableFuture.create();
            this.loaded = Futures.transformAsync(
                    allAsList(requested, previousReleased),
                    ignored -> loader.apply(partitionNumber),
                    directExecutor());
            this.released = SettableFuture.create();
            released.addListener(() -> disposer.accept(partitionNumber), directExecutor());
            this.pendingReleases = consumersCount;
        }

        public int number()
        {
            return partitionNumber;
        }

        public ListenableFuture<T> load()
        {
            requested.set(null);
            return loaded;
        }

        public synchronized void release()
        {
            checkState(loaded.isDone());
            pendingReleases--;
            checkState(pendingReleases >= 0);
            if (pendingReleases == 0) {
                released.set(null);
            }
        }
    }
}
