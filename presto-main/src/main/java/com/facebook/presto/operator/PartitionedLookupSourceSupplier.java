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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.MoreFutures;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class PartitionedLookupSourceSupplier
        implements LookupSourceSupplier, Closeable
{
    private final List<Type> types;
    private final Map<Symbol, Integer> layout;
    private final List<Type> hashChannelTypes;
    private final SettableFuture<LookupSource> lookupSourceFuture = SettableFuture.create();
    private final LookupSource[] partitions;
    private final boolean outer;
    private final CompletableFuture<?> destroyed = new CompletableFuture<>();

    @GuardedBy("this")
    private int partitionsSet;

    public PartitionedLookupSourceSupplier(List<Type> types, List<Integer> hashChannels, int partitionCount, Map<Symbol, Integer> layout, boolean outer)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.layout = ImmutableMap.copyOf(layout);
        this.partitions = new LookupSource[partitionCount];
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
    public Map<Symbol, Integer> getLayout()
    {
        return layout;
    }

    @Override
    public ListenableFuture<LookupSource> getLookupSource()
    {
        return lookupSourceFuture;
    }

    public void setLookupSource(int partitionIndex, LookupSource lookupSource)
    {
        PartitionedLookupSource partitionedLookupSource = null;
        synchronized (this) {
            requireNonNull(lookupSource, "lookupSource is null");

            if (destroyed.isDone()) {
                return;
            }

            checkState(partitions[partitionIndex] == null, "Partition already set");
            partitions[partitionIndex] = lookupSource;
            partitionsSet++;

            if (partitionsSet == partitions.length) {
                partitionedLookupSource = new PartitionedLookupSource(ImmutableList.copyOf(partitions), hashChannelTypes, outer);
            }
        }

        if (partitionedLookupSource != null) {
            lookupSourceFuture.set(partitionedLookupSource);
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

    @Override
    public void close()
    {
    }
}
