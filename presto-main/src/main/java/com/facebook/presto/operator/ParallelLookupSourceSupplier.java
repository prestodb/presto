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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ParallelLookupSourceSupplier
        implements LookupSourceSupplier
{
    private final List<Type> types;
    private final List<Type> hashChannelTypes;
    private final ListenableFuture<LookupSource> lookupSourceFuture;
    private final List<? extends ListenableFuture<SharedLookupSource>> partitions;
    private final AtomicInteger referenceCount = new AtomicInteger(1);

    public ParallelLookupSourceSupplier(List<Type> types, List<Integer> hashChannels, List<? extends ListenableFuture<SharedLookupSource>> partitions)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.partitions = requireNonNull(partitions, "partitions is null");

        hashChannelTypes = hashChannels.stream()
                .map(types::get)
                .collect(toImmutableList());

        checkArgument(Integer.bitCount(partitions.size()) == 1, "partitions must be a power of 2");
        lookupSourceFuture = Futures.transform(Futures.allAsList(partitions), (List<SharedLookupSource> input) -> {
            return new PartitionedLookupSource(input, hashChannelTypes);
        });
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public ListenableFuture<LookupSource> getLookupSource(OperatorContext operatorContext)
    {
        return lookupSourceFuture;
    }

    @Override
    public void retain()
    {
        referenceCount.incrementAndGet();
    }

    @Override
    public void release()
    {
        if (referenceCount.decrementAndGet() == 0) {
            // We own the shared lookup sources, so we need to free their memory
            for (ListenableFuture<SharedLookupSource> future : partitions) {
                Futures.addCallback(future, new FutureCallback<SharedLookupSource>() {
                    @Override
                    public void onSuccess(SharedLookupSource result)
                    {
                        result.freeMemory();
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        // ignored
                    }
                });
            }
        }
    }
}
