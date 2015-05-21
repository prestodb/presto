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
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;

public final class SettableLookupSourceSupplier
        implements LookupSourceSupplier
{
    private final List<Type> types;
    private final SettableFuture<SharedLookupSource> lookupSourceFuture = SettableFuture.create();
    private final AtomicInteger referenceCount = new AtomicInteger(1);

    public SettableLookupSourceSupplier(List<Type> types)
    {
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public ListenableFuture<LookupSource> getLookupSource(OperatorContext operatorContext)
    {
        return transform(lookupSourceFuture, (AsyncFunction<SharedLookupSource, LookupSource>) Futures::immediateFuture);
    }

    public void setLookupSource(SharedLookupSource lookupSource)
    {
        checkNotNull(lookupSource, "lookupSource is null");
        boolean wasSet = lookupSourceFuture.set(lookupSource);
        checkState(wasSet, "Lookup source already set");
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
            // We own the shared lookup source, so we need to free their memory
            Futures.addCallback(lookupSourceFuture, new FutureCallback<SharedLookupSource>() {
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
