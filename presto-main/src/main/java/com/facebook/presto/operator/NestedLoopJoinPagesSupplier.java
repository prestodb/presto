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
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static java.util.Objects.requireNonNull;

public final class NestedLoopJoinPagesSupplier
{
    private final List<Type> types;
    private final SettableFuture<NestedLoopJoinPages> pagesFuture = SettableFuture.create();
    private final AtomicInteger referenceCount = new AtomicInteger(0);

    public NestedLoopJoinPagesSupplier(List<Type> types)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public ListenableFuture<NestedLoopJoinPages> getPagesFuture()
    {
        return transformAsync(pagesFuture, Futures::immediateFuture);
    }

    public void setPages(NestedLoopJoinPages nestedLoopJoinPages)
    {
        requireNonNull(nestedLoopJoinPages, "nestedLoopJoinPages is null");
        boolean wasSet = pagesFuture.set(nestedLoopJoinPages);
        checkState(wasSet, "pagesFuture already set");
    }

    public void retain()
    {
        referenceCount.incrementAndGet();
    }

    public void release()
    {
        if (referenceCount.decrementAndGet() == 0) {
            // We own the shared pageSource, so we need to free their memory
            Futures.addCallback(pagesFuture, new FutureCallback<NestedLoopJoinPages>()
            {
                @Override
                public void onSuccess(NestedLoopJoinPages result)
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
