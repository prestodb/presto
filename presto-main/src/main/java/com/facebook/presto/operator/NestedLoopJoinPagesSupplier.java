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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static java.util.Objects.requireNonNull;

public final class NestedLoopJoinPagesSupplier
        implements NestedLoopJoinPagesBridge
{
    private final SettableFuture<NestedLoopJoinPages> pagesFuture = SettableFuture.create();
    private final AtomicInteger referenceCount = new AtomicInteger(0);

    @Override
    public ListenableFuture<NestedLoopJoinPages> getPagesFuture()
    {
        return transformAsync(pagesFuture, Futures::immediateFuture);
    }

    @Override
    public void setPages(NestedLoopJoinPages nestedLoopJoinPages)
    {
        requireNonNull(nestedLoopJoinPages, "nestedLoopJoinPages is null");
        boolean wasSet = pagesFuture.set(nestedLoopJoinPages);
        checkState(wasSet, "pagesFuture already set");
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
            // We own the shared pageSource, so we need to free their memory
            addSuccessCallback(pagesFuture, NestedLoopJoinPages::freeMemory);
        }
    }
}
