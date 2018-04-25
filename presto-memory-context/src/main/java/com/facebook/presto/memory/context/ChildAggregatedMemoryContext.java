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
package com.facebook.presto.memory.context;

import com.google.common.util.concurrent.ListenableFuture;

import static java.util.Objects.requireNonNull;

class ChildAggregatedMemoryContext
        extends AggregatedMemoryContext
{
    private final AggregatedMemoryContext parentMemoryContext;

    ChildAggregatedMemoryContext(AggregatedMemoryContext parentMemoryContext)
    {
        this.parentMemoryContext = requireNonNull(parentMemoryContext, "parentMemoryContext is null");
    }

    synchronized ListenableFuture<?> updateBytes(long bytes)
    {
        // update the parent before updating usedBytes as it may throw a runtime exception (e.g., ExceededMemoryLimitException)
        ListenableFuture<?> future = parentMemoryContext.updateBytes(bytes);
        addBytes(bytes);
        return future;
    }

    synchronized boolean tryUpdateBytes(long delta)
    {
        if (parentMemoryContext.tryUpdateBytes(delta)) {
            addBytes(delta);
            return true;
        }
        return false;
    }

    synchronized AggregatedMemoryContext getParent()
    {
        return parentMemoryContext;
    }

    void closeContext()
    {
        parentMemoryContext.updateBytes(-getBytes());
    }
}
