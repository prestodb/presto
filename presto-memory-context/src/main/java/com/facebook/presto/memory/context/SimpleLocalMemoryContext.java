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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import static com.facebook.presto.memory.context.AbstractAggregatedMemoryContext.addExact;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class SimpleLocalMemoryContext
        implements LocalMemoryContext
{
    private static final ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);

    private final AbstractAggregatedMemoryContext parentMemoryContext;
    @GuardedBy("this")
    private long usedBytes;
    @GuardedBy("this")
    private boolean closed;

    public SimpleLocalMemoryContext(AggregatedMemoryContext parentMemoryContext)
    {
        verify(parentMemoryContext instanceof AbstractAggregatedMemoryContext);
        this.parentMemoryContext = (AbstractAggregatedMemoryContext) requireNonNull(parentMemoryContext, "parentMemoryContext is null");
    }

    @Override
    public synchronized long getBytes()
    {
        return usedBytes;
    }

    @Override
    public synchronized ListenableFuture<?> setBytes(long bytes)
    {
        checkState(!closed, "SimpleLocalMemoryContext is already closed");
        checkArgument(bytes >= 0, "bytes cannot be negative");

        if (bytes == usedBytes) {
            return NOT_BLOCKED;
        }

        // update the parent first as it may throw a runtime exception (e.g., ExceededMemoryLimitException)
        ListenableFuture<?> future = parentMemoryContext.updateBytes(bytes - usedBytes);
        usedBytes = bytes;
        return future;
    }

    @Override
    public synchronized boolean trySetBytes(long bytes)
    {
        checkState(!closed, "SimpleLocalMemoryContext is already closed");
        checkArgument(bytes >= 0, "bytes cannot be negative");
        long delta = bytes - usedBytes;
        if (parentMemoryContext.tryUpdateBytes(delta)) {
            usedBytes = bytes;
            return true;
        }
        return false;
    }

    /**
     * This method transfers the allocations from this memory context to the "to" memory context,
     * where parent of this is a descendant of to.parent (there can be multiple AggregatedMemoryContexts between them).
     * <p>
     * During the transfer the implementation of this method must not reflect any state changes outside of the contexts
     * (e.g., by calling the reservation handlers).
     * <p>
     * This method currently has a single use ({@code NestedLoopJoinPages}) and any change should be tested carefully due to
     * its somewhat complex semantics.
     */
    @Deprecated
    @Override
    public synchronized void transferMemory(LocalMemoryContext to)
    {
        checkArgument(to instanceof SimpleLocalMemoryContext, "to must be an instance of SimpleLocalMemoryContext");
        checkState(!closed, "already closed");

        SimpleLocalMemoryContext target = (SimpleLocalMemoryContext) to;
        checkMemoryContextState(target);

        AbstractAggregatedMemoryContext parent = parentMemoryContext;
        while (parent != null && parent != target.parentMemoryContext) {
            parent.addBytes(-usedBytes);
            parent = parent.getParent();
        }
        target.addBytes(usedBytes);
        usedBytes = 0;
    }

    private void checkMemoryContextState(SimpleLocalMemoryContext target)
    {
        AbstractAggregatedMemoryContext parent = parentMemoryContext;
        while (parent != null && parent != target.parentMemoryContext) {
            parent = parent.getParent();
        }
        // if parent is null at this point, we fail as the memory context state is probably corrupt.
        checkState(parent != null, "memory context state is corrupt");
    }

    private synchronized void addBytes(long bytes)
    {
        usedBytes = addExact(usedBytes, bytes);
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        parentMemoryContext.updateBytes(-usedBytes);
        usedBytes = 0;
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("usedBytes", usedBytes)
                .toString();
    }
}
