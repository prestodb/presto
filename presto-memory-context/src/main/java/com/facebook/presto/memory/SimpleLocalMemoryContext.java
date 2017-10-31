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
package com.facebook.presto.memory;

import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import static com.facebook.presto.memory.MemoryReservationHandler.NOT_BLOCKED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SimpleLocalMemoryContext
        implements LocalMemoryContext
{
    private final AggregatedMemoryContext parentMemoryContext;
    @GuardedBy("this")
    private long usedBytes;
    @GuardedBy("this")
    private boolean closed;

    public SimpleLocalMemoryContext(AggregatedMemoryContext parentMemoryContext)
    {
        this.parentMemoryContext = requireNonNull(parentMemoryContext, "parentMemoryContext is null");
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

        if (parentMemoryContext.tryUpdateBytes(bytes)) {
            usedBytes += bytes;
            return true;
        }
        return false;
    }

    @Override
    public synchronized void transferOwnership(LocalMemoryContext to)
    {
        checkArgument(to instanceof SimpleLocalMemoryContext, "to has to be SimpleLocalMemoryContext");
        checkState(!closed, "SimpleLocalMemoryContext is already closed");

        SimpleLocalMemoryContext target = (SimpleLocalMemoryContext) to;
        AggregatedMemoryContext parent = parentMemoryContext;
        while (parent != null && parent != target.parentMemoryContext) {
            synchronized (parent) {
                parent.usedBytes -= usedBytes;
            }
            parent = parent.parentMemoryContext;
        }

        // If parent is null at this point, we fail as the memory context state is probably corrupt.
        // Also, there is also a possibility of a deadlock if we continue in that case.
        checkState(parent != null, "memory context state is corrupt");

        synchronized (to) {
            target.usedBytes += usedBytes;
        }

        usedBytes = 0;
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
