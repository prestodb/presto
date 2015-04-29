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

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import org.weakref.jmx.Managed;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MemoryPool
{
    private final MemoryPoolId id;
    private final long maxBytes;
    private final boolean enableBlocking;

    @GuardedBy("this")
    private long freeBytes;

    @Nullable
    @GuardedBy("this")
    private SettableFuture<?> future;

    public MemoryPool(MemoryPoolId id, DataSize size, boolean enableBlocking)
    {
        this.id = requireNonNull(id, "name is null");
        requireNonNull(size, "size is null");
        maxBytes = size.toBytes();
        this.enableBlocking = enableBlocking;
        freeBytes = size.toBytes();
    }

    public MemoryPoolId getId()
    {
        return id;
    }

    public synchronized MemoryPoolInfo getInfo()
    {
        return new MemoryPoolInfo(maxBytes, freeBytes);
    }

    /**
     * Reserves the given number of bytes. Caller should wait on the returned future, before allocating more memory.
     */
    public synchronized ListenableFuture<?> reserve(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        freeBytes -= bytes;
        if (freeBytes <= 0) {
            if (future == null) {
                future = SettableFuture.create();
            }
            checkState(!future.isDone(), "future is already completed");
            if (enableBlocking) {
                return future;
            }
        }
        return NOT_BLOCKED;
    }

    /**
     * Try to reserve the given number of bytes. Return value indicates whether the caller may use the requested memory.
     */
    public synchronized boolean tryReserve(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        if (freeBytes - bytes < 0) {
            return false;
        }
        freeBytes -= bytes;
        return true;
    }

    public synchronized void free(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(freeBytes + bytes <= maxBytes, "tried to free more memory than is reserved");
        freeBytes += bytes;
        if (freeBytes > 0 && future != null) {
            future.set(null);
            future = null;
        }
    }

    /**
     * Returns the number of free bytes. This value may be negative, which indicates that the pool is over-committed.
     */
    @Managed
    public synchronized long getFreeBytes()
    {
        return freeBytes;
    }

    @Managed
    public synchronized long getMaxBytes()
    {
        return maxBytes;
    }

    @Override
    public synchronized String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("maxBytes", maxBytes)
                .add("enableBlocking", enableBlocking)
                .add("freeBytes", freeBytes)
                .add("future", future)
                .toString();
    }
}
