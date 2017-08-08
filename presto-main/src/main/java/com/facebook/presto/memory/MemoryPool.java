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

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import org.weakref.jmx.Managed;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MemoryPool
{
    private final MemoryPoolId id;
    private final long maxBytes;

    @GuardedBy("this")
    private long reservedBytes;
    @GuardedBy("this")
    private long reservedRevocableBytes;

    @Nullable
    @GuardedBy("this")
    private SettableFuture<?> future;

    @GuardedBy("this")
    // TODO: It would be better if we just tracked QueryContexts, but their lifecycle is managed by a weak reference, so we can't do that
    private final Map<QueryId, Long> queryMemoryReservations = new HashMap<>();
    @GuardedBy("this")
    private final Map<QueryId, Long> queryMemoryRevocableReservations = new HashMap<>();

    public MemoryPool(MemoryPoolId id, DataSize size)
    {
        this.id = requireNonNull(id, "name is null");
        requireNonNull(size, "size is null");
        maxBytes = size.toBytes();
    }

    public MemoryPoolId getId()
    {
        return id;
    }

    public synchronized MemoryPoolInfo getInfo()
    {
        return new MemoryPoolInfo(maxBytes, reservedBytes, reservedRevocableBytes, queryMemoryReservations, queryMemoryRevocableReservations);
    }

    /**
     * Reserves the given number of bytes. Caller should wait on the returned future, before allocating more memory.
     */
    public synchronized ListenableFuture<?> reserve(QueryId queryId, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        if (bytes != 0) {
            queryMemoryReservations.merge(queryId, bytes, Long::sum);
        }
        reservedBytes += bytes;
        if (getFreeBytes() <= 0) {
            if (future == null) {
                future = SettableFuture.create();
            }
            checkState(!future.isDone(), "future is already completed");
            return future;
        }
        return NOT_BLOCKED;
    }

    public synchronized ListenableFuture<?> reserveRevocable(QueryId queryId, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        if (bytes != 0) {
            queryMemoryRevocableReservations.merge(queryId, bytes, Long::sum);
        }
        reservedRevocableBytes += bytes;
        if (getFreeBytes() <= 0) {
            if (future == null) {
                future = SettableFuture.create();
            }
            checkState(!future.isDone(), "future is already completed");
            return future;
        }
        return NOT_BLOCKED;
    }

    /**
     * Try to reserve the given number of bytes. Return value indicates whether the caller may use the requested memory.
     */
    public synchronized boolean tryReserve(QueryId queryId, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        if (getFreeBytes() - bytes < 0) {
            return false;
        }
        reservedBytes += bytes;
        if (bytes != 0) {
            queryMemoryReservations.merge(queryId, bytes, Long::sum);
        }
        return true;
    }

    public synchronized void free(QueryId queryId, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(reservedBytes >= bytes, "tried to free more memory than is reserved");
        if (bytes == 0) {
            // Freeing zero bytes is a no-op
            return;
        }

        Long queryReservation = queryMemoryReservations.get(queryId);
        requireNonNull(queryReservation, "queryReservation is null");
        checkArgument(queryReservation - bytes >= 0, "tried to free more memory than is reserved by query");
        queryReservation -= bytes;
        if (queryReservation == 0) {
            queryMemoryReservations.remove(queryId);
        }
        else {
            queryMemoryReservations.put(queryId, queryReservation);
        }
        reservedBytes -= bytes;
        if (getFreeBytes() > 0 && future != null) {
            future.set(null);
            future = null;
        }
    }

    public synchronized void freeRevocable(QueryId queryId, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(reservedRevocableBytes >= bytes, "tried to free more revocable memory than is reserved");
        if (bytes == 0) {
            // Freeing zero bytes is a no-op
            return;
        }

        Long queryReservation = queryMemoryRevocableReservations.get(queryId);
        requireNonNull(queryReservation, "queryReservation is null");
        checkArgument(queryReservation - bytes >= 0, "tried to free more revocable memory than is reserved by query");
        queryReservation -= bytes;
        if (queryReservation == 0) {
            queryMemoryRevocableReservations.remove(queryId);
        }
        else {
            queryMemoryRevocableReservations.put(queryId, queryReservation);
        }
        reservedRevocableBytes -= bytes;
        if (getFreeBytes() > 0 && future != null) {
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
        return maxBytes - reservedBytes - reservedRevocableBytes;
    }

    @Managed
    public synchronized long getMaxBytes()
    {
        return maxBytes;
    }

    @Managed
    public long getReservedBytes()
    {
        return reservedBytes;
    }

    @Managed
    public long getReservedRevocableBytes()
    {
        return reservedRevocableBytes;
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("maxBytes", maxBytes)
                .add("freeBytes", getFreeBytes())
                .add("reservedBytes", reservedBytes)
                .add("reservedRevocableBytes", reservedRevocableBytes)
                .add("future", future)
                .toString();
    }
}
