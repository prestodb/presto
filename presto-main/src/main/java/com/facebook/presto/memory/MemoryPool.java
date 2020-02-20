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
import com.facebook.presto.spi.memory.MemoryAllocation;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.weakref.jmx.Managed;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.presto.memory.context.AbstractAggregatedMemoryContext.FORCE_FREE_TAG;
import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class MemoryPool
{
    private static final String MOVE_QUERY_TAG = "MOVE_QUERY_OPERATION";

    private final MemoryPoolId id;
    private final long maxBytes;

    private volatile long reservedBytes;
    private volatile long reservedRevocableBytes;

    @Nullable
    private NonCancellableMemoryFuture<?> future;

    // TODO: It would be better if we just tracked QueryContexts, but their lifecycle is managed by a weak reference, so we can't do that
    private final Map<QueryId, Long> queryMemoryReservations = new ConcurrentHashMap<>();

    // This map keeps track of all the tagged allocations, e.g., query-1 -> ['TableScanOperator': 10MB, 'LazyOutputBuffer': 5MB, ...]
    private final Map<QueryId, Map<String, Long>> taggedMemoryAllocations = new ConcurrentHashMap<>();

    private final Map<QueryId, Long> queryMemoryRevocableReservations = new ConcurrentHashMap<>();

    private final List<MemoryPoolListener> listeners = new CopyOnWriteArrayList<>();

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

    public MemoryPoolInfo getInfo()
    {
        Map<QueryId, List<MemoryAllocation>> memoryAllocations = new HashMap<>();
        for (Entry<QueryId, Map<String, Long>> entry : taggedMemoryAllocations.entrySet()) {
            List<MemoryAllocation> allocations = new ArrayList<>();
            if (entry.getValue() != null) {
                entry.getValue().forEach((tag, allocation) -> allocations.add(new MemoryAllocation(tag, allocation)));
            }
            memoryAllocations.put(entry.getKey(), allocations);
        }
        return new MemoryPoolInfo(maxBytes, reservedBytes, reservedRevocableBytes, queryMemoryReservations, memoryAllocations, queryMemoryRevocableReservations);
    }

    public void addListener(MemoryPoolListener listener)
    {
        listeners.add(requireNonNull(listener, "listener cannot be null"));
    }

    public void removeListener(MemoryPoolListener listener)
    {
        listeners.remove(requireNonNull(listener, "listener cannot be null"));
    }

    /**
     * Reserves the given number of bytes. Caller should wait on the returned future, before allocating more memory.
     */
    public ListenableFuture<?> reserve(QueryId queryId, String allocationTag, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        ListenableFuture<?> result;
        if (bytes != 0) {
            while (true) {
                if (queryMemoryReservations.computeIfPresent(queryId, (ignored, value) -> value + bytes) != null) {
                    // queryMemoryReservations is updated,
                    // use flag putQueryIdAbsent = false to prevent adding new queryId to taggedMemoryAllocations
                    // in race condition when queryId has been removed
                    updateTaggedMemoryAllocations(queryId, allocationTag, bytes, false);
                    break;
                }
                synchronized (this) {
                    if (queryMemoryReservations.putIfAbsent(queryId, bytes) == null) {
                        // queryId is successfully put into queryMemoryReservations,
                        // safe to update taggedMemoryAllocations
                        updateTaggedMemoryAllocations(queryId, allocationTag, bytes, true);
                        break;
                    }
                }
            }
        }
        synchronized (this) {
            reservedBytes += bytes;
            if (getFreeBytes() <= 0) {
                if (future == null) {
                    future = NonCancellableMemoryFuture.create();
                }
                checkState(!future.isDone(), "future is already completed");
                result = future;
            }
            else {
                result = NOT_BLOCKED;
            }
        }

        onMemoryReserved();
        return result;
    }

    private void onMemoryReserved()
    {
        listeners.forEach(listener -> listener.onMemoryReserved(this));
    }

    public ListenableFuture<?> reserveRevocable(QueryId queryId, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        ListenableFuture<?> result;

        if (bytes != 0) {
            queryMemoryRevocableReservations.merge(queryId, bytes, Long::sum);
        }
        synchronized (this) {
            reservedRevocableBytes += bytes;
            if (getFreeBytes() <= 0) {
                if (future == null) {
                    future = NonCancellableMemoryFuture.create();
                }
                checkState(!future.isDone(), "future is already completed");
                result = future;
            }
            else {
                result = NOT_BLOCKED;
            }
        }

        onMemoryReserved();
        return result;
    }

    /**
     * Try to reserve the given number of bytes. Return value indicates whether the caller may use the requested memory.
     */
    public boolean tryReserve(QueryId queryId, String allocationTag, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        synchronized (this) {
            if (getFreeBytes() - bytes < 0) {
                return false;
            }
            reservedBytes += bytes;
        }
        if (bytes != 0) {
            while (true) {
                if (queryMemoryReservations.computeIfPresent(queryId, (ignored, value) -> value + bytes) != null) {
                    // queryMemoryReservations is updated,
                    // use flag putQueryIdAbsent = false to prevent adding new queryId to taggedMemoryAllocations
                    // in race condition when queryId has been removed
                    updateTaggedMemoryAllocations(queryId, allocationTag, bytes, false);
                    break;
                }
                synchronized (this) {
                    if (queryMemoryReservations.putIfAbsent(queryId, bytes) == null) {
                        // queryId is successfully put into queryMemoryReservations,
                        // safe to update taggedMemoryAllocations
                        updateTaggedMemoryAllocations(queryId, allocationTag, bytes, true);
                        break;
                    }
                }
            }
        }

        onMemoryReserved();
        return true;
    }

    public void free(QueryId queryId, String allocationTag, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(reservedBytes >= bytes, "tried to free more memory than is reserved");
        if (bytes == 0) {
            // Freeing zero bytes is a no-op
            return;
        }

        Long queryReservation = queryMemoryReservations.merge(queryId, -bytes, Long::sum);
        checkArgument(queryReservation != -bytes, "queryId does not exist in queryMemoryReservations");
        checkArgument(queryReservation >= 0, "tried to free more memory than is reserved by query");
        if (queryReservation == 0) {
            synchronized (this) {
                if (queryMemoryReservations.remove(queryId, queryReservation)) {
                    // queryId is successfully removed from queryMemoryReservations,
                    // safe to update taggedMemoryAllocations
                    taggedMemoryAllocations.remove(queryId);
                }
            }
        }
        else {
            updateTaggedMemoryAllocations(queryId, allocationTag, -bytes, false);
        }

        synchronized (this) {
            reservedBytes -= bytes;
            if (getFreeBytes() > 0 && future != null) {
                future.set(null);
                future = null;
            }
        }
    }

    public void freeRevocable(QueryId queryId, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(reservedRevocableBytes >= bytes, "tried to free more revocable memory than is reserved");
        if (bytes == 0) {
            // Freeing zero bytes is a no-op
            return;
        }

        Long queryReservation = queryMemoryRevocableReservations.merge(queryId, -bytes, Long::sum);
        checkArgument(queryReservation != -bytes, "queryId does not exist in queryMemoryRevocableReservations");
        checkArgument(queryReservation >= 0, "tried to free more revocable memory than is reserved by query");
        if (queryReservation == 0) {
            queryMemoryRevocableReservations.remove(queryId, queryReservation);
        }

        synchronized (this) {
            reservedRevocableBytes -= bytes;
            if (getFreeBytes() > 0 && future != null) {
                future.set(null);
                future = null;
            }
        }
    }

    // When this method returns the MOVE_QUERY_TAG won't be visible in the tagged memory allocations map.
    // Because, we remove the tagged allocations from this MemoryPool for queryId, and then we reserve
    // N bytes with MOVE_QUERY_TAG in the targetMemoryPool, and then immediately overwrite it
    // with a put() call.
    synchronized ListenableFuture<?> moveQuery(QueryId queryId, MemoryPool targetMemoryPool)
    {
        long originalReserved = getQueryMemoryReservation(queryId);
        long originalRevocableReserved = getQueryRevocableMemoryReservation(queryId);
        // Get the tags before we call free() as that would remove the tags and we will lose the tags.
        Map<String, Long> taggedAllocations = taggedMemoryAllocations.remove(queryId);
        ListenableFuture<?> future = targetMemoryPool.reserve(queryId, MOVE_QUERY_TAG, originalReserved);
        free(queryId, MOVE_QUERY_TAG, originalReserved);
        targetMemoryPool.reserveRevocable(queryId, originalRevocableReserved);
        freeRevocable(queryId, originalRevocableReserved);
        if (taggedAllocations != null) {
            targetMemoryPool.taggedMemoryAllocations.put(queryId, taggedAllocations);
        }
        return future;
    }

    /**
     * Returns the number of free bytes. This value may be negative, which indicates that the pool is over-committed.
     */
    @Managed
    public long getFreeBytes()
    {
        return maxBytes - reservedBytes - reservedRevocableBytes;
    }

    @Managed
    public long getMaxBytes()
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

    long getQueryMemoryReservation(QueryId queryId)
    {
        return queryMemoryReservations.getOrDefault(queryId, 0L);
    }

    long getQueryRevocableMemoryReservation(QueryId queryId)
    {
        return queryMemoryRevocableReservations.getOrDefault(queryId, 0L);
    }

    @Override
    public String toString()
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

    private static class NonCancellableMemoryFuture<V>
            extends AbstractFuture<V>
    {
        public static <V> NonCancellableMemoryFuture<V> create()
        {
            return new NonCancellableMemoryFuture<>();
        }

        @Override
        public boolean set(@Nullable V value)
        {
            return super.set(value);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            throw new UnsupportedOperationException("cancellation is not supported");
        }
    }

    private void updateTaggedMemoryAllocations(QueryId queryId, String allocationTag, long delta, boolean putQueryIdIfAbsent)
    {
        if (delta == 0) {
            return;
        }

        Map<String, Long> allocations;
        if (putQueryIdIfAbsent) {
            allocations = taggedMemoryAllocations.computeIfAbsent(queryId, ignored -> new ConcurrentHashMap<>());
        }
        else {
            allocations = taggedMemoryAllocations.get(queryId);
            if (allocations == null) {
                return;
            }
        }
        allocations.compute(allocationTag, (ignored, oldValue) -> {
            if (oldValue == null) {
                return delta;
            }
            long newValue = oldValue.longValue() + delta;
            if (newValue == 0) {
                return null;
            }
            return newValue;
        });
    }

    @VisibleForTesting
    Map<QueryId, Map<String, Long>> getTaggedMemoryAllocations()
    {
        return taggedMemoryAllocations.keySet().stream()
                .collect(toImmutableMap(identity(), this::getTaggedMemoryAllocations));
    }

    @VisibleForTesting
    Map<String, Long> getTaggedMemoryAllocations(QueryId targetQueryId)
    {
        if (taggedMemoryAllocations.get(targetQueryId) == null) {
            return null;
        }
        return taggedMemoryAllocations.get(targetQueryId)
                .entrySet().stream()
                .filter(entry -> !entry.getKey().equals(FORCE_FREE_TAG))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
