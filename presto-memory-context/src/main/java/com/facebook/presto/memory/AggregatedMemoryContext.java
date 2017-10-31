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

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.OptionalLong;

import static com.facebook.presto.memory.MemoryReservationHandler.NOT_BLOCKED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class AggregatedMemoryContext
{
    @Nullable
    protected final AggregatedMemoryContext parentMemoryContext;
    @Nullable
    private final MemoryReservationHandler reservationHandler;
    @GuardedBy("this")
    protected long usedBytes;
    @GuardedBy("this")
    private boolean closed;
    private final OptionalLong guaranteedMemory;

    public AggregatedMemoryContext()
    {
        this(null, OptionalLong.empty());
    }

    public AggregatedMemoryContext(MemoryReservationHandler reservationHandler, OptionalLong guaranteedMemoryInBytes)
    {
        this(null, reservationHandler, guaranteedMemoryInBytes);
    }

    private AggregatedMemoryContext(AggregatedMemoryContext parentMemoryContext, MemoryReservationHandler reservationHandler, OptionalLong guaranteedMemory)
    {
        checkArgument(reservationHandler == null || parentMemoryContext == null, "at least one of reservationHandler and parentMemoryContext must be null");
        this.parentMemoryContext = parentMemoryContext;
        this.reservationHandler = reservationHandler;
        this.guaranteedMemory = requireNonNull(guaranteedMemory, "guaranteedMemory is null");
    }

    public AggregatedMemoryContext newAggregatedMemoryContext()
    {
        return new AggregatedMemoryContext(this, null, OptionalLong.empty());
    }

    public LocalMemoryContext newLocalMemoryContext()
    {
        return new SimpleLocalMemoryContext(this);
    }

    public synchronized long getBytes()
    {
        return usedBytes;
    }

    synchronized ListenableFuture<?> updateBytes(long bytes)
    {
        ListenableFuture<?> future = NOT_BLOCKED;
        // delegate to parent if exists
        if (parentMemoryContext != null) {
            // update the parent before updating usedBytes as it may throw a runtime exception (e.g., ExceededMemoryLimitException)
            future = parentMemoryContext.updateBytes(bytes);
            usedBytes += bytes;
            return future;
        }

        // At this point this is root
        if (reservationHandler != null) {
            future = reservationHandler.reserveMemory(bytes);
        }

        usedBytes += bytes;

        // If guaranteedMemory is set, make sure we never block queries below that threshold
        if (guaranteedMemory.isPresent() && usedBytes < guaranteedMemory.getAsLong()) {
            future = NOT_BLOCKED;
        }

        return future;
    }

    synchronized boolean tryUpdateBytes(long bytes)
    {
        // delegate to parent if exists
        if (parentMemoryContext != null) {
            if (parentMemoryContext.tryUpdateBytes(bytes)) {
                usedBytes += bytes;
                return true;
            }
            return false;
        }
        // At this point this is root
        if (reservationHandler != null) {
            if (reservationHandler.tryReserveMemory(bytes)) {
                usedBytes += bytes;
                return true;
            }
            return false;
        }
        // both reservationHandler and parentMemoryContext set to null
        usedBytes += bytes;
        return true;
    }

    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        // delegate to parent if exists
        if (parentMemoryContext != null) {
            parentMemoryContext.updateBytes(-usedBytes);
            usedBytes = 0;
            return;
        }
        // At this point this is root
        if (reservationHandler != null) {
            reservationHandler.reserveMemory(-usedBytes);
        }
        usedBytes = 0;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("usedBytes", usedBytes)
                .add("closed", closed)
                .toString();
    }
}
