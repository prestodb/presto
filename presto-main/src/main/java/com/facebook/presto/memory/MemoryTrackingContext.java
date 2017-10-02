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

import com.facebook.presto.spi.memory.AggregatedMemoryContext;
import com.facebook.presto.spi.memory.LocalMemoryContext;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class is used to track memory usage at all levels (operator, driver, pipeline, etc.).
 *
 * At every level we have three aggregate and three local memory contexts. The local memory contexts
 * track the allocations in the current level while the aggregate memory contexts aggregate the memory
 * allocated by the children levels and the current level.
 *
 * The reason we have local memory contexts at every level is that not all the
 * allocations are done by the children levels (e.g., at the pipeline level exchange clients
 * can do local allocations directly, see the ExchangeOperator, another example is the buffers
 * doing local allocations at the task context level, etc.).
 *
 * As another example, at the pipeline level there will be local allocations initiated by the operator context
 * and there will be local allocations initiated by the exchange clients. All these local
 * allocations will be visible in the userReservedAggregateMemoryContext.
 *
 * To perform local allocations clients should call localUserMemoryContext()
 * and get a reference to the local memory context. The other child-originated allocations will go through
 * reserveUserMemory() and reserveRevocableMemory() methods.
 */
@ThreadSafe
public class MemoryTrackingContext
{
    private final AggregatedMemoryContext userReservedAggregateMemoryContext;
    private final AggregatedMemoryContext userRevocableAggregateMemoryContext;

    private final LocalMemoryContext userReservedLocalMemoryContext;
    private final LocalMemoryContext userRevocableLocalMemoryContext;

    public MemoryTrackingContext(AggregatedMemoryContext userReservedAggregateMemoryContext, AggregatedMemoryContext userRevocableAggregateMemoryContext)
    {
        this.userReservedAggregateMemoryContext = requireNonNull(userReservedAggregateMemoryContext, "userReservedAggregateMemoryContext is null");
        this.userRevocableAggregateMemoryContext = requireNonNull(userRevocableAggregateMemoryContext, "userRevocableAggregateMemoryContext is null");
        this.userReservedLocalMemoryContext = userReservedAggregateMemoryContext.newLocalMemoryContext();
        this.userRevocableLocalMemoryContext = userRevocableAggregateMemoryContext.newLocalMemoryContext();
    }

    // below methods are for reserving memory locally
    public void reserveUserMemory(long delta)
    {
        checkArgument(delta >= 0, "delta is negative");
        userReservedLocalMemoryContext.addBytes(delta);
    }

    public void reserveRevocableMemory(long delta)
    {
        checkArgument(delta >= 0, "delta is negative");
        userRevocableLocalMemoryContext.addBytes(delta);
    }

    // "free" methods always free from the local context, which is reflected to the aggregate context
    public void freeUserMemory(long delta)
    {
        checkArgument(delta >= 0, "delta is negative");
        checkArgument(delta <= userReservedLocalMemoryContext.getBytes(), "cannot free more memory than reserved");
        userReservedLocalMemoryContext.addBytes(-delta);
    }

    // frees all the allocations whether they are done through userReservedLocalMemoryContext or
    // through the new local memory contexts created through the operator context (e.g., by the spill logic).
    public void forceFreeUserMemory()
    {
        long localReservation = userReservedLocalMemoryContext.getBytes();
        long aggregatedReserved = userRevocableAggregateMemoryContext.getBytes();
        userRevocableAggregateMemoryContext.addBytes(-aggregatedReserved);
        userReservedLocalMemoryContext.addBytes(-localReservation);
    }

    public void freeRevocableMemory(long delta)
    {
        checkArgument(delta >= 0, "delta is negative");
        checkArgument(delta <= userRevocableLocalMemoryContext.getBytes(), "cannot free more memory than reserved");
        userRevocableLocalMemoryContext.addBytes(-delta);
    }

    public LocalMemoryContext localUserMemoryContext()
    {
        return userReservedLocalMemoryContext;
    }

    public LocalMemoryContext newLocalMemoryContext()
    {
        return userReservedAggregateMemoryContext.newLocalMemoryContext();
    }

    // below methods are for getting the aggregate reserved memory
    public long reservedUserMemory()
    {
        return userReservedAggregateMemoryContext.getBytes();
    }

    public long reservedRevocableMemory()
    {
        return userRevocableAggregateMemoryContext.getBytes();
    }

    // below methods are for getting the locally reserved memory
    public long reservedLocalUserMemory()
    {
        return userReservedLocalMemoryContext.getBytes();
    }

    public MemoryTrackingContext newMemoryTrackingContext()
    {
        return new MemoryTrackingContext(userReservedAggregateMemoryContext.newAggregatedMemoryContext(), userRevocableAggregateMemoryContext.newAggregatedMemoryContext());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("userReservedAggregateMemoryContext", userReservedAggregateMemoryContext)
                .add("userRevocableAggregateMemoryContext", userRevocableAggregateMemoryContext)
                .add("userReservedLocalMemoryContext", userReservedLocalMemoryContext)
                .add("userRevocableLocalMemoryContext", userRevocableLocalMemoryContext)
                .toString();
    }
}
