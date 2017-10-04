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
 * allocations will be visible in the reservedAggregateMemoryContext.
 *
 * To perform local allocations clients should call localMemoryContext()
 * and get a reference to the local memory context. The other child-originated allocations will go through
 * reserveMemory() and reserveRevocableMemory() methods.
 */
@ThreadSafe
public class MemoryTrackingContext
{
    private final AggregatedMemoryContext reservedAggregateMemoryContext;
    private final AggregatedMemoryContext revocableAggregateMemoryContext;

    private final LocalMemoryContext reservedLocalMemoryContext;
    private final LocalMemoryContext revocableLocalMemoryContext;

    public MemoryTrackingContext(AggregatedMemoryContext reservedAggregateMemoryContext, AggregatedMemoryContext revocableAggregateMemoryContext)
    {
        this.reservedAggregateMemoryContext = requireNonNull(reservedAggregateMemoryContext, "reservedAggregateMemoryContext is null");
        this.revocableAggregateMemoryContext = requireNonNull(revocableAggregateMemoryContext, "revocableAggregateMemoryContext is null");
        this.reservedLocalMemoryContext = reservedAggregateMemoryContext.newLocalMemoryContext();
        this.revocableLocalMemoryContext = revocableAggregateMemoryContext.newLocalMemoryContext();
    }

    // below methods are for reserving memory locally
    public void reserveMemory(long delta)
    {
        checkArgument(delta >= 0, "delta is negative");
        reservedLocalMemoryContext.addBytes(delta);
    }

    public void reserveRevocableMemory(long delta)
    {
        checkArgument(delta >= 0, "delta is negative");
        revocableLocalMemoryContext.addBytes(delta);
    }

    // "free" methods always free from the local context, which is reflected to the aggregate context
    public void freeMemory(long delta)
    {
        checkArgument(delta >= 0, "delta is negative");
        checkArgument(delta <= reservedLocalMemoryContext.getBytes(), "cannot free more memory than reserved");
        reservedLocalMemoryContext.addBytes(-delta);
    }

    // frees all the allocations whether they are done through reservedLocalMemoryContext or
    // through the new local memory contexts created through the operator context (e.g., by the spill logic).
    public void forceFreeMemory()
    {
        reservedLocalMemoryContext.setBytes(0);
        long aggregatedReserved = reservedAggregateMemoryContext.getBytes();
        reservedAggregateMemoryContext.addBytes(-aggregatedReserved);
    }

    public void freeRevocableMemory(long delta)
    {
        checkArgument(delta >= 0, "delta is negative");
        checkArgument(delta <= revocableLocalMemoryContext.getBytes(), "cannot free more memory than reserved");
        revocableLocalMemoryContext.addBytes(-delta);
    }

    public LocalMemoryContext localMemoryContext()
    {
        return reservedLocalMemoryContext;
    }

    public LocalMemoryContext newLocalMemoryContext()
    {
        return reservedAggregateMemoryContext.newLocalMemoryContext();
    }

    // below methods are for getting the aggregate reserved memory
    public long reservedMemory()
    {
        return reservedAggregateMemoryContext.getBytes();
    }

    public long reservedRevocableMemory()
    {
        return revocableAggregateMemoryContext.getBytes();
    }

    // below methods are for getting the locally reserved memory
    public long reservedLocalMemory()
    {
        return reservedLocalMemoryContext.getBytes();
    }

    public MemoryTrackingContext newMemoryTrackingContext()
    {
        return new MemoryTrackingContext(reservedAggregateMemoryContext.newAggregatedMemoryContext(), revocableAggregateMemoryContext.newAggregatedMemoryContext());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("reservedAggregateMemoryContext", reservedAggregateMemoryContext)
                .add("revocableAggregateMemoryContext", revocableAggregateMemoryContext)
                .add("reservedLocalMemoryContext", reservedLocalMemoryContext)
                .add("revocableLocalMemoryContext", revocableLocalMemoryContext)
                .toString();
    }
}
