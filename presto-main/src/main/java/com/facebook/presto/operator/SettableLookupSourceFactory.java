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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.facebook.presto.operator.OuterLookupSource.createOuterLookupSourceSupplier;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class SettableLookupSourceFactory
        implements LookupSourceFactory
{
    private enum State
    {
        NOT_SET, SET, DESTROYED
    }

    private final List<Type> types;
    private final boolean outer;
    private final Map<Symbol, Integer> layout;

    @GuardedBy("this")
    private State state = State.NOT_SET;

    @GuardedBy("this")
    private Supplier<LookupSource> lookupSourceSupplier;

    @GuardedBy("this")
    private final List<SettableFuture<LookupSource>> lookupSourceFutures = new ArrayList<>();

    @GuardedBy("this")
    private Runnable onDestroy;

    public SettableLookupSourceFactory(List<Type> types, Map<Symbol, Integer> layout, boolean outer)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.layout = ImmutableMap.copyOf(requireNonNull(layout, "layout is null"));
        this.outer = outer;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public Map<Symbol, Integer> getLayout()
    {
        return layout;
    }

    @Override
    public synchronized ListenableFuture<LookupSource> createLookupSource()
    {
        if (lookupSourceSupplier != null) {
            return Futures.immediateFuture(lookupSourceSupplier.get());
        }

        SettableFuture<LookupSource> lookupSourceFuture = SettableFuture.create();
        lookupSourceFutures.add(lookupSourceFuture);
        return lookupSourceFuture;
    }

    public void setLookupSourceSupplier(Supplier<LookupSource> lookupSourceSupplier, OperatorContext operatorContext)
    {
        requireNonNull(lookupSourceSupplier, "lookupSourceSupplier is null");
        requireNonNull(operatorContext, "operatorContext is null");

        if (outer) {
            lookupSourceSupplier = createOuterLookupSourceSupplier(lookupSourceSupplier);
        }

        List<SettableFuture<LookupSource>> lookupSourceFutures;
        synchronized (this) {
            if (state == State.DESTROYED) {
                return;
            }

            checkState(state == State.NOT_SET, "Lookup source already set");
            state = State.SET;

            // transfer lookup source memory to task context
            long lookupSourceSizeInBytes = lookupSourceSupplier.get().getInMemorySizeInBytes();
            operatorContext.transferMemoryToTaskContext(lookupSourceSizeInBytes);

            // when all references are released, free the task memory
            TaskContext taskContext = operatorContext.getDriverContext().getPipelineContext().getTaskContext();
            onDestroy = () -> taskContext.freeMemory(lookupSourceSizeInBytes);

            this.lookupSourceSupplier = lookupSourceSupplier;
            lookupSourceFutures = ImmutableList.copyOf(this.lookupSourceFutures);
        }

        for (SettableFuture<LookupSource> lookupSourceFuture : lookupSourceFutures) {
            lookupSourceFuture.set(lookupSourceSupplier.get());
        }
    }

    @Override
    public void destroy()
    {
        Runnable onDestroy;
        synchronized (this) {
            if (state == State.DESTROYED) {
                return;
            }
            state = State.DESTROYED;
            onDestroy = this.onDestroy;
        }

        if (onDestroy != null) {
            onDestroy.run();
        }
    }
}
