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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class SettableLookupSourceSupplier
        implements LookupSourceSupplier
{
    private enum State
    {
        NOT_SET, SET, DESTROYED
    }

    private final List<Type> types;
    private final boolean outer;
    private final SettableFuture<LookupSource> lookupSourceFuture = SettableFuture.create();
    private final Map<Symbol, Integer> layout;

    @GuardedBy("this")
    private State state = State.NOT_SET;

    @GuardedBy("this")
    private Runnable onDestroy;

    public SettableLookupSourceSupplier(List<Type> types, Map<Symbol, Integer> layout, boolean outer)
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
    public ListenableFuture<LookupSource> getLookupSource()
    {
        return lookupSourceFuture;
    }

    public void setLookupSource(LookupSource lookupSource, OperatorContext operatorContext)
    {
        if (outer) {
            lookupSource = new OuterLookupSource(lookupSource);
        }

        synchronized (this) {
            requireNonNull(lookupSource, "lookupSource is null");
            requireNonNull(operatorContext, "operatorContext is null");

            if (state == State.DESTROYED) {
                return;
            }

            checkState(state == State.NOT_SET, "Lookup source already set");
            state = State.SET;

            // transfer lookup source memory to task context
            long lookupSourceSizeInBytes = lookupSource.getInMemorySizeInBytes();
            operatorContext.transferMemoryToTaskContext(lookupSourceSizeInBytes);

            // when all references are released, free the task memory
            TaskContext taskContext = operatorContext.getDriverContext().getPipelineContext().getTaskContext();
            onDestroy = () -> taskContext.freeMemory(lookupSourceSizeInBytes);
        }

        lookupSourceFuture.set(lookupSource);
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
