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
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * LookupSourceFactory wrapper that enables sharing a LookupSourceFactory
 * across multiple probe factories.
 * <p>
 * When LookupSourceFactory is shared across multiple probe factories, each of them
 * will call destroy on the LookupSourceFactory when they are done with it.
 * This leads to pre-mature destroy because other probes may still be actively using it.
 * This class makes it possible to destroy the underlying LookupSourceFactroy
 * in a coordinated fashion.
 */
@ThreadSafe
public class SharedLookupSourceFactory
        implements LookupSourceFactory
{
    private final LookupSourceFactory delegate;
    private final Runnable onDestroy;

    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    public SharedLookupSourceFactory(LookupSourceFactory delegate, Runnable onDestroy)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.onDestroy = requireNonNull(onDestroy, "onDestroy is null");
    }

    @Override
    public void destroy()
    {
        if (destroyed.compareAndSet(false, true)) {
            onDestroy.run();
        }
    }

    @Override
    public List<Type> getTypes()
    {
        return delegate.getTypes();
    }

    @Override
    public List<Type> getOutputTypes()
    {
        return delegate.getOutputTypes();
    }

    @Override
    public ListenableFuture<LookupSourceProvider> createLookupSourceProvider()
    {
        return delegate.createLookupSourceProvider();
    }

    @Override
    public int partitions()
    {
        return delegate.partitions();
    }

    @Override
    public ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>> finishProbeOperator(OptionalInt lookupJoinsCount)
    {
        return delegate.finishProbeOperator(lookupJoinsCount);
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        return delegate.getOuterPositionIterator();
    }

    @Override
    public Map<Symbol, Integer> getLayout()
    {
        return delegate.getLayout();
    }

    @Override
    public void setTaskContext(TaskContext taskContext)
    {
        delegate.setTaskContext(taskContext);
    }

    @Override
    public ListenableFuture<?> lendPartitionLookupSource(int partitionIndex, Supplier<LookupSource> partitionLookupSource)
    {
        return delegate.lendPartitionLookupSource(partitionIndex, partitionLookupSource);
    }

    @Override
    public void setPartitionSpilledLookupSourceHandle(int partitionIndex, SpilledLookupSourceHandle spilledLookupSourceHandle)
    {
        delegate.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
    }

    @Override
    public ListenableFuture<?> isDestroyed()
    {
        return delegate.isDestroyed();
    }
}
