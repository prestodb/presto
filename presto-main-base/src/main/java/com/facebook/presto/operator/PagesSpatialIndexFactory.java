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

import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

/**
 * This factory is used to pass spatial index built by SpatialIndexBuilderOperator
 * to SpatialJoinOperators.
 * <p>
 * SpatialIndexBuilderOperator creates a spatial index {@link Supplier} and provides
 * it to this factory by calling {@link #lendPagesSpatialIndex(Supplier)}.
 * <p>
 * SpatialJoinOperators call {@link #createPagesSpatialIndex()} to get a Future that
 * will provide an instance of the spatial index when done. The {@link Supplier}
 * is used to create separate instances of an index for each SpatialJoinOperator. All
 * these instances share the index of geometries on the build side, but have their own
 * instances of an optional extra potentially stateful filter function.
 * <p>
 * SpatialIndexBuilderOperator is responsible for keeping track of how much memory
 * is used by the index shared among SpatialJoinOperators. To do so
 * SpatialIndexBuilderOperator has to stay active until all the SpatialJoinOperators
 * have finished.
 * <p>
 * This factory keeps track of the number of active SpatialJoinOperators in cooperation
 * with SpatialJoinOperatorFactory and SpatialJoinOperators. Initially, the number of
 * active SpatialJoinOperators is set to 1. When SpatialJoinOperator is created, it calls
 * {@link #createPagesSpatialIndex()} and the counter goes up by 1. When SpatialJoinOperator
 * finishes, it calls {@link #probeOperatorFinished()} and the counter goes down by 1.
 * In addition SpatialJoinOperatorFactory calls {@link #noMoreProbeOperators()} when done
 * creating operators and the counter goes down by 1.
 * <p>
 * {@link #lendPagesSpatialIndex(Supplier)} returns a Future that completes once all
 * the SpatialJoinOperators completed. SpatialIndexBuilderOperator uses that Future
 * to decide on its own completion.
 */
@ThreadSafe
public class PagesSpatialIndexFactory
{
    private final List<Type> types;
    private final List<Type> outputTypes;

    @GuardedBy("this")
    private final List<SettableFuture<PagesSpatialIndex>> pagesSpatialIndexFutures = new ArrayList<>();

    @GuardedBy("this")
    @Nullable
    private Supplier<PagesSpatialIndex> pagesSpatialIndex;

    private final ReferenceCount activeProbeOperators = new ReferenceCount(1);

    public PagesSpatialIndexFactory(List<Type> types, List<Type> outputTypes)
    {
        this.types = ImmutableList.copyOf(types);
        this.outputTypes = ImmutableList.copyOf(outputTypes);
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    /**
     * Called by {@link SpatialJoinOperator}.
     * <p>
     * {@link SpatialJoinOperator} must call {@link #probeOperatorFinished()} on completion
     * to signal that spatial index is no longer needed.
     */
    public synchronized ListenableFuture<PagesSpatialIndex> createPagesSpatialIndex()
    {
        activeProbeOperators.retain();

        if (pagesSpatialIndex != null) {
            return immediateFuture(pagesSpatialIndex.get());
        }

        SettableFuture<PagesSpatialIndex> future = SettableFuture.create();
        pagesSpatialIndexFutures.add(future);
        return future;
    }

    /**
     * Called by SpatialJoinOperatorFactory to indicate that a duplicate factory will be created
     * and this class should wait for an extra {@link noMoreProbeOperators} call before
     * releasing spatial index.
     */
    public synchronized void addProbeOperatorFactory()
    {
        activeProbeOperators.retain();
    }

    /**
     * Called by SpatialJoinOperatorFactory to indicate that all
     * {@link SpatialJoinOperator} have been created.
     */
    public synchronized void noMoreProbeOperators()
    {
        activeProbeOperators.release();
        if (activeProbeOperators.getFreeFuture().isDone()) {
            pagesSpatialIndex = null;
        }
    }

    /**
     * Called by {@link SpatialJoinOperator} on completion to signal that spatial index
     * is no longer needed.
     */
    public synchronized void probeOperatorFinished()
    {
        activeProbeOperators.release();
        if (activeProbeOperators.getFreeFuture().isDone()) {
            pagesSpatialIndex = null;
        }
    }

    /**
     * Called by {@link SpatialIndexBuilderOperator} to provide a
     * {@link Supplier} of spatial indexes for {@link SpatialJoinOperator}s to use.
     * <p>
     * Returns a Future that completes once all the {@link SpatialJoinOperator}s have completed.
     */
    public ListenableFuture<?> lendPagesSpatialIndex(Supplier<PagesSpatialIndex> pagesSpatialIndex)
    {
        requireNonNull(pagesSpatialIndex, "pagesSpatialIndex is null");

        if (activeProbeOperators.getFreeFuture().isDone()) {
            return NOT_BLOCKED;
        }

        List<SettableFuture<PagesSpatialIndex>> settableFutures;
        synchronized (this) {
            verify(this.pagesSpatialIndex == null);
            this.pagesSpatialIndex = pagesSpatialIndex;
            settableFutures = ImmutableList.copyOf(pagesSpatialIndexFutures);
            pagesSpatialIndexFutures.clear();
        }

        for (SettableFuture<PagesSpatialIndex> settableFuture : settableFutures) {
            settableFuture.set(pagesSpatialIndex.get());
        }

        return activeProbeOperators.getFreeFuture();
    }
}
