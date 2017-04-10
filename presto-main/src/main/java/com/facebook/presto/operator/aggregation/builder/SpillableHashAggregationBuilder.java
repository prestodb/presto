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
package com.facebook.presto.operator.aggregation.builder;

import com.facebook.presto.memory.AbstractAggregatedMemoryContext;
import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.operator.HashCollisionsCounter;
import com.facebook.presto.operator.MergeHashSort;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.Spiller;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.Math.max;

public class SpillableHashAggregationBuilder
        implements HashAggregationBuilder
{
    private InMemoryHashAggregationBuilder hashAggregationBuilder;
    private final SpillerFactory spillerFactory;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final AggregationNode.Step step;
    private final int expectedGroups;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final Optional<Integer> hashChannel;
    private final OperatorContext operatorContext;
    private final long memorySizeBeforeSpill;
    private final long memoryLimitForMergeWithMemory;
    private Optional<Spiller> spiller = Optional.empty();
    private Optional<MergingHashAggregationBuilder> merger = Optional.empty();
    private Optional<MergeHashSort> mergeHashSort = Optional.empty();
    private ListenableFuture<?> spillInProgress = immediateFuture(null);
    private final LocalMemoryContext aggregationMemoryContext;
    private final LocalMemoryContext spillMemoryContext;
    private final JoinCompiler joinCompiler;

    private long hashCollisions;
    private double expectedHashCollisions;

    public SpillableHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            DataSize memoryLimitBeforeSpill,
            DataSize memoryLimitForMergeWithMemory,
            SpillerFactory spillerFactory,
            JoinCompiler joinCompiler)
    {
        this.accumulatorFactories = accumulatorFactories;
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.groupByTypes = groupByTypes;
        this.groupByChannels = groupByChannels;
        this.hashChannel = hashChannel;
        this.operatorContext = operatorContext;
        this.memorySizeBeforeSpill = memoryLimitBeforeSpill.toBytes();
        this.memoryLimitForMergeWithMemory = memoryLimitForMergeWithMemory.toBytes();
        this.spillerFactory = spillerFactory;
        this.joinCompiler = joinCompiler;

        AbstractAggregatedMemoryContext systemMemoryContext = operatorContext.getSystemMemoryContext();
        this.aggregationMemoryContext = systemMemoryContext.newLocalMemoryContext();
        this.spillMemoryContext = systemMemoryContext.newLocalMemoryContext();

        rebuildHashAggregationBuilder();
    }

    @Override
    public void processPage(Page page)
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");

        hashAggregationBuilder.processPage(page);

        if (shouldSpill(getSizeInMemory())) {
            spillToDisk();
        }
    }

    @Override
    public void updateMemory()
    {
        aggregationMemoryContext.setBytes(getSizeInMemory());

        if (spillInProgress.isDone()) {
            spillMemoryContext.setBytes(0L);
        }
    }

    public long getSizeInMemory()
    {
        // TODO: we could skip memory reservation for hashAggregationBuilder.getGroupIdsSortingSize()
        // if before building result from hashAggregationBuilder we would convert it to "read only" version.
        // Read only version of GroupByHash from hashAggregationBuilder could be compacted by dropping
        // most of it's field, freeing up some memory that could be used for sorting.
        return hashAggregationBuilder.getSizeInMemory() + hashAggregationBuilder.getGroupIdsSortingSize();
    }

    @Override
    public void recordHashCollisions(HashCollisionsCounter hashCollisionsCounter)
    {
        hashCollisionsCounter.recordHashCollision(hashCollisions, expectedHashCollisions);
        hashCollisions = 0;
        expectedHashCollisions = 0;
    }

    @Override
    public boolean isFull()
    {
        return false;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return spillInProgress;
    }

    private boolean hasPreviousSpillCompletedSuccessfully()
    {
        if (isBlocked().isDone()) {
            // check for exception from previous spill for early failure
            getFutureValue(spillInProgress);
            return true;
        }
        else {
            return false;
        }
    }

    private boolean shouldSpill(long memorySize)
    {
        return (memorySizeBeforeSpill > 0 && memorySize > memorySizeBeforeSpill);
    }

    private boolean shouldMergeWithMemory(long memorySize)
    {
        return memorySize < memoryLimitForMergeWithMemory;
    }

    @Override
    public Iterator<Page> buildResult()
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");

        if (!spiller.isPresent()) {
            return hashAggregationBuilder.buildResult();
        }

        try {
            if (shouldMergeWithMemory(getSizeInMemory())) {
                return mergeFromDiskAndMemory();
            }
            else {
                spillToDisk().get();
                return mergeFromDisk();
            }
        }
        catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close()
    {
        if (merger.isPresent()) {
            merger.get().close();
        }
        if (spiller.isPresent()) {
            spiller.get().close();
        }
        if (mergeHashSort.isPresent()) {
            mergeHashSort.get().close();
        }
    }

    private ListenableFuture<?> spillToDisk()
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");
        hashAggregationBuilder.setOutputPartial();

        if (!spiller.isPresent()) {
            spiller = Optional.of(spillerFactory.create(
                    hashAggregationBuilder.buildTypes(),
                    operatorContext.getSpillContext(),
                    operatorContext.getSystemMemoryContext().newAggregatedMemoryContext()));
        }
        long spillMemoryUsage = getSizeInMemory();

        // start spilling process with current content of the hashAggregationBuilder builder...
        spillInProgress = spiller.get().spill(hashAggregationBuilder.buildHashSortedResult());
        // ... and immediately create new hashAggregationBuilder so effectively memory ownership
        // over hashAggregationBuilder is transferred from this thread to a spilling thread
        rebuildHashAggregationBuilder();

        // First decrease memory usage of aggregation context...
        aggregationMemoryContext.setBytes(getSizeInMemory());
        // And then transfer this memory to spill context
        // TODO: is there an easy way to do this atomically?
        spillMemoryContext.setBytes(spillMemoryUsage);

        return spillInProgress;
    }

    private Iterator<Page> mergeFromDiskAndMemory()
    {
        checkState(spiller.isPresent());

        hashAggregationBuilder.setOutputPartial();
        mergeHashSort = Optional.of(new MergeHashSort(operatorContext.getSystemMemoryContext().newAggregatedMemoryContext()));

        Iterator<Page> mergedSpilledPages = mergeHashSort.get().merge(
                groupByTypes,
                hashAggregationBuilder.buildIntermediateTypes(),
                ImmutableList.<Iterator<Page>>builder()
                        .addAll(spiller.get().getSpills())
                        .add(hashAggregationBuilder.buildHashSortedResult())
                        .build());

        return mergeSortedPages(mergedSpilledPages, max(memorySizeBeforeSpill - memoryLimitForMergeWithMemory, 1L));
    }

    private Iterator<Page> mergeFromDisk()
    {
        checkState(spiller.isPresent());

        mergeHashSort = Optional.of(new MergeHashSort(operatorContext.getSystemMemoryContext().newAggregatedMemoryContext()));

        Iterator<Page> mergedSpilledPages = mergeHashSort.get().merge(
                groupByTypes,
                hashAggregationBuilder.buildIntermediateTypes(),
                spiller.get().getSpills());

        return mergeSortedPages(mergedSpilledPages, memorySizeBeforeSpill);
    }

    private Iterator<Page> mergeSortedPages(Iterator<Page> sortedPages, long memorySizeBeforeSpill)
    {
        merger = Optional.of(new MergingHashAggregationBuilder(
                accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                hashChannel,
                operatorContext,
                sortedPages,
                operatorContext.getSystemMemoryContext().newLocalMemoryContext(),
                memorySizeBeforeSpill,
                hashAggregationBuilder.getKeyChannels(),
                joinCompiler));

        return merger.get().buildResult();
    }

    private void rebuildHashAggregationBuilder()
    {
        if (hashAggregationBuilder != null) {
            hashCollisions += hashAggregationBuilder.getHashCollisions();
            expectedHashCollisions += hashAggregationBuilder.getExpectedHashCollisions();
        }

        this.hashAggregationBuilder = new InMemoryHashAggregationBuilder(
                accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                hashChannel,
                operatorContext,
                DataSize.succinctBytes(0),
                joinCompiler);
    }
}
