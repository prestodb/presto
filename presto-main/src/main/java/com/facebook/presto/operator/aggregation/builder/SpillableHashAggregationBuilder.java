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
import com.facebook.presto.operator.MergeHashSort;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.Spiller;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
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
    private final long memoryLimitForMerge;
    private final long memoryLimitForMergeWithMemory;
    private Optional<Spiller> spiller = Optional.empty();
    private Optional<MergingHashAggregationBuilder> merger = Optional.empty();
    private CompletableFuture<?> spillInProgress = CompletableFuture.completedFuture(null);
    private final LocalMemoryContext memoryContext;

    public SpillableHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            DataSize memoryLimitForMerge,
            DataSize memoryLimitForMergeWithMemory,
            SpillerFactory spillerFactory)
    {
        this.accumulatorFactories = accumulatorFactories;
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.groupByTypes = groupByTypes;
        this.groupByChannels = groupByChannels;
        this.hashChannel = hashChannel;
        this.operatorContext = operatorContext;
        this.memoryLimitForMerge = memoryLimitForMerge.toBytes();
        this.memoryLimitForMergeWithMemory = memoryLimitForMergeWithMemory.toBytes();
        this.spillerFactory = spillerFactory;

        AbstractAggregatedMemoryContext systemMemoryContext = operatorContext.getSystemMemoryContext();
        this.memoryContext = systemMemoryContext.newLocalMemoryContext();

        rebuildHashAggregationBuilder();
    }

    @Override
    public void processPage(Page page)
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");

        hashAggregationBuilder.processPage(page);
    }

    @Override
    public void updateMemory()
    {
        if (!spillInProgress.isDone()) {
            return;
        }
        memoryContext.setRevocableBytes(hashAggregationBuilder.getSizeInMemory());
    }

    @Override
    public boolean isFull()
    {
        return false;
    }

    public CompletableFuture<?> isBlocked()
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

    @Override
    public CompletableFuture<?> startMemoryRevoke()
    {
        if (!spillInProgress.isDone()) {
            return spillInProgress;
        }
        spillToDisk();
        return spillInProgress;
    }

    @Override
    public void finishMemoryRevoke()
    {
        updateMemory();
    }

    private boolean shouldMergeWithMemory(long memorySize)
    {
        return memorySize < memoryLimitForMergeWithMemory;
    }

    public Iterator<Page> buildResult()
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");

        if (!spiller.isPresent()) {
            return hashAggregationBuilder.buildResult();
        }

        try {
            if (shouldMergeWithMemory(hashAggregationBuilder.getSizeInMemory())) {
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
    }

    private CompletableFuture<?> spillToDisk()
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");
        hashAggregationBuilder.setOutputPartial();

        if (!spiller.isPresent()) {
            spiller = Optional.of(spillerFactory.create(hashAggregationBuilder.buildTypes()));
        }

        // start spilling process with current content of the hashAggregationBuilder builder...
        spillInProgress = spiller.get().spill(hashAggregationBuilder.buildHashSortedResult());
        // ... and immediately create new hashAggregationBuilder so effectively memory ownership
        // over hashAggregationBuilder is transferred from this thread to a spilling thread
        rebuildHashAggregationBuilder();

        return spillInProgress;
    }

    private Iterator<Page> mergeFromDiskAndMemory()
    {
        checkState(spiller.isPresent());

        hashAggregationBuilder.setOutputPartial();

        Iterator<Page> mergedSpilledPages = MergeHashSort.merge(
                groupByTypes,
                hashAggregationBuilder.buildIntermediateTypes(),
                ImmutableList.<Iterator<Page>>builder()
                        .addAll(spiller.get().getSpills())
                        .add(hashAggregationBuilder.buildHashSortedResult())
                        .build());

        return mergeSortedPages(mergedSpilledPages, max(memoryLimitForMerge - memoryLimitForMergeWithMemory, 1L));
    }

    private Iterator<Page> mergeFromDisk()
    {
        checkState(spiller.isPresent());

        Iterator<Page> mergedSpilledPages = MergeHashSort.merge(
                groupByTypes,
                hashAggregationBuilder.buildIntermediateTypes(),
                spiller.get().getSpills());

        return mergeSortedPages(mergedSpilledPages, memoryLimitForMerge);
    }

    private Iterator<Page> mergeSortedPages(Iterator<Page> sortedPages, long memoryLimitForMerge)
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
                memoryLimitForMerge,
                hashAggregationBuilder.getKeyChannels()));

        return merger.get().buildResult();
    }

    private void rebuildHashAggregationBuilder()
    {
        this.hashAggregationBuilder = new InMemoryHashAggregationBuilder(
                accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                hashChannel,
                operatorContext);
    }
}
