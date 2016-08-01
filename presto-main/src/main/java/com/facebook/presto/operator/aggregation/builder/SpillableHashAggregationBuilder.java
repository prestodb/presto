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

import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.operator.MergeSort;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.spiller.Spiller;
import com.facebook.presto.spi.spiller.SpillerFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import io.airlift.units.DataSize;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;

public class SpillableHashAggregationBuilder
    implements HashAggregationBuilder
{
    private InMemoryHashAggregationBuilder hashAggregationBuilder;
    private final Optional<SpillerFactory> spillerFactory;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final AggregationNode.Step step;
    private final int expectedGroups;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final Optional<Integer> hashChannel;
    private final OperatorContext operatorContext;
    private final long maxEntriesBeforeSpill;
    private final long memorySizeBeforeSpill;
    private Optional<Spiller> spiller = Optional.empty();
    private CompletableFuture<?> spillInProgress = CompletableFuture.completedFuture(null);
    private final LocalMemoryContext systemMemoryContext;
    private final Closer closer = Closer.create();

    public SpillableHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            long maxEntriesBeforeSpill,
            DataSize memoryLimitBeforeSpill,
            Optional<SpillerFactory> spillerFactory)
    {
        this.accumulatorFactories = accumulatorFactories;
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.groupByTypes = groupByTypes;
        this.groupByChannels = groupByChannels;
        this.hashChannel = hashChannel;
        this.operatorContext = operatorContext;
        this.maxEntriesBeforeSpill = maxEntriesBeforeSpill;
        this.memorySizeBeforeSpill = memoryLimitBeforeSpill.toBytes();
        this.spillerFactory = spillerFactory;
        this.systemMemoryContext = operatorContext.getSystemMemoryContext().newLocalMemoryContext();

        rebuildHashAggregationBuilder();
    }

    public void processPage(Page page)
    {
        checkState(!isBusy(), "Previous spill hasn't yet finished");

        hashAggregationBuilder.processPage(page);
    }

    public boolean checkFullAndUpdateMemory()
    {
        if (isBusy()) {
            return true;
        }

        long memorySize = hashAggregationBuilder.getSizeInMemory();

        systemMemoryContext.setBytes(memorySize);

        if (shouldSpill(memorySize)) {
            spillToDisk();
        }
        return false;
    }

    @Override
    public boolean isBusy()
    {
        return !spillInProgress.isDone();
    }

    private boolean shouldSpill(long memorySize)
    {
        return (maxEntriesBeforeSpill > 0 && hashAggregationBuilder.getGroupCount() > maxEntriesBeforeSpill)
                || (memorySizeBeforeSpill > 0 && memorySize > memorySizeBeforeSpill);
    }

    public Iterator<Page> buildResult()
    {
        checkState(!isBusy(), "Previous spill hasn't yet finished");

        if (!spiller.isPresent()) {
            return hashAggregationBuilder.buildResult();
        }

        try {
            // TODO: don't spill here to disk, instead merge disk content with memory content
            spillToDisk().get();
            return mergeFromDisk();
        }
        catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    private CompletableFuture<?> spillToDisk()
    {
        hashAggregationBuilder.setOutputPartial();

        if (!spiller.isPresent()) {
            spiller = Optional.of(spillerFactory.get().create(hashAggregationBuilder.buildTypes()));
            closer.register(spiller.get());
        }

        // start spilling process with current content of the hashAggregationBuilder builder...
        spillInProgress = spiller.get().spill(hashAggregationBuilder.buildResultSorted());
        // ... and immediately create new hashAggregationBuilder so effectively memory ownership
        // over hashAggregationBuilder is transferred from this thread to a spilling thread
        rebuildHashAggregationBuilder();

        return spillInProgress;
    }

    private Iterator<Page> mergeFromDisk()
    {
        checkState(spiller.isPresent());

        Iterator<Page> mergedSpilledPages = MergeSort.merge(
                groupByTypes,
                hashAggregationBuilder.buildIntermediateTypes(),
                spiller.get().getSpills());

        MergingHashAggregationBuilder mergingHashAggregationBuilder = new MergingHashAggregationBuilder(
                accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                hashChannel,
                operatorContext,
                mergedSpilledPages,
                systemMemoryContext,
                maxEntriesBeforeSpill,
                memorySizeBeforeSpill,
                hashAggregationBuilder.getKeyChannels());

        closer.register(mergingHashAggregationBuilder);

        return mergingHashAggregationBuilder.buildResult();
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
