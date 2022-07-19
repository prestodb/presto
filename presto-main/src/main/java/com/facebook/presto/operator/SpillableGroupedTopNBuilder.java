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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spiller.Spiller;
import com.facebook.presto.spiller.SpillerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.openjdk.jol.info.ClassLayout;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.facebook.presto.operator.SpillingUtils.checkSpillSucceeded;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class SpillableGroupedTopNBuilder
        implements GroupedTopNBuilder
{
    private static final Logger logger = Logger.get(SpillableGroupedTopNBuilder.class);
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SpillableGroupedTopNBuilder.class).instanceSize();

    private final List<Type> sourceTypes;
    private final PageWithPositionComparator comparator;
    private final int topN;
    private final boolean produceRowNumber;
    private final GroupByHash groupByHash;
    private final SpillerFactory spillerFactory;
    private final OperatorContext operatorContext;

    private InMemoryGroupedTopNBuilder inMemoryGroupedTopNBuilder;
    private InMemoryGroupedTopNBuilder mergingInMemoryGroupedTopNBuilder;

    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    private final LocalMemoryContext systemMemoryContext;
    private final long memoryLimitForMergeWithMemory;

    private Optional<Spiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = immediateFuture(null);
    private Optional<MergeHashSort> mergeHashSort = Optional.empty();
    private boolean producingOutput;

    public SpillableGroupedTopNBuilder(
            List<Type> sourceTypes,
            PageWithPositionComparator comparator,
            int topN,
            boolean produceRowNumber,
            GroupByHash groupByHash,
            OperatorContext operatorContext,
            SpillerFactory spillerFactory)
    {
        this.sourceTypes = sourceTypes;
        this.comparator = comparator;
        this.topN = topN;
        this.produceRowNumber = produceRowNumber;
        this.groupByHash = groupByHash;
        this.spillerFactory = spillerFactory;
        this.operatorContext = operatorContext;

        initializeInMemoryGroupedTopNBuilder();

        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();
        this.systemMemoryContext = operatorContext.localSystemMemoryContext();

        this.memoryLimitForMergeWithMemory = 1024 * 1024 * 1024 * 1024 * 4;
        this.producingOutput = false;
    }

    public Work<?> processPage(Page page)
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");

        Work<?> result =  inMemoryGroupedTopNBuilder.processPage(page);
        updateMemoryReservations(); // what if we run out of revocable memory ?? We should ideally spill here as well.
        return result;
    }

    private boolean hasPreviousSpillCompletedSuccessfully()
    {
        if (spillInProgress.isDone()) {
            // check for exception from previous spill for early failure
            checkSpillSucceeded(spillInProgress);
            return true;
        }
        else {
            return false;
        }
    }

    public Iterator<Page> buildResult()
    {
        producingOutput = true;
        // Convert revocable memory to user memory as returned Iterator holds on to memory so we no longer can revoke.
        if (localRevocableMemoryContext.getBytes() > 0) {
            long currentRevocableBytes = localRevocableMemoryContext.getBytes();
            localRevocableMemoryContext.setBytes(0);
            if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + currentRevocableBytes)) {
                localRevocableMemoryContext.setBytes(currentRevocableBytes);
                logger.info("Started spilling in buildResult() currentRevocableBytes=%s", currentRevocableBytes);
                checkSpillSucceeded(spillToDisk());
                logger.info("Completed spilling in buildResult()");
                updateMemoryReservations();
            }
        }

        if (!spiller.isPresent()) {
            return inMemoryGroupedTopNBuilder.buildResult();
        }

        /* This means we have spilled some data. Either spill the current in-memory
        chunk or merge it. Either way get the final list of page streams to merge.
         */
        List<WorkProcessor<Page>> sortedPageStreams = null;
        if (shouldMergeWithMemory(getSizeInMemory())) {
            logger.info("mergingPages: without spilling");
            sortedPageStreams = ImmutableList.<WorkProcessor<Page>>builder()
                    .addAll(spiller.get().getSpills().stream()
                            .map(WorkProcessor::fromIterator)
                            .collect(toImmutableList()))
                    .add(WorkProcessor.fromIterator(inMemoryGroupedTopNBuilder.buildHashSortedResult()))
                    .build();
        }
        else {
            logger.info("mergingPages: with spilling");
            checkSpillSucceeded(spillToDisk());
            updateMemoryReservations();
            sortedPageStreams = ImmutableList.<WorkProcessor<Page>>builder()
                    .addAll(spiller.get().getSpills().stream()
                            .map(WorkProcessor::fromIterator)
                            .collect(toImmutableList()))
                    .build();
        }
        logger.info("mergingPages: Completed");

        /* Sort-merge join sorted grouped streams and generate result group at a time
         */
        return getFinalResult(sortedPageStreams);
    }

    @Override
    public long getEstimatedSizeInBytes()
    {
        return getSizeInMemory();
    }

    @Override
    public long getGroupIdsSortingSize()
    {
        return 0;
    }

    public ListenableFuture<?> startMemoryRevoke()
    {
        checkState(spillInProgress.isDone());
        if (producingOutput) {
            // All revocable memory has been released in buildResult method.
            // At this point, InMemoryGroupedTopNBuilder is no longer accepting any input so no point in spilling.
            verify(localRevocableMemoryContext.getBytes() == 0);
            return NOT_BLOCKED;
        }
        spillToDisk();
        return spillInProgress;
    }

    public void finishMemoryRevoke()
    {
        if (spiller.isPresent()) {
            checkState(spillInProgress.isDone());
            spiller.get().commit();
        }
        updateMemoryReservations();
    }

    private boolean shouldMergeWithMemory(long memorySize)
    {
        return memorySize < memoryLimitForMergeWithMemory;
    }

    @VisibleForTesting
    private Iterator<Page> getFinalResult(List<WorkProcessor<Page>> sortedPageStreams)
    {
        mergeHashSort = Optional.of(new MergeHashSort(operatorContext.aggregateSystemMemoryContext()));
        WorkProcessor<Page> mergedSortedPages = WorkProcessor.fromIterator(mergeHashSort.get().merge(
                groupByHash.getTypes(),
                sourceTypes,
                sortedPageStreams,
                operatorContext.getDriverContext().getYieldSignal()).iterator());

        /* Process page by page to return grouped TopN Rows */
        WorkProcessor<Page> result = mergedSortedPages.flatTransform(new WorkProcessor.Transformation<Page, WorkProcessor<Page>>()
        {
            boolean reset = true;
            long memorySize;

            public WorkProcessor.TransformationState<WorkProcessor<Page>> process(Optional<Page> inputPageOptional)
            {
                if (reset) {
                    initializeMergingGroupedTopNBuilder();
                    memorySize = 0;
                    reset = false;
                }

                boolean inputIsPresent = inputPageOptional.isPresent();
                if (!inputIsPresent && memorySize == 0) {
                    // no more pages and aggregation builder is empty
                    return WorkProcessor.TransformationState.finished();
                }

                if (inputIsPresent) {
                    Page inputPage = inputPageOptional.get();
                    boolean done = mergingInMemoryGroupedTopNBuilder.processPage(inputPage).process();
                    // TODO: this class does not yield wrt memory limit; enable it
                    verify(done);
                    memorySize = mergingInMemoryGroupedTopNBuilder.getEstimatedSizeInBytes();
                    systemMemoryContext.setBytes(memorySize);

                    if (memorySize < memoryLimitForMergeWithMemory) {
                        logger.info("needsMoreData. memorysize=%s", memorySize);
                        return WorkProcessor.TransformationState.needsMoreData();
                    }
                }
                logger.info("inputFinished. memorysize=%s", memorySize);

                reset = true;
                // we can produce output after every input page, because input pages do not have
                // hash values that span multiple pages (guaranteed by MergeHashSort)
                return WorkProcessor.TransformationState.ofResult(WorkProcessor.fromIterator(mergingInMemoryGroupedTopNBuilder.buildResult()), inputIsPresent);
            }
        });

        return result.iterator();
    }

    private ListenableFuture<?> spillToDisk()
    {
        if (!spiller.isPresent()) {
            spiller = Optional.of(spillerFactory.create(
                    this.sourceTypes,
                    operatorContext.getSpillContext(),
                    operatorContext.aggregateSystemMemoryContext()));
        }

        // start spilling process with current content of the inMemoryGroupedTopNBuilder builder...
        spillInProgress = spiller.get().spill(inMemoryGroupedTopNBuilder.buildHashSortedResult());
        // ... and immediately create new inMemoryGroupedTopNBuilder so effectively memory ownership
        // over inMemoryGroupedTopNBuilder is transferred from this thread to a spilling thread
        initializeInMemoryGroupedTopNBuilder();
        updateMemoryReservations();

        return spillInProgress;
    }

    private void initializeInMemoryGroupedTopNBuilder()
    {
        this.inMemoryGroupedTopNBuilder = new InMemoryGroupedTopNBuilder(
            this.sourceTypes,
            this.comparator,
            this.topN,
            this.produceRowNumber,
            this.groupByHash,
                Optional.empty());
    }

    private void initializeMergingGroupedTopNBuilder()
    {
        this.mergingInMemoryGroupedTopNBuilder = new InMemoryGroupedTopNBuilder(
            this.sourceTypes,
            this.comparator,
            this.topN,
            this.produceRowNumber,
            this.groupByHash,
                Optional.empty());
    }

    public void updateMemoryReservations()
    {
        this.localRevocableMemoryContext.setBytes(getEstimatedSizeInBytes());
    }

    public long getSizeInMemory()
    {
        // TODO: we could skip memory reservation for hashAggregationBuilder.getGroupIdsSortingSize()
        // if before building result from hashAggregationBuilder we would convert it to "read only" version.
        // Read only version of GroupByHash from hashAggregationBuilder could be compacted by dropping
        // most of it's field, freeing up some memory that could be used for sorting.
        return inMemoryGroupedTopNBuilder.getEstimatedSizeInBytes() + inMemoryGroupedTopNBuilder.getGroupIdsSortingSize();
    }
}
