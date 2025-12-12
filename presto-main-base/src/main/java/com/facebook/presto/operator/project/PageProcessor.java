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
package com.facebook.presto.operator.project;

import com.facebook.airlift.concurrent.NotThreadSafe;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.array.ReferenceCountMap;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.DictionaryId;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.WorkProcessor;
import com.facebook.presto.operator.WorkProcessor.ProcessState;
import com.facebook.presto.sql.gen.ExpressionProfiler;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SizeOf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.common.block.DictionaryId.randomDictionaryId;
import static com.facebook.presto.operator.WorkProcessor.ProcessState.finished;
import static com.facebook.presto.operator.WorkProcessor.ProcessState.ofResult;
import static com.facebook.presto.operator.project.SelectedPositions.positionsRange;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

@NotThreadSafe
public class PageProcessor
{
    public static final int MAX_BATCH_SIZE = 8 * 1024;
    static final int MAX_PAGE_SIZE_IN_BYTES = 4 * 1024 * 1024;
    static final int MIN_PAGE_SIZE_IN_BYTES = 1024 * 1024;

    private final ExpressionProfiler expressionProfiler;
    private final DictionarySourceIdFunction dictionarySourceIdFunction = new DictionarySourceIdFunction();
    private final Optional<PageFilter> filter;
    private final List<PageProjectionWithOutputs> projections;
    private final int outputCount;

    private int projectBatchSize;

    @VisibleForTesting
    public PageProcessor(Optional<PageFilter> filter, List<PageProjectionWithOutputs> projections, OptionalInt initialBatchSize)
    {
        this(filter, projections, initialBatchSize, new ExpressionProfiler());
    }

    @VisibleForTesting
    public PageProcessor(Optional<PageFilter> filter, List<PageProjectionWithOutputs> projections, OptionalInt initialBatchSize, ExpressionProfiler expressionProfiler)
    {
        List<Integer> outputChannels = projections.stream().map(PageProjectionWithOutputs::getOutputChannels).map(Arrays::stream).map(IntStream::boxed).flatMap(identity()).distinct().collect(toImmutableList());
        int outputCount = projections.stream().map(PageProjectionWithOutputs::getOutputCount).reduce(Integer::sum).orElse(0);
        verify(
                outputChannels.size() == outputCount && (outputCount == 0 || outputChannels.stream().max(Integer::compareTo).orElse(0) == outputChannels.size() - 1),
                format("Invalid outputChannels: outputCount: %d, outputChannels: %s", outputCount, outputChannels));

        this.filter = requireNonNull(filter, "filter is null")
                .map(pageFilter -> {
                    if (pageFilter.getInputChannels().size() == 1 && pageFilter.isDeterministic()) {
                        return new DictionaryAwarePageFilter(pageFilter);
                    }
                    return pageFilter;
                });
        this.outputCount = outputCount;
        this.projections = requireNonNull(projections, "projections is null").stream()
                .map(projectionWithOutputs -> {
                    PageProjection projection = projectionWithOutputs.getPageProjection();
                    if (projection.getInputChannels().size() == 1 && projection.isDeterministic()
                            && !(projection instanceof InputPageProjection)) {
                        return new PageProjectionWithOutputs(new DictionaryAwarePageProjection(projection, dictionarySourceIdFunction), projectionWithOutputs.getOutputChannels());
                    }
                    return projectionWithOutputs;
                })
                .collect(toImmutableList());
        this.projectBatchSize = initialBatchSize.orElse(1);
        this.expressionProfiler = requireNonNull(expressionProfiler, "expressionProfiler is null");
    }

    public PageProcessor(Optional<PageFilter> filter, List<PageProjectionWithOutputs> projections)
    {
        this(filter, projections, OptionalInt.of(1));
    }

    public Iterator<Optional<Page>> process(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, LocalMemoryContext memoryContext, Page page)
    {
        WorkProcessor<Page> processor = createWorkProcessor(properties, yieldSignal, memoryContext, page);
        return processor.yieldingIterator();
    }

    private WorkProcessor<Page> createWorkProcessor(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, LocalMemoryContext memoryContext, Page page)
    {
        // limit the scope of the dictionary ids to just one page
        dictionarySourceIdFunction.reset();

        if (page.getPositionCount() == 0) {
            return WorkProcessor.of();
        }

        if (filter.isPresent()) {
            SelectedPositions selectedPositions = filter.get().filter(properties, filter.get().getInputChannels().getInputChannels(page));
            if (selectedPositions.isEmpty()) {
                return WorkProcessor.of();
            }

            if (projections.isEmpty()) {
                // retained memory for empty page is negligible
                return WorkProcessor.of(new Page(selectedPositions.size()));
            }

            if (selectedPositions.size() != page.getPositionCount()) {
                return WorkProcessor.create(new ProjectSelectedPositions(properties, yieldSignal, memoryContext, page, selectedPositions));
            }
        }
        else if (projections.isEmpty()) {
            // retained memory for empty page is negligible
            return WorkProcessor.of(new Page(page.getPositionCount()));
        }

        return WorkProcessor.create(new ProjectSelectedPositions(properties, yieldSignal, memoryContext, page, positionsRange(0, page.getPositionCount())));
    }

    private class ProjectSelectedPositions
            implements WorkProcessor.Process<Page>
    {
        private final SqlFunctionProperties properties;
        private final DriverYieldSignal yieldSignal;
        private final LocalMemoryContext memoryContext;

        private Page page;
        private Block[] previouslyComputedResults;
        private SelectedPositions selectedPositions;
        private long retainedSizeInBytes;

        // remember if we need to re-use the same batch size if we yield last time
        private boolean lastComputeYielded;
        private int lastComputeBatchSize;
        private Work<List<Block>> pageProjectWork;

        private ProjectSelectedPositions(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, LocalMemoryContext memoryContext, Page page, SelectedPositions selectedPositions)
        {
            checkArgument(!selectedPositions.isEmpty(), "selectedPositions is empty");

            this.properties = properties;
            this.yieldSignal = yieldSignal;
            this.page = page;
            this.memoryContext = memoryContext;
            this.selectedPositions = selectedPositions;
            this.previouslyComputedResults = new Block[outputCount];
        }

        @Override
        public ProcessState<Page> process()
        {
            int batchSize;
            while (true) {
                if (selectedPositions.isEmpty()) {
                    verify(!lastComputeYielded);
                    return finished();
                }

                // we always process one chunk
                if (lastComputeYielded) {
                    // re-use the batch size from the last checkpoint
                    verify(lastComputeBatchSize > 0);
                    batchSize = lastComputeBatchSize;
                    lastComputeYielded = false;
                    lastComputeBatchSize = 0;
                }
                else {
                    batchSize = Math.min(selectedPositions.size(), projectBatchSize);
                }
                ProcessBatchResult result = processBatch(batchSize);

                if (result.isYieldFinish()) {
                    // if we are running out of time, save the batch size and continue next time
                    lastComputeYielded = true;
                    lastComputeBatchSize = batchSize;
                    updateRetainedSize();
                    return ProcessState.yield();
                }

                if (result.isPageTooLarge()) {
                    // if the page buffer filled up, so halve the batch size and retry
                    verify(batchSize > 1);
                    projectBatchSize = projectBatchSize / 2;
                    continue;
                }

                verify(result.isSuccess());
                Page resultPage = result.getPage();

                // if we produced a large page or if the expression is expensive, halve the batch size for the next call
                // We use the getApproximateLogicalSizeInBytes() instead of getSizeInBytes() for several reasons:
                //
                // 1. For DictionaryBlock, getSizeInBytes() calculates the compacted size. For example a DictionaryBlock with ids [1, 1] and dictionary with 3 elements of sizes
                // [5, 100, 10] would have sizeInBytes = 100 + 4 * 2 = 108. However if both position 0 and 1 are being projected, the outcome block should contain the second
                // element of the dictionary twice and the actual size of the output block should be 100 * 2 at least.
                //
                // 2. getSizeInBytes() is more sensitive to skew and may cause false positives on a larger number of pages. Suppose there are multiple page/block views on a base
                // page/blocks. Page/BlockView 1 may happen to project positions with larger elements, while Page/BlockView 2 may project positions with smaller elements.
                // Using the sizeInBytes for view 1 may cause view 2 to be sized down which is not desired. On the other hand, getApproximateLogicalSizeInBytes() returns amortized
                // sizes and is less jittery.
                //
                // 3. getApproximateLogicalSizeInBytes() is over 20x faster than getSizeInBytes() for DictionaryBlock and RleBlocks, and it avoids excessive memory allocations
                // that was known to cause reliability issues.
                long pageSize = resultPage.getApproximateLogicalSizeInBytes();
                if (resultPage.getPositionCount() > 1 && (pageSize > MAX_PAGE_SIZE_IN_BYTES || expressionProfiler.isExpressionExpensive())) {
                    projectBatchSize = projectBatchSize / 2;
                }

                // if we produced a small page, double the batch size for the next call
                if (pageSize < MIN_PAGE_SIZE_IN_BYTES && projectBatchSize < MAX_BATCH_SIZE && !expressionProfiler.isExpressionExpensive()) {
                    projectBatchSize = projectBatchSize * 2;
                }

                // remove batch from selectedPositions and previouslyComputedResults
                selectedPositions = selectedPositions.subRange(batchSize, selectedPositions.size());
                for (int i = 0; i < previouslyComputedResults.length; i++) {
                    if (previouslyComputedResults[i] != null && previouslyComputedResults[i].getPositionCount() > batchSize) {
                        previouslyComputedResults[i] = previouslyComputedResults[i].getRegion(batchSize, previouslyComputedResults[i].getPositionCount() - batchSize);
                    }
                    else {
                        previouslyComputedResults[i] = null;
                    }
                }

                if (!selectedPositions.isEmpty()) {
                    // there are still some positions to process therefore we need to retain page and account its memory
                    updateRetainedSize();
                }
                else {
                    page = null;
                    for (int i = 0; i < previouslyComputedResults.length; i++) {
                        previouslyComputedResults[i] = null;
                    }
                    memoryContext.setBytes(0);
                }

                return ofResult(resultPage);
            }
        }

        private void updateRetainedSize()
        {
            // increment the size only when it is the first reference
            retainedSizeInBytes = Page.INSTANCE_SIZE + SizeOf.sizeOfObjectArray(page.getChannelCount());
            ReferenceCountMap referenceCountMap = new ReferenceCountMap();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                if (!isNotLoadedLazyBlock(block)) {
                    block.retainedBytesForEachPart((object, size) -> {
                        if (referenceCountMap.incrementAndGet(object) == 1) {
                            retainedSizeInBytes += size;
                        }
                    });
                }
            }
            for (Block previouslyComputedResult : previouslyComputedResults) {
                if (previouslyComputedResult != null) {
                    previouslyComputedResult.retainedBytesForEachPart((object, size) -> {
                        if (referenceCountMap.incrementAndGet(object) == 1) {
                            retainedSizeInBytes += size;
                        }
                    });
                }
            }

            memoryContext.setBytes(retainedSizeInBytes);
        }

        private ProcessBatchResult processBatch(int batchSize)
        {
            Block[] blocks = new Block[outputCount];

            int pageSize = 0;
            SelectedPositions positionsBatch = selectedPositions.subRange(0, batchSize);
            for (PageProjectionWithOutputs projection : projections) {
                if (yieldSignal.isSet()) {
                    return ProcessBatchResult.processBatchYield();
                }

                if (positionsBatch.size() > 1 && pageSize > MAX_PAGE_SIZE_IN_BYTES) {
                    return ProcessBatchResult.processBatchTooLarge();
                }

                // if possible, use previouslyComputedResults produced in prior optimistic failure attempt
                int[] outputChannels = projection.getOutputChannels();
                // The progress on all output channels of a projection should be the same, so we just use the first one.
                if (previouslyComputedResults[outputChannels[0]] != null && previouslyComputedResults[outputChannels[0]].getPositionCount() >= batchSize) {
                    for (int channel : outputChannels) {
                        blocks[channel] = previouslyComputedResults[channel].getRegion(0, batchSize);
                        pageSize += blocks[channel].getApproximateRegionLogicalSizeInBytes(0, blocks[channel].getPositionCount());
                    }
                }
                else {
                    if (pageProjectWork == null) {
                        expressionProfiler.start();
                        pageProjectWork = projection.project(properties, yieldSignal, projection.getPageProjection().getInputChannels().getInputChannels(page), positionsBatch);
                        expressionProfiler.stop(positionsBatch.size());
                    }
                    if (!pageProjectWork.process()) {
                        return ProcessBatchResult.processBatchYield();
                    }
                    List<Block> projectionOutputs = pageProjectWork.getResult();
                    for (int j = 0; j < outputChannels.length; j++) {
                        int channel = outputChannels[j];
                        previouslyComputedResults[channel] = projectionOutputs.get(j);
                        blocks[channel] = previouslyComputedResults[channel];
                        pageSize += blocks[channel].getApproximateRegionLogicalSizeInBytes(0, blocks[channel].getPositionCount());
                    }
                    pageProjectWork = null;
                }
            }
            return ProcessBatchResult.processBatchSuccess(new Page(positionsBatch.size(), blocks));
        }
    }

    @VisibleForTesting
    public List<PageProjectionWithOutputs> getProjections()
    {
        return projections;
    }

    private static boolean isNotLoadedLazyBlock(Block block)
    {
        return (block instanceof LazyBlock) && !((LazyBlock) block).isLoaded();
    }

    @NotThreadSafe
    private static class DictionarySourceIdFunction
            implements Function<DictionaryBlock, DictionaryId>
    {
        private final Map<DictionaryId, DictionaryId> dictionarySourceIds = new HashMap<>();

        @Override
        public DictionaryId apply(DictionaryBlock block)
        {
            return dictionarySourceIds.computeIfAbsent(block.getDictionarySourceId(), ignored -> randomDictionaryId());
        }

        public void reset()
        {
            dictionarySourceIds.clear();
        }
    }

    private static class ProcessBatchResult
    {
        private final ProcessBatchState state;
        private final Page page;

        private ProcessBatchResult(ProcessBatchState state, Page page)
        {
            this.state = state;
            this.page = page;
        }

        public static ProcessBatchResult processBatchYield()
        {
            return new ProcessBatchResult(ProcessBatchState.YIELD, null);
        }

        public static ProcessBatchResult processBatchTooLarge()
        {
            return new ProcessBatchResult(ProcessBatchState.PAGE_TOO_LARGE, null);
        }

        public static ProcessBatchResult processBatchSuccess(Page page)
        {
            return new ProcessBatchResult(ProcessBatchState.SUCCESS, requireNonNull(page));
        }

        public boolean isYieldFinish()
        {
            return state == ProcessBatchState.YIELD;
        }

        public boolean isPageTooLarge()
        {
            return state == ProcessBatchState.PAGE_TOO_LARGE;
        }

        public boolean isSuccess()
        {
            return state == ProcessBatchState.SUCCESS;
        }

        public Page getPage()
        {
            verify(page != null);
            verify(state == ProcessBatchState.SUCCESS);
            return page;
        }

        private enum ProcessBatchState
        {
            YIELD,
            PAGE_TOO_LARGE,
            SUCCESS
        }
    }
}
