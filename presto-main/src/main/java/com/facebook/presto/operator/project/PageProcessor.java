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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.DictionaryId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.operator.project.PageProcessorOutput.EMPTY_PAGE_PROCESSOR_OUTPUT;
import static com.facebook.presto.spi.block.DictionaryId.randomDictionaryId;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class PageProcessor
{
    static final int MAX_BATCH_SIZE = 8 * 1024;
    static final int MAX_PAGE_SIZE_IN_BYTES = 4 * 1024 * 1024;
    static final int MIN_PAGE_SIZE_IN_BYTES = 1024 * 1024;

    private final DictionarySourceIdFunction dictionarySourceIdFunction = new DictionarySourceIdFunction();
    private final Optional<PageFilter> filter;
    private final List<PageProjection> projections;

    private int projectBatchSize = MAX_BATCH_SIZE;

    public PageProcessor(Optional<PageFilter> filter, List<? extends PageProjection> projections)
    {
        this.filter = requireNonNull(filter, "filter is null")
                .map(pageFilter -> {
                    if (pageFilter.getInputChannels().size() == 1 && pageFilter.isDeterministic()) {
                        return new DictionaryAwarePageFilter(pageFilter);
                    }
                    return pageFilter;
                });
        this.projections = requireNonNull(projections, "projections is null").stream()
                .map(projection -> {
                    if (projection.getInputChannels().size() == 1 && projection.isDeterministic()) {
                        return new DictionaryAwarePageProjection(projection, dictionarySourceIdFunction);
                    }
                    return projection;
                })
                .collect(toImmutableList());
    }

    public PageProcessorOutput process(ConnectorSession session, Page page)
    {
        // limit the scope of the dictionary ids to just one page
        dictionarySourceIdFunction.reset();

        if (page.getPositionCount() == 0) {
            return EMPTY_PAGE_PROCESSOR_OUTPUT;
        }

        if (filter.isPresent()) {
            SelectedPositions selectedPositions = filter.get().filter(session, filter.get().getInputChannels().getInputChannels(page));
            if (selectedPositions.isEmpty()) {
                return EMPTY_PAGE_PROCESSOR_OUTPUT;
            }

            if (projections.isEmpty()) {
                return new PageProcessorOutput(page.getRetainedSizeInBytes(), Iterators.singletonIterator(new Page(selectedPositions.size())));
            }

            if (selectedPositions.size() != page.getPositionCount()) {
                return new PageProcessorOutput(page.getRetainedSizeInBytes(), new PositionsPageProcessorIterator(session, page, selectedPositions));
            }
        }

        return new PageProcessorOutput(
                page.getRetainedSizeInBytes(),
                new PositionsPageProcessorIterator(session, page, SelectedPositions.positionsRange(0, page.getPositionCount())));
    }

    @VisibleForTesting
    public List<PageProjection> getProjections()
    {
        return projections;
    }

    private class PositionsPageProcessorIterator
            extends AbstractIterator<Page>
    {
        private final ConnectorSession session;
        private final Page page;

        private SelectedPositions selectedPositions;
        private final Block[] previouslyComputedResults;

        public PositionsPageProcessorIterator(ConnectorSession session, Page page, SelectedPositions selectedPositions)
        {
            this.session = session;
            this.page = page;
            this.selectedPositions = selectedPositions;
            this.previouslyComputedResults = new Block[projections.size()];
        }

        @Override
        protected Page computeNext()
        {
            while (true) {
                if (selectedPositions.isEmpty()) {
                    return endOfData();
                }

                int batchSize = Math.min(selectedPositions.size(), projectBatchSize);
                Optional<Page> result = processBatch(batchSize);

                // if the page buffer filled up, so halve the batch size and retry
                if (!result.isPresent()) {
                    verify(batchSize > 1);
                    projectBatchSize = projectBatchSize / 2;
                    continue;
                }

                Page page = result.get();

                // if we produced a large page, halve the batch size for the next call
                long pageSize = page.getSizeInBytes();
                if (page.getPositionCount() > 1 && pageSize > MAX_PAGE_SIZE_IN_BYTES) {
                    projectBatchSize = projectBatchSize / 2;
                }

                // if we produced a small page, double the batch size for the next call
                if (pageSize < MIN_PAGE_SIZE_IN_BYTES && projectBatchSize < MAX_BATCH_SIZE) {
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

                return page;
            }
        }

        private Optional<Page> processBatch(int batchSize)
        {
            Block[] blocks = new Block[projections.size()];

            int pageSize = 0;
            SelectedPositions positionsBatch = selectedPositions.subRange(0, batchSize);
            for (int i = 0; i < projections.size(); i++) {
                if (positionsBatch.size() > 1 && pageSize > MAX_PAGE_SIZE_IN_BYTES) {
                    return Optional.empty();
                }

                // if possible, use previouslyComputedResults produced in prior optimistic failure attempt
                PageProjection projection = projections.get(i);
                if (previouslyComputedResults[i] != null && previouslyComputedResults[i].getPositionCount() > batchSize) {
                    blocks[i] = previouslyComputedResults[i].getRegion(0, batchSize);
                }
                else {
                    previouslyComputedResults[i] = projection.project(session, projection.getInputChannels().getInputChannels(page), positionsBatch);
                    blocks[i] = previouslyComputedResults[i];
                }

                pageSize += blocks[i].getSizeInBytes();
            }
            return Optional.of(new Page(positionsBatch.size(), blocks));
        }
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
}
