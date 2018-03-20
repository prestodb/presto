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
package com.facebook.presto.util;

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.ContinuousWork;
import com.facebook.presto.operator.ContinuousWorkUtils.WorkState;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.PageWithPositionComparator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.AbstractIterator;

import java.util.List;

import static com.facebook.presto.operator.ContinuousWork.mergeSorted;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class MergeSortedPages
{
    private MergeSortedPages() {}

    public static ContinuousWork<Page> mergeSortedPages(
            List<ContinuousWork<Page>> pageProducers,
            PageWithPositionComparator comparator,
            List<Integer> outputChannels,
            List<Type> outputTypes,
            AggregatedMemoryContext aggregatedMemoryContext,
            DriverYieldSignal yieldSignal)
    {
        return buildPage(mergeSorted(
                pageProducers.stream()
                        .map(pageProducer -> pageWithPositions(pageProducer, aggregatedMemoryContext.newLocalMemoryContext()))
                        .collect(toImmutableList()),
                (firstPageWithPosition, secondPageWithPosition) -> comparator.compareTo(
                        firstPageWithPosition.page, firstPageWithPosition.position,
                        secondPageWithPosition.page, secondPageWithPosition.position)),
                outputChannels,
                outputTypes,
                aggregatedMemoryContext.newLocalMemoryContext(),
                yieldSignal);
    }

    private static ContinuousWork<Page> buildPage(
            ContinuousWork<PageWithPosition> pageWithPositions,
            List<Integer> outputChannels,
            List<Type> outputTypes,
            LocalMemoryContext memoryContext,
            DriverYieldSignal yieldSignal)
    {
        PageBuilder pageBuilder = new PageBuilder(outputTypes);
        return pageWithPositions.transform(pageWithPositionOptional -> {
            if (yieldSignal.isSet()) {
                return WorkState.yield();
            }

            boolean finished = !pageWithPositionOptional.isPresent();
            if (finished && pageBuilder.isEmpty()) {
                return WorkState.finished();
            }

            if (finished || pageBuilder.isFull()) {
                // update memory usage just before producing page to cap from top
                memoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                Page page = pageBuilder.build();
                pageBuilder.reset();
                if (!finished) {
                    pageWithPositionOptional.get().appendTo(pageBuilder, outputChannels, outputTypes);
                }
                return WorkState.ofResult(page, !finished);
            }

            pageWithPositionOptional.get().appendTo(pageBuilder, outputChannels, outputTypes);
            return WorkState.needsMoreData();
        });
    }

    private static ContinuousWork<PageWithPosition> pageWithPositions(ContinuousWork<Page> pages, LocalMemoryContext memoryContext)
    {
        return pages.flatMap(page -> {
            memoryContext.setBytes(page.getRetainedSizeInBytes());
            return new AbstractIterator<PageWithPosition>()
            {
                int position;

                @Override
                public PageWithPosition computeNext()
                {
                    if (position >= page.getPositionCount()) {
                        memoryContext.setBytes(0);
                        return endOfData();
                    }

                    return new PageWithPosition(page, position++);
                }
            };
        });
    }

    private static class PageWithPosition
    {
        final Page page;
        final int position;

        PageWithPosition(Page page, int position)
        {
            this.page = requireNonNull(page, "page is null");
            this.position = position;
        }

        void appendTo(PageBuilder pageBuilder, List<Integer> outputChannels, List<Type> outputTypes)
        {
            pageBuilder.declarePosition();
            for (int i = 0; i < outputChannels.size(); i++) {
                Type type = outputTypes.get(i);
                Block block = page.getBlock(outputChannels.get(i));
                type.appendTo(block, position, pageBuilder.getBlockBuilder(i));
            }
        }
    }
}
