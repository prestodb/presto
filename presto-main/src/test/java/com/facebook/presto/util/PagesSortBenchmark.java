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

import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.WorkProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.MergeSortedPages.mergeSortedPages;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.testng.Assert.assertEquals;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 5, time = 400, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 400, timeUnit = TimeUnit.MILLISECONDS)
public class PagesSortBenchmark
{
    private static final OrderingCompiler ORDERING_COMPILER = new OrderingCompiler();

    @Benchmark
    public List<Page> runPagesIndexSortBenchmark(PagesIndexSortBenchmarkData data)
    {
        PagesIndex.TestingFactory pagesIndexFactory = new PagesIndex.TestingFactory(false);
        PagesIndex pageIndex = pagesIndexFactory.newPagesIndex(data.getTypes(), data.getTotalPositions());
        for (Page page : data.getPages()) {
            pageIndex.addPage(page);
        }

        pageIndex.sort(data.getSortChannels(), data.getSortOrders());

        return Streams.stream(pageIndex.getSortedPages()).collect(toImmutableList());
    }

    @Test
    public void verifyPagesIndexSortBenchmark()
    {
        PagesIndexSortBenchmarkData state = new PagesIndexSortBenchmarkData();
        state.setup();

        List<Page> pages = runPagesIndexSortBenchmark(state);

        int positionCount = pages.stream()
                .mapToInt(Page::getPositionCount)
                .sum();
        assertEquals(positionCount, state.getTotalPositions());
    }

    @State(Thread)
    public static class PagesIndexSortBenchmarkData
            extends BaseBenchmarkData
    {
        @Param({"1"})
        private int numSortChannels = 1;

        @Param({"1", "8"})
        private int totalChannels = 1;

        @Param({"200", "400"})
        private int pagesCount = 200;

        @Setup
        public void setup()
        {
            super.setup(numSortChannels, totalChannels, 1, pagesCount);
        }
    }

    @Benchmark
    public List<Page> runPagesMergeSortBenchmark(MergeSortedBenchmarkData data)
    {
        WorkProcessor<Page> sortedPagesWork = mergeSortedPages(
                data.getSplitPages().stream()
                        .map(WorkProcessor::fromIterable)
                        .collect(toImmutableList()),
                ORDERING_COMPILER.compilePageWithPositionComparator(data.getSortTypes(), data.getSortChannels(), data.getSortOrders()),
                data.getOutputChannels(),
                data.getTypes(),
                (pageBuilder, pageWithPosition) -> pageBuilder.isFull(),
                false,
                newSimpleAggregatedMemoryContext(),
                new DriverYieldSignal());

        ImmutableList.Builder<Page> sortedPages = ImmutableList.builder();
        while (true) {
            sortedPagesWork.process();

            if (sortedPagesWork.isFinished()) {
                return sortedPages.build();
            }

            sortedPages.add(sortedPagesWork.getResult());
        }
    }

    @Test
    public void verifyPagesMergeSortBenchmark()
    {
        MergeSortedBenchmarkData state = new MergeSortedBenchmarkData();
        state.setup();

        List<Page> pages = runPagesMergeSortBenchmark(state);

        int positionCount = pages.stream()
                .mapToInt(Page::getPositionCount)
                .sum();
        assertEquals(positionCount, state.getTotalPositions());
    }

    @State(Thread)
    public static class MergeSortedBenchmarkData
            extends BaseBenchmarkData
    {
        @Param({"1"})
        private int numSortChannels = 1;

        @Param({"1", "8"})
        private int totalChannels = 1;

        @Param({"2", "16"})
        private int numMergeSources = 2;

        @Param({"200", "400"})
        private int pagesCount = 200;

        @Setup
        public void setup()
        {
            super.setup(numSortChannels, totalChannels, numMergeSources, pagesCount);
        }
    }

    public static class BaseBenchmarkData
    {
        private List<Page> pages;
        private int totalPositions;
        private List<List<Page>> splitPages;
        private List<Type> types;
        private List<Integer> sortChannels;
        private List<Type> sortTypes;
        private List<SortOrder> sortOrders;
        private List<Integer> outputChannels;

        protected void setup(int numSortChannels, int totalChannels, int numMergeSources, int pagesCount)
        {
            types = nCopies(totalChannels, BIGINT);
            sortChannels = new ArrayList<>();
            for (int i = 0; i < numSortChannels; i++) {
                sortChannels.add(i);
            }
            sortTypes = nCopies(numSortChannels, BIGINT);
            sortOrders = nCopies(numSortChannels, ASC_NULLS_FIRST);
            outputChannels = new ArrayList<>();
            for (int i = 0; i < totalChannels; i++) {
                outputChannels.add(i);
            }

            createPages(totalChannels, pagesCount);
            createPageProducers(numMergeSources);
        }

        private void createPages(int totalChannels, int pagesCount)
        {
            int positionCount = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES / (totalChannels * 8);
            pages = new ArrayList<>(pagesCount);
            for (int numPage = 0; numPage < pagesCount; numPage++) {
                pages.add(createSequencePage(types, positionCount));
            }
            totalPositions = positionCount * pagesCount;
        }

        private void createPageProducers(int numMergeSources)
        {
            AtomicInteger counter = new AtomicInteger(0);
            splitPages = pages.stream()
                    .collect(Collectors.groupingBy(it -> counter.getAndIncrement() % numMergeSources))
                    .values().stream()
                    .collect(toImmutableList());
        }

        List<Page> getPages()
        {
            return pages;
        }

        int getTotalPositions()
        {
            return totalPositions;
        }

        List<List<Page>> getSplitPages()
        {
            return splitPages;
        }

        List<Type> getTypes()
        {
            return types;
        }

        List<Integer> getSortChannels()
        {
            return sortChannels;
        }

        List<Type> getSortTypes()
        {
            return sortTypes;
        }

        List<SortOrder> getSortOrders()
        {
            return sortOrders;
        }

        List<Integer> getOutputChannels()
        {
            return outputChannels;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + PagesSortBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
