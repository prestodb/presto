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

import com.facebook.presto.operator.CompletedWork;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createSlicesBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static com.facebook.presto.operator.project.PageProcessor.MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.operator.project.PageProcessor.MIN_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.operator.project.SelectedPositions.positionsRange;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.join;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static sun.misc.Unsafe.ARRAY_OBJECT_INDEX_SCALE;

public class TestPageProcessor
{
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-%s"));

    @Test
    public void testProjectNoColumns()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of());

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        Page outputPage = outputPages.get(0).orElse(null);
        assertEquals(outputPage.getChannelCount(), 0);
        assertEquals(outputPage.getPositionCount(), inputPage.getPositionCount());
    }

    @Test
    public void testFilterNoColumns()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new TestingPageFilter(positionsRange(0, 50))), ImmutableList.of());

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        Page outputPage = outputPages.get(0).orElse(null);
        assertEquals(outputPage.getChannelCount(), 0);
        assertEquals(outputPage.getPositionCount(), 50);
    }

    @Test
    public void testPartialFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new TestingPageFilter(positionsRange(25, 50))), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0).orElse(null), new Page(createLongSequenceBlock(25, 75)));
    }

    @Test
    public void testSelectAllFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0).orElse(null), new Page(createLongSequenceBlock(0, 100)));
    }

    @Test
    public void testSelectNoneFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectNoneFilter()), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), 0);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 0);
    }

    @Test
    public void testProjectEmptyPage()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        Page inputPage = new Page(createLongSequenceBlock(0, 0));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), 0);

        // output should be one page containing no columns (only a count)
        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 0);
    }

    @Test
    public void testSelectNoneFilterLazyLoad()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectNoneFilter()), ImmutableList.of(new InputPageProjection(1, BIGINT)));

        // if channel 1 is loaded, test will fail
        Page inputPage = new Page(createLongSequenceBlock(0, 100), new LazyBlock(100, lazyBlock -> {
            throw new AssertionError("Lazy block should not be loaded");
        }));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), 0);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 0);
    }

    @Test
    public void testProjectLazyLoad()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(new LazyPagePageProjection()));

        // if channel 1 is loaded, test will fail
        Page inputPage = new Page(createLongSequenceBlock(0, 100), new LazyBlock(100, lazyBlock -> {
            throw new AssertionError("Lazy block should not be loaded");
        }));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), new Page(createLongSequenceBlock(0, 100)).getRetainedSizeInBytes() + ARRAY_OBJECT_INDEX_SCALE);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0).orElse(null), new Page(createLongSequenceBlock(0, 100)));
    }

    @Test
    public void testBatchedOutput()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        Page inputPage = new Page(createLongSequenceBlock(0, (int) (MAX_BATCH_SIZE * 2.5)));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 3);
        for (int i = 0; i < outputPages.size(); i++) {
            Page actualPage = outputPages.get(i).orElse(null);
            int offset = i * MAX_BATCH_SIZE;
            Page expectedPage = new Page(createLongSequenceBlock(offset, offset + Math.min(inputPage.getPositionCount() - offset, MAX_BATCH_SIZE)));
            assertPageEquals(ImmutableList.of(BIGINT), actualPage, expectedPage);
        }
    }

    @Test
    public void testAdaptiveBatchSize()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of(new InputPageProjection(0, VARCHAR)));

        // process large page which will reduce batch size
        Slice[] slices = new Slice[(int) (MAX_BATCH_SIZE * 2.5)];
        Arrays.fill(slices, Slices.allocate(1024));
        Page inputPage = new Page(createSlicesBlock(slices));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        int batchSize = MAX_BATCH_SIZE;
        for (Optional<Page> actualPage : outputPages) {
            Page expectedPage = new Page(createSlicesBlock(Arrays.copyOfRange(slices, 0, batchSize)));
            assertPageEquals(ImmutableList.of(VARCHAR), actualPage.orElse(null), expectedPage);
            if (actualPage.orElseThrow(() -> new AssertionError("page is not present")).getSizeInBytes() > MAX_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize / 2;
            }
        }

        // process small page which will increase batch size
        Arrays.fill(slices, Slices.allocate(128));
        inputPage = new Page(createSlicesBlock(slices));

        output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        outputPages = ImmutableList.copyOf(output);
        int offset = 0;
        for (Optional<Page> actualPage : outputPages) {
            Page expectedPage = new Page(createSlicesBlock(Arrays.copyOfRange(slices, 0, Math.min(inputPage.getPositionCount() - offset, batchSize))));
            assertPageEquals(ImmutableList.of(VARCHAR), actualPage.orElse(null), expectedPage);
            offset += actualPage.orElseThrow(() -> new AssertionError("page is not present")).getPositionCount();
            if (actualPage.orElseThrow(() -> new AssertionError("page is not present")).getSizeInBytes() < MIN_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize * 2;
            }
        }
    }

    @Test
    public void testOptimisticProcessing()
    {
        InvocationCountPageProjection firstProjection = new InvocationCountPageProjection(new InputPageProjection(0, VARCHAR));
        InvocationCountPageProjection secondProjection = new InvocationCountPageProjection(new InputPageProjection(0, VARCHAR));
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of(firstProjection, secondProjection));

        // process large page which will reduce batch size
        Slice[] slices = new Slice[(int) (MAX_BATCH_SIZE * 2.5)];
        Arrays.fill(slices, Slices.allocate(1024));
        Page inputPage = new Page(createSlicesBlock(slices));

        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        // batch size will be reduced before the first page is produced until the first block is within the page size bounds
        int batchSize = MAX_BATCH_SIZE;
        while (inputPage.getBlock(0).getRegionSizeInBytes(0, batchSize) > MAX_PAGE_SIZE_IN_BYTES) {
            batchSize /= 2;
        }

        int pageCount = 0;
        while (output.hasNext()) {
            Page actualPage = output.next().orElse(null);
            Block sliceBlock = createSlicesBlock(Arrays.copyOfRange(slices, 0, batchSize));
            Page expectedPage = new Page(sliceBlock, sliceBlock);
            assertPageEquals(ImmutableList.of(VARCHAR, VARCHAR), actualPage, expectedPage);
            pageCount++;

            // batch size will be further reduced to fit withing the bounds
            if (actualPage.getSizeInBytes() > MAX_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize / 2;
            }
        }
        // second project is invoked once per output page
        assertEquals(secondProjection.getInvocationCount(), pageCount);

        // the page processor saves the results when the page size is exceeded, so the first projection
        // will be invoked less times
        assertTrue(firstProjection.getInvocationCount() < secondProjection.getInvocationCount());
    }

    @Test
    public void testRetainedSize()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(new InputPageProjection(0, VARCHAR), new InputPageProjection(1, VARCHAR)));

        // create 2 columns X 800 rows of strings with each string's size = 10KB
        // this can force previouslyComputedResults to be saved given the page is 16MB in size
        String value = join("", nCopies(10_000, "a"));
        List<String> values = nCopies(800, value);
        Page inputPage = new Page(createStringsBlock(values), createStringsBlock(values));
        PageProcessorOutput output = pageProcessor.process(SESSION, new DriverYieldSignal(), inputPage);

        // force a compute
        // one block of previouslyComputedResults will be saved given the first column is with 8MB
        output.hasNext();

        // verify we do not count block sizes twice
        // comparing with the input page, the output page also contains an extra instance size for previouslyComputedResults
        assertEquals(output.getRetainedSizeInBytes() - ClassLayout.parseClass(VariableWidthBlock.class).instanceSize(), inputPage.getRetainedSizeInBytes());
    }

    @Test
    public void testYieldProjection()
    {
        // each projection can finish without yield
        // while between two projections, there is a yield
        int rows = 128;
        int columns = 20;
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Collections.nCopies(columns, new YieldPageProjection(new InputPageProjection(0, VARCHAR))));

        Slice[] slices = new Slice[rows];
        Arrays.fill(slices, Slices.allocate(rows));
        Page inputPage = new Page(createSlicesBlock(slices));

        PageProcessorOutput output = pageProcessor.process(SESSION, yieldSignal, inputPage);

        // Test yield signal works for page processor.
        // The purpose of this test is NOT to test the yield signal in page projection; we have other tests to cover that.
        // In page processor, we check yield signal after a column has been completely processed.
        // So we would like to set yield signal when the column has just finished processing in order to let page processor capture the yield signal when the block is returned.
        // Also, we would like to reset the yield signal before starting to process the next column in order NOT to yield per position inside the column.
        for (int i = 0; i < columns - 1; i++) {
            assertTrue(output.hasNext());
            assertNull(output.next().orElse(null));
            assertTrue(yieldSignal.isSet());
            yieldSignal.reset();
        }
        assertTrue(output.hasNext());
        Page actualPage = output.next().orElse(null);
        assertNotNull(actualPage);
        assertTrue(yieldSignal.isSet());
        yieldSignal.reset();

        Block[] blocks = new Block[columns];
        Arrays.fill(blocks, createSlicesBlock(Arrays.copyOfRange(slices, 0, rows)));
        Page expectedPage = new Page(blocks);
        assertPageEquals(Collections.nCopies(columns, VARCHAR), actualPage, expectedPage);
        assertFalse(output.hasNext());
    }

    private static class InvocationCountPageProjection
            implements PageProjection
    {
        protected final PageProjection delegate;
        private int invocationCount;

        public InvocationCountPageProjection(PageProjection delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public Type getType()
        {
            return delegate.getType();
        }

        @Override
        public boolean isDeterministic()
        {
            return delegate.isDeterministic();
        }

        @Override
        public InputChannels getInputChannels()
        {
            return delegate.getInputChannels();
        }

        @Override
        public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            setInvocationCount(getInvocationCount() + 1);
            return delegate.project(session, yieldSignal, page, selectedPositions);
        }

        public int getInvocationCount()
        {
            return invocationCount;
        }

        public void setInvocationCount(int invocationCount)
        {
            this.invocationCount = invocationCount;
        }
    }

    private class YieldPageProjection
            extends InvocationCountPageProjection
    {
        public YieldPageProjection(PageProjection delegate)
        {
            super(delegate);
        }

        @Override
        public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            return new YieldPageProjectionWork(session, yieldSignal, page, selectedPositions);
        }

        private class YieldPageProjectionWork
                implements Work<Block>
        {
            private final DriverYieldSignal yieldSignal;
            private final Work<Block> work;

            public YieldPageProjectionWork(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
            {
                this.yieldSignal = yieldSignal;
                this.work = delegate.project(session, yieldSignal, page, selectedPositions);
            }

            @Override
            public boolean process()
            {
                assertTrue(work.process());
                yieldSignal.setWithDelay(1, executor);
                yieldSignal.forceYieldForTesting();
                return true;
            }

            @Override
            public Block getResult()
            {
                return work.getResult();
            }
        }
    }

    public static class LazyPagePageProjection
            implements PageProjection
    {
        @Override
        public Type getType()
        {
            return BIGINT;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0, 1);
        }

        @Override
        public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            return new CompletedWork<>(page.getBlock(0).getLoadedBlock());
        }
    }

    private static class TestingPageFilter
            implements PageFilter
    {
        private final SelectedPositions selectedPositions;

        public TestingPageFilter(SelectedPositions selectedPositions)
        {
            this.selectedPositions = selectedPositions;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, Page page)
        {
            return selectedPositions;
        }
    }

    public static class SelectAllFilter
            implements PageFilter
    {
        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, Page page)
        {
            return positionsRange(0, page.getPositionCount());
        }
    }

    private static class SelectNoneFilter
            implements PageFilter
    {
        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, Page page)
        {
            return positionsRange(0, 0);
        }
    }
}
