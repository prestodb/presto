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
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static com.facebook.presto.operator.project.PageProcessor.MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.operator.project.PageProcessor.MIN_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.operator.project.SelectedPositions.positionsRange;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPageProcessor
{
    @Test
    public void testProjectNoColumns()
            throws Exception
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of());

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        PageProcessorOutput output = pageProcessor.process(SESSION, inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Page> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        Page outputPage = outputPages.get(0);
        assertEquals(outputPage.getChannelCount(), 0);
        assertEquals(outputPage.getPositionCount(), inputPage.getPositionCount());
    }

    @Test
    public void testFilterNoColumns()
            throws Exception
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new TestingPageFilter(positionsRange(0, 50))), ImmutableList.of());

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        PageProcessorOutput output = pageProcessor.process(SESSION, inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Page> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        Page outputPage = outputPages.get(0);
        assertEquals(outputPage.getChannelCount(), 0);
        assertEquals(outputPage.getPositionCount(), 50);
    }

    @Test
    public void testPartialFilter()
            throws Exception
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new TestingPageFilter(positionsRange(25, 50))), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        PageProcessorOutput output = pageProcessor.process(SESSION, inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Page> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0), new Page(createLongSequenceBlock(25, 75)));
    }

    @Test
    public void testSelectAllFilter()
            throws Exception
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        PageProcessorOutput output = pageProcessor.process(SESSION, inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Page> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0), new Page(createLongSequenceBlock(0, 100)));
    }

    @Test
    public void testSelectNoneFilter()
            throws Exception
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectNoneFilter()), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        Page inputPage = new Page(createLongSequenceBlock(0, 100));

        PageProcessorOutput output = pageProcessor.process(SESSION, inputPage);
        assertEquals(output.getRetainedSizeInBytes(), 0);

        List<Page> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 0);
    }

    @Test
    public void testProjectEmptyPage()
            throws Exception
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        Page inputPage = new Page(createLongSequenceBlock(0, 0));

        PageProcessorOutput output = pageProcessor.process(SESSION, inputPage);
        assertEquals(output.getRetainedSizeInBytes(), 0);

        // output should be one page containing no columns (only a count)
        List<Page> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 0);
    }

    @Test
    public void testBatchedOutput()
            throws Exception
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        Page inputPage = new Page(createLongSequenceBlock(0, (int) (MAX_BATCH_SIZE * 2.5)));

        PageProcessorOutput output = pageProcessor.process(SESSION, inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Page> outputPages = ImmutableList.copyOf(output);
        assertEquals(outputPages.size(), 3);
        for (int i = 0; i < outputPages.size(); i++) {
            Page actualPage = outputPages.get(i);
            int offset = i * MAX_BATCH_SIZE;
            Page expectedPage = new Page(createLongSequenceBlock(offset, offset + Math.min(inputPage.getPositionCount() - offset, MAX_BATCH_SIZE)));
            assertPageEquals(ImmutableList.of(BIGINT), actualPage, expectedPage);
        }
    }

    @Test
    public void testAdaptiveBatchSize()
            throws Exception
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of(new InputPageProjection(0, VARCHAR)));

        // process large page which will reduce batch size
        Slice[] slices = new Slice[(int) (MAX_BATCH_SIZE * 2.5)];
        Arrays.fill(slices, Slices.allocate(1024));
        Page inputPage = new Page(new SliceArrayBlock(slices.length, slices));

        PageProcessorOutput output = pageProcessor.process(SESSION, inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        List<Page> outputPages = ImmutableList.copyOf(output);
        int batchSize = MAX_BATCH_SIZE;
        for (Page actualPage : outputPages) {
            Page expectedPage = new Page(new SliceArrayBlock(batchSize, slices));
            assertPageEquals(ImmutableList.of(VARCHAR), actualPage, expectedPage);
            if (actualPage.getSizeInBytes() > MAX_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize / 2;
            }
        }

        // process small page which will increase batch size
        Arrays.fill(slices, Slices.allocate(128));
        inputPage = new Page(new SliceArrayBlock(slices.length, slices));

        output = pageProcessor.process(SESSION, inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        outputPages = ImmutableList.copyOf(output);
        int offset = 0;
        for (Page actualPage : outputPages) {
            Page expectedPage = new Page(new SliceArrayBlock(Math.min(inputPage.getPositionCount() - offset, batchSize), slices));
            assertPageEquals(ImmutableList.of(VARCHAR), actualPage, expectedPage);
            offset += actualPage.getPositionCount();
            if (actualPage.getSizeInBytes() < MIN_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize * 2;
            }
        }
    }

    @Test
    public void testOptimisticProcessing()
            throws Exception
    {
        InvocationCountPageProjection firstProjection = new InvocationCountPageProjection(new InputPageProjection(0, VARCHAR));
        InvocationCountPageProjection secondProjection = new InvocationCountPageProjection(new InputPageProjection(0, VARCHAR));
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), ImmutableList.of(firstProjection, secondProjection));

        // process large page which will reduce batch size
        Slice[] slices = new Slice[(int) (MAX_BATCH_SIZE * 2.5)];
        Arrays.fill(slices, Slices.allocate(1024));
        Page inputPage = new Page(new SliceArrayBlock(slices.length, slices));

        PageProcessorOutput output = pageProcessor.process(SESSION, inputPage);
        assertEquals(output.getRetainedSizeInBytes(), inputPage.getRetainedSizeInBytes());

        // batch size will be reduced before the first page is produced until the first block is within the page size bounds
        int batchSize = MAX_BATCH_SIZE;
        while (inputPage.getBlock(0).getRegionSizeInBytes(0, batchSize) > MAX_PAGE_SIZE_IN_BYTES) {
            batchSize /= 2;
        }

        int pageCount = 0;
        while (output.hasNext()) {
            Page actualPage = output.next();
            Page expectedPage = new Page(new SliceArrayBlock(batchSize, slices), new SliceArrayBlock(batchSize, slices));
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

    private static class InvocationCountPageProjection
            implements PageProjection
    {
        private final PageProjection delegate;
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
        public Block project(ConnectorSession session, Page page, SelectedPositions selectedPositions)
        {
            setInvocationCount(getInvocationCount() + 1);
            return delegate.project(session, page, selectedPositions);
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
            return new InputChannels();
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, Page page)
        {
            return selectedPositions;
        }
    }

    private static class SelectAllFilter
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
            return new InputChannels();
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
            return new InputChannels();
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, Page page)
        {
            return positionsRange(0, 0);
        }
    }
}
