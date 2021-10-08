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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.google.common.collect.Iterators.transform;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestMergingPageOutput
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, REAL, DOUBLE);

    @Test
    public void testMinPageSizeThreshold()
    {
        Page page = createSequencePage(TYPES, 10);

        MergingPageOutput output = new MergingPageOutput(TYPES, page.getSizeInBytes(), PageProcessor.MAX_BATCH_SIZE, Integer.MAX_VALUE);

        assertTrue(output.needsInput());
        assertNull(output.getOutput());

        output.addInput(createPagesIterator(page));
        assertFalse(output.needsInput());
        assertSame(output.getOutput(), page);
    }

    @Test
    public void testMinRowCountThreshold()
    {
        Page page = createSequencePage(TYPES, 10);

        MergingPageOutput output = new MergingPageOutput(TYPES, 1024 * 1024, page.getPositionCount(), Integer.MAX_VALUE);

        assertTrue(output.needsInput());
        assertNull(output.getOutput());

        output.addInput(createPagesIterator(page));
        assertFalse(output.needsInput());
        assertSame(output.getOutput(), page);
    }

    @Test
    public void testBufferSmallPages()
    {
        int singlePageRowCount = 10;
        Page page = createSequencePage(TYPES, singlePageRowCount * 2);
        List<Page> splits = splitPage(page, page.getSizeInBytes() / 2);

        MergingPageOutput output = new MergingPageOutput(TYPES, page.getSizeInBytes() + 1, page.getPositionCount() + 1, Integer.MAX_VALUE);

        assertTrue(output.needsInput());
        assertNull(output.getOutput());

        output.addInput(createPagesIterator(splits.get(0)));
        assertFalse(output.needsInput());
        assertNull(output.getOutput());
        assertTrue(output.needsInput());

        output.addInput(createPagesIterator(splits.get(1)));
        assertFalse(output.needsInput());
        assertNull(output.getOutput());

        output.finish();

        assertFalse(output.needsInput());
        assertPageEquals(TYPES, output.getOutput(), page);
    }

    @Test
    public void testFlushOnBigPage()
    {
        Page smallPage = createSequencePage(TYPES, 10);
        Page bigPage = createSequencePage(TYPES, 100);

        MergingPageOutput output = new MergingPageOutput(TYPES, bigPage.getSizeInBytes(), bigPage.getPositionCount(), Integer.MAX_VALUE);

        assertTrue(output.needsInput());
        assertNull(output.getOutput());

        output.addInput(createPagesIterator(smallPage));
        assertFalse(output.needsInput());
        assertNull(output.getOutput());
        assertTrue(output.needsInput());

        output.addInput(createPagesIterator(bigPage));
        assertFalse(output.needsInput());
        assertPageEquals(TYPES, output.getOutput(), smallPage);
        assertFalse(output.needsInput());
        assertSame(output.getOutput(), bigPage);
    }

    @Test
    public void testFlushOnFullPage()
    {
        int singlePageRowCount = 10;
        List<Type> types = ImmutableList.of(BIGINT);
        Page page = createSequencePage(types, singlePageRowCount * 2);
        List<Page> splits = splitPage(page, page.getSizeInBytes() / 2);

        MergingPageOutput output = new MergingPageOutput(types, page.getSizeInBytes() / 2 + 1, page.getPositionCount() / 2 + 1, toIntExact(page.getSizeInBytes()));

        assertTrue(output.needsInput());
        assertNull(output.getOutput());

        output.addInput(createPagesIterator(splits.get(0)));
        assertFalse(output.needsInput());
        assertNull(output.getOutput());
        assertTrue(output.needsInput());

        output.addInput(createPagesIterator(splits.get(1)));
        assertFalse(output.needsInput());
        assertPageEquals(types, output.getOutput(), page);

        output.addInput(createPagesIterator(splits.get(0), splits.get(1)));
        assertFalse(output.needsInput());
        assertPageEquals(types, output.getOutput(), page);
    }

    @Test
    public void testPositionCountOnly()
    {
        int minRowCount = 256;
        MergingPageOutput output = new MergingPageOutput(ImmutableList.of(), 1024 * 1024, minRowCount);

        Page bufferedPage = new Page(minRowCount - 1);
        output.addInput(createPagesIterator(bufferedPage));
        assertEquals(output.getRetainedSizeInBytes(), MergingPageOutput.INSTANCE_SIZE); // no page builder or outputQueue items
        assertNull(output.getOutput());
        assertTrue(output.needsInput());

        Page hugePage = new Page(PageProcessor.MAX_BATCH_SIZE * 2);
        output.addInput(createPagesIterator(hugePage));
        // Sufficiently large pages are passed through directly and not combined with pending counts
        assertSame(output.getOutput(), hugePage);
        assertNull(output.getOutput());

        // Pages >= minRowCount are passed through, but combined with accumulated input
        assertTrue(output.needsInput());
        output.addInput(createPagesIterator(new Page(minRowCount)));
        assertEquals(output.getOutput().getPositionCount(), minRowCount + bufferedPage.getPositionCount());
        assertNull(output.getOutput());

        // Small inputs accumulate until MAX_BATCH_SIZE is reached
        int combinedPositions = 0;
        while (combinedPositions + 100 < PageProcessor.MAX_BATCH_SIZE) {
            combinedPositions += 100;
            assertTrue(output.needsInput());
            output.addInput(createPagesIterator(new Page(100)));
            assertNull(output.getOutput());
        }
        assertTrue(output.needsInput());
        combinedPositions += 100;
        output.addInput(createPagesIterator(new Page(100)));
        assertEquals(output.getOutput().getPositionCount(), PageProcessor.MAX_BATCH_SIZE);
        output.finish();
        assertEquals(output.getOutput().getPositionCount(), combinedPositions - PageProcessor.MAX_BATCH_SIZE);
        assertTrue(output.isFinished());
    }

    private static Iterator<Optional<Page>> createPagesIterator(Page... pages)
    {
        return createPagesIterator(ImmutableList.copyOf(pages));
    }

    private static Iterator<Optional<Page>> createPagesIterator(List<Page> pages)
    {
        return transform(pages.iterator(), Optional::of);
    }
}
