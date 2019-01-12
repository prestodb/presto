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
package io.prestosql.operator.project;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterators.transform;
import static io.prestosql.SequencePageBuilder.createSequencePage;
import static io.prestosql.execution.buffer.PageSplitterUtil.splitPage;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static java.lang.Math.toIntExact;
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

        MergingPageOutput output = new MergingPageOutput(TYPES, page.getSizeInBytes(), Integer.MAX_VALUE, Integer.MAX_VALUE);

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

    private static Iterator<Optional<Page>> createPagesIterator(Page... pages)
    {
        return createPagesIterator(ImmutableList.copyOf(pages));
    }

    private static Iterator<Optional<Page>> createPagesIterator(List<Page> pages)
    {
        return transform(pages.iterator(), Optional::of);
    }
}
