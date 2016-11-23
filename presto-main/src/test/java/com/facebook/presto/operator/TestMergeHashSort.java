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

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMergeHashSort
{
    @Test
    public void testChannelIterator()
    {
        RowPagesBuilder pageBuilder = rowPagesBuilder(BIGINT);
        pageBuilder.addSequencePage(2, 2);
        pageBuilder.addSequencePage(2, 10);

        MergeHashSort.PagePositions iterator = new MergeHashSort.SingleChannelPagePositions(pageBuilder.build().iterator());

        assertTrue(iterator.hasNext());
        assertEquals(iterator.next().getPosition(), 0);

        assertTrue(iterator.hasNext());
        assertEquals(iterator.next().getPosition(), 1);

        assertTrue(iterator.hasNext());
        assertEquals(iterator.next().getPosition(), 0);

        assertTrue(iterator.hasNext());
        assertEquals(iterator.next().getPosition(), 1);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testBinaryMergeIteratorOverEmptyPage()
    {
        Page emptyPage = new Page(0, BIGINT.createFixedSizeBlockBuilder(0).build());

        Iterator<Page> mergedPage = MergeHashSort.merge(
                ImmutableList.of(BIGINT),
                ImmutableList.of(BIGINT),
                ImmutableList.of(ImmutableList.of(emptyPage).iterator()));

        assertTrue(mergedPage.hasNext());

        Page actualPage = mergedPage.next();
        assertEquals(actualPage.getPositionCount(), 0);

        assertFalse(mergedPage.hasNext());
    }

    @Test
    public void testBinaryMergeIteratorOverEmptyPageAndNonEmptyPage()
    {
        Page emptyPage = new Page(0, BIGINT.createFixedSizeBlockBuilder(0).build());
        Page page = rowPagesBuilder(BIGINT).row(42).build().get(0);

        Iterator<Page> mergedPage = MergeHashSort.merge(
                ImmutableList.of(BIGINT),
                ImmutableList.of(BIGINT),
                ImmutableList.of(ImmutableList.of(emptyPage, page).iterator()));

        assertTrue(mergedPage.hasNext());

        Page actualPage = mergedPage.next();
        assertEquals(actualPage.getPositionCount(), 1);
        assertEquals(actualPage.getChannelCount(), 1);
        assertEquals(actualPage.getBlock(0).getLong(0, 0), 42);

        assertFalse(mergedPage.hasNext());
    }

    @Test
    public void testPageRewriteIterator()
    {
        ImmutableList<Type> types = ImmutableList.of(BIGINT, BIGINT);
        RowPagesBuilder pagesBuilder = rowPagesBuilder(types);
        pagesBuilder.row(0, 42);
        pagesBuilder.row(0, 43);
        pagesBuilder.pageBreak();
        pagesBuilder.row(0, 44);
        pagesBuilder.row(1, 45);
        pagesBuilder.pageBreak();
        pagesBuilder.row(2, 46);

        Iterator<Page> rewriterIterator = new MergeHashSort.PageRewriteIterator(
                new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] { 0 }),
                types,
                new MergeHashSort.SingleChannelPagePositions(pagesBuilder.build().iterator()));

        List<Page> pages = Lists.newArrayList(rewriterIterator);
        assertEquals(pages.size(), 1);

        List<Page> expectedPages = rowPagesBuilder(types)
                .row(0, 42)
                .row(0, 43)
                .row(0, 44)
                .row(1, 45)
                .row(2, 46)
                .build();

        assertPageEquals(types, pages.get(0), expectedPages.get(0));
    }

    private long readBigint(MergeHashSort.PagePosition pagePosition)
    {
        return BIGINT.getLong(pagePosition.getPage().getBlock(0), pagePosition.getPosition());
    }
}
