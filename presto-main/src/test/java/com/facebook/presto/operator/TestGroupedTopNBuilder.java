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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.UpdateMemory.NOOP;
import static io.airlift.slice.SizeOf.sizeOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestGroupedTopNBuilder
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(GroupedTopNBuilder.class).instanceSize();
    private static final long INT_FIFO_QUEUE_SIZE = ClassLayout.parseClass(IntArrayFIFOQueue.class).instanceSize();
    private static final long OBJECT_OVERHEAD = ClassLayout.parseClass(Object.class).instanceSize();
    private static final long PAGE_REFERENCE_INSTANCE_SIZE = ClassLayout.parseClass(TestPageReference.class).instanceSize();

    @DataProvider
    public static Object[][] produceRowNumbers()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider
    public static Object[][] pageRowCounts()
    {
        // make either page or row count > 1024 to expand the big arrays
        return new Object[][] {{10000, 20}, {20, 10000}};
    }

    @Test
    public void testEmptyInput()
    {
        GroupedTopNBuilder groupedTopNBuilder = new GroupedTopNBuilder(
                ImmutableList.of(BIGINT),
                (left, leftPosition, right, rightPosition) -> {
                    throw new UnsupportedOperationException();
                },
                5,
                false,
                new NoChannelGroupByHash());
        assertFalse(groupedTopNBuilder.buildResult().hasNext());
    }

    @Test(dataProvider = "produceRowNumbers")
    public void testMultiGroupTopN(boolean produceRowNumbers)
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        List<Page> input = rowPagesBuilder(types)
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.9)
                .row(3L, 0.1)
                .pageBreak()
                .row(1L, 0.4)
                .pageBreak()
                .row(1L, 0.5)
                .row(1L, 0.6)
                .row(4L, 0.6)
                .row(2L, 0.8)
                .row(2L, 0.7)
                .pageBreak()
                .row(2L, 0.9)
                .build();

        for (Page page : input) {
            page.compact();
        }

        GroupByHash groupByHash = createGroupByHash(ImmutableList.of(types.get(0)), ImmutableList.of(0), NOOP);
        GroupedTopNBuilder groupedTopNBuilder = new GroupedTopNBuilder(
                types,
                new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                2,
                produceRowNumbers,
                groupByHash);
        assertBuilderSize(groupByHash, types, ImmutableList.of(), ImmutableList.of(), groupedTopNBuilder.getEstimatedSizeInBytes());

        // add 4 rows for the first page and created three heaps with 1, 1, 2 rows respectively
        assertTrue(groupedTopNBuilder.processPage(input.get(0)).process());
        assertBuilderSize(groupByHash, types, ImmutableList.of(4), ImmutableList.of(1, 1, 2), groupedTopNBuilder.getEstimatedSizeInBytes());

        // add 1 row for the second page and the three heaps become 2, 1, 2 rows respectively
        assertTrue(groupedTopNBuilder.processPage(input.get(1)).process());
        assertBuilderSize(groupByHash, types, ImmutableList.of(4, 1), ImmutableList.of(2, 1, 2), groupedTopNBuilder.getEstimatedSizeInBytes());

        // add 2 new rows for the third page (which will be compacted into two rows only) and we have four heaps with 2, 2, 2, 1 rows respectively
        assertTrue(groupedTopNBuilder.processPage(input.get(2)).process());
        assertBuilderSize(groupByHash, types, ImmutableList.of(4, 1, 2), ImmutableList.of(2, 2, 2, 1), groupedTopNBuilder.getEstimatedSizeInBytes());

        // the last page will be discarded
        assertTrue(groupedTopNBuilder.processPage(input.get(3)).process());
        assertBuilderSize(groupByHash, types, ImmutableList.of(4, 1, 2, 0), ImmutableList.of(2, 2, 2, 1), groupedTopNBuilder.getEstimatedSizeInBytes());

        List<Page> output = ImmutableList.copyOf(groupedTopNBuilder.buildResult());
        assertEquals(output.size(), 1);

        Page expected = rowPagesBuilder(BIGINT, DOUBLE, BIGINT)
                .row(1L, 0.3, 1)
                .row(1L, 0.4, 2)
                .row(2L, 0.2, 1)
                .row(2L, 0.7, 2)
                .row(3L, 0.1, 1)
                .row(3L, 0.9, 2)
                .row(4L, 0.6, 1)
                .build()
                .get(0);
        if (produceRowNumbers) {
            assertPageEquals(ImmutableList.of(BIGINT, DOUBLE, BIGINT), output.get(0), expected);
        }
        else {
            assertPageEquals(types, output.get(0), new Page(expected.getBlock(0), expected.getBlock(1)));
        }

        assertBuilderSize(groupByHash, types, ImmutableList.of(0, 0, 0, 0), ImmutableList.of(0, 0, 0, 0), groupedTopNBuilder.getEstimatedSizeInBytes());
    }

    @Test(dataProvider = "produceRowNumbers")
    public void testSingleGroupTopN(boolean produceRowNumbers)
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        List<Page> input = rowPagesBuilder(types)
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.9)
                .row(3L, 0.1)
                .pageBreak()
                .row(1L, 0.4)
                .pageBreak()
                .row(1L, 0.5)
                .row(1L, 0.6)
                .row(4L, 0.6)
                .row(2L, 0.8)
                .row(2L, 0.7)
                .pageBreak()
                .row(2L, 0.9)
                .build();

        for (Page page : input) {
            page.compact();
        }

        GroupedTopNBuilder groupedTopNBuilder = new GroupedTopNBuilder(
                types,
                new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                5,
                produceRowNumbers,
                new NoChannelGroupByHash());
        assertBuilderSize(new NoChannelGroupByHash(), types, ImmutableList.of(), ImmutableList.of(), groupedTopNBuilder.getEstimatedSizeInBytes());

        // add 4 rows for the first page and created a single heap with 4 rows
        assertTrue(groupedTopNBuilder.processPage(input.get(0)).process());
        assertBuilderSize(new NoChannelGroupByHash(), types, ImmutableList.of(4), ImmutableList.of(4), groupedTopNBuilder.getEstimatedSizeInBytes());

        // add 1 row for the second page and the heap is with 5 rows
        assertTrue(groupedTopNBuilder.processPage(input.get(1)).process());
        assertBuilderSize(new NoChannelGroupByHash(), types, ImmutableList.of(4, 1), ImmutableList.of(5), groupedTopNBuilder.getEstimatedSizeInBytes());

        // update 1 new row from the third page (which will be compacted into a single row only)
        assertTrue(groupedTopNBuilder.processPage(input.get(2)).process());
        assertBuilderSize(new NoChannelGroupByHash(), types, ImmutableList.of(4, 1, 1), ImmutableList.of(5), groupedTopNBuilder.getEstimatedSizeInBytes());

        // the last page will be discarded
        assertTrue(groupedTopNBuilder.processPage(input.get(3)).process());
        assertBuilderSize(new NoChannelGroupByHash(), types, ImmutableList.of(4, 1, 1), ImmutableList.of(5), groupedTopNBuilder.getEstimatedSizeInBytes());

        List<Page> output = ImmutableList.copyOf(groupedTopNBuilder.buildResult());
        assertEquals(output.size(), 1);

        Page expected = rowPagesBuilder(BIGINT, DOUBLE, BIGINT)
                .row(3L, 0.1, 1)
                .row(2L, 0.2, 2)
                .row(1L, 0.3, 3)
                .row(1L, 0.4, 4)
                .row(1L, 0.5, 5)
                .build()
                .get(0);
        if (produceRowNumbers) {
            assertPageEquals(ImmutableList.of(BIGINT, DOUBLE, BIGINT), output.get(0), expected);
        }
        else {
            assertPageEquals(types, output.get(0), new Page(expected.getBlock(0), expected.getBlock(1)));
        }

        assertBuilderSize(new NoChannelGroupByHash(), types, ImmutableList.of(0, 0, 0), ImmutableList.of(0), groupedTopNBuilder.getEstimatedSizeInBytes());
    }

    @Test
    public void testYield()
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        Page input = rowPagesBuilder(types)
                .row(1L, 0.3)
                .row(1L, 0.2)
                .row(1L, 0.9)
                .row(1L, 0.1)
                .build()
                .get(0);
        input.compact();

        AtomicBoolean unblock = new AtomicBoolean();
        GroupByHash groupByHash = createGroupByHash(ImmutableList.of(types.get(0)), ImmutableList.of(0), unblock::get);
        GroupedTopNBuilder groupedTopNBuilder = new GroupedTopNBuilder(
                types,
                new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                5,
                false,
                groupByHash);
        assertBuilderSize(groupByHash, types, ImmutableList.of(), ImmutableList.of(), groupedTopNBuilder.getEstimatedSizeInBytes());

        Work<?> work = groupedTopNBuilder.processPage(input);
        assertFalse(work.process());
        assertFalse(work.process());
        unblock.set(true);
        assertTrue(work.process());
        List<Page> output = ImmutableList.copyOf(groupedTopNBuilder.buildResult());
        assertEquals(output.size(), 1);

        Page expected = rowPagesBuilder(types)
                .row(1L, 0.1)
                .row(1L, 0.2)
                .row(1L, 0.3)
                .row(1L, 0.9)
                .build()
                .get(0);
        assertPageEquals(types, output.get(0), expected);
        assertBuilderSize(groupByHash, types, ImmutableList.of(0), ImmutableList.of(), groupedTopNBuilder.getEstimatedSizeInBytes());
    }

    @Test
    public void testAutoCompact()
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        List<Page> input = rowPagesBuilder(types)
                .row(1L, 0.8)
                .row(2L, 0.7)
                .row(3L, 0.9)
                .row(3L, 0.2)
                .row(3L, 0.2)
                .row(3L, 0.2)
                .row(3L, 0.2)
                .pageBreak()
                .row(3L, 0.8)
                .pageBreak()
                .row(2L, 0.6)
                .row(3L, 0.1)
                .pageBreak()
                .row(1L, 0.7)
                .pageBreak()
                .row(1L, 0.6)
                .build();

        GroupedTopNBuilder groupedTopNBuilder = new GroupedTopNBuilder(
                types,
                new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                1,
                false,
                createGroupByHash(ImmutableList.of(types.get(0)), ImmutableList.of(0), NOOP));

        // page 1:
        // the first page will be compacted
        assertTrue(groupedTopNBuilder.processPage(input.get(0)).process());
        assertEquals(groupedTopNBuilder.getBufferedPages().size(), 1);
        Page firstCompactPage = groupedTopNBuilder.getBufferedPages().get(0);
        Page expected = rowPagesBuilder(types)
                .row(1L, 0.8)
                .row(2L, 0.7)
                .row(3L, 0.2)
                .build()
                .get(0);
        assertPageEquals(types, firstCompactPage, expected);

        // page 2:
        // the second page will be removed
        assertTrue(groupedTopNBuilder.processPage(input.get(1)).process());
        assertEquals(groupedTopNBuilder.getBufferedPages().size(), 1);
        // assert the first page is not affected
        assertEquals(firstCompactPage, groupedTopNBuilder.getBufferedPages().get(0));

        // page 3:
        // the third page will trigger another compaction of the first page
        assertTrue(groupedTopNBuilder.processPage(input.get(2)).process());
        List<Page> bufferedPages = groupedTopNBuilder.getBufferedPages();
        assertEquals(bufferedPages.size(), 2);
        // assert the previously compacted first page no longer exists
        assertNotEquals(firstCompactPage, bufferedPages.get(0));
        assertNotEquals(firstCompactPage, bufferedPages.get(1));

        List<Page> expectedPages = rowPagesBuilder(types)
                .row(1L, 0.8)
                .pageBreak()
                .row(2L, 0.6)
                .row(3L, 0.1)
                .build();
        assertPageEquals(types, bufferedPages.get(0), expectedPages.get(0));
        assertPageEquals(types, bufferedPages.get(1), expectedPages.get(1));

        // page 4:
        // the fourth page will remove the first page; also it leaves it with an empty slot
        assertTrue(groupedTopNBuilder.processPage(input.get(3)).process());
        bufferedPages = groupedTopNBuilder.getBufferedPages();
        assertEquals(bufferedPages.size(), 2);

        expectedPages = rowPagesBuilder(types)
                .row(2L, 0.6)
                .row(3L, 0.1)
                .pageBreak()
                .row(1L, 0.7)
                .build();
        assertPageEquals(types, bufferedPages.get(0), expectedPages.get(0));
        assertPageEquals(types, bufferedPages.get(1), expectedPages.get(1));

        // page 5:
        // the fifth page will remove the fourth page and it will take the empty slot from the first page
        assertTrue(groupedTopNBuilder.processPage(input.get(4)).process());
        bufferedPages = groupedTopNBuilder.getBufferedPages();
        assertEquals(bufferedPages.size(), 2);

        // assert the fifth page indeed takes the first empty slot
        expectedPages = rowPagesBuilder(types)
                .row(1L, 0.6)
                .pageBreak()
                .row(2L, 0.6)
                .row(3L, 0.1)
                .build();
        assertPageEquals(types, bufferedPages.get(0), expectedPages.get(0));
        assertPageEquals(types, bufferedPages.get(1), expectedPages.get(1));
    }

    @Test(dataProvider = "pageRowCounts")
    public void testLargePagesMemoryTracking(int pageCount, int rowCount)
    {
        // Memory tracking has been tested in other tests for various cases (e.g., compaction, row removal, etc).
        // The purpose of this test is to verify:
        // (1) the sizes of containers (e.g., big array, heap, etc) are properly tracked when they grow and
        // (2) when we flush, the memory usage decreases accordingly.
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);

        // Create pageCount pages each page is with rowCount positions:
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(types);
        for (int i = 0; i < pageCount; i++) {
            rowPagesBuilder.addSequencePage(rowCount, 0, rowCount * i);
        }
        List<Page> input = rowPagesBuilder.build();

        GroupByHash groupByHash = createGroupByHash(ImmutableList.of(types.get(0)), ImmutableList.of(0), NOOP);
        GroupedTopNBuilder groupedTopNBuilder = new GroupedTopNBuilder(
                types,
                new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                pageCount * rowCount,
                false,
                groupByHash);

        // Assert memory usage gradually goes up
        for (int i = 0; i < pageCount; i++) {
            assertTrue(groupedTopNBuilder.processPage(input.get(i)).process());
            assertBuilderSize(groupByHash, types, Collections.nCopies(i + 1, rowCount), Collections.nCopies(rowCount, i + 1), groupedTopNBuilder.getEstimatedSizeInBytes());
        }

        // Assert memory usage gradually goes down (i.e., proportional to the number of rows/pages we have produced)
        int outputPageCount = 0;
        int remainingRows = pageCount * rowCount;
        Iterator<Page> output = groupedTopNBuilder.buildResult();
        while (output.hasNext()) {
            remainingRows -= output.next().getPositionCount();
            assertBuilderSize(
                    groupByHash,
                    types,
                    remainingRows == 0 ? Collections.nCopies(pageCount, 0) : Collections.nCopies(pageCount, rowCount),
                    new ImmutableList.Builder<Integer>()
                            .addAll(Collections.nCopies((remainingRows + pageCount - 1) / pageCount, pageCount))
                            .addAll(Collections.nCopies(rowCount - (remainingRows + pageCount - 1) / pageCount, 0))
                            .build(),
                    groupedTopNBuilder.getEstimatedSizeInBytes());
            outputPageCount++;
        }
        assertEquals(remainingRows, 0);
        assertGreaterThan(outputPageCount, 3);
        assertBuilderSize(groupByHash, types, Collections.nCopies(pageCount, 0), Collections.nCopies(rowCount, 0), groupedTopNBuilder.getEstimatedSizeInBytes());
    }

    private static GroupByHash createGroupByHash(List<Type> partitionTypes, List<Integer> partitionChannels, UpdateMemory updateMemory)
    {
        return GroupByHash.createGroupByHash(
                partitionTypes,
                Ints.toArray(partitionChannels),
                Optional.empty(),
                1,
                false,
                new JoinCompiler(createTestMetadataManager(), new FeaturesConfig()),
                updateMemory);
    }

    /**
     * Assert the retained size in Bytes of {@param builder} with
     * {@param groupByHash},
     * a list of {@param types} of the input pages,
     * a list of how many positions ({@param pagePositions}) of each page, and
     * a list of how many rows ({}@param rowCounts}) of each group.
     * Currently we do not assert the size of emptyPageReferenceSlots and assume the queue is always with INITIAL_CAPACITY = 4.
     */
    private static void assertBuilderSize(
            GroupByHash groupByHash,
            List<Type> types,
            List<Integer> pagePositions,
            List<Integer> rowCounts,
            long actualSizeInBytes)
    {
        ObjectBigArray<Object> pageReferences = new ObjectBigArray<>();
        pageReferences.ensureCapacity(pagePositions.size());
        long pageReferencesSizeInBytes = pageReferences.sizeOf();

        ObjectBigArray<Object> groupedRows = new ObjectBigArray<>();
        groupedRows.ensureCapacity(rowCounts.size());
        long groupedRowsSizeInBytes = groupedRows.sizeOf();

        int emptySlots = 4;
        long emptyPageReferenceSlotsSizeInBytes = INT_FIFO_QUEUE_SIZE + sizeOf(new int[emptySlots]);

        // build fake pages to get the real retained sizes
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(types);
        for (int pagePosition : pagePositions) {
            if (pagePosition > 0) {
                rowPagesBuilder.addSequencePage(pagePosition, new int[types.size()]);
            }
        }

        long referencedPagesSizeInBytes = 0;
        for (Page page : rowPagesBuilder.build()) {
            // each page reference is with two arrays and a page
            referencedPagesSizeInBytes += PAGE_REFERENCE_INSTANCE_SIZE +
                    page.getRetainedSizeInBytes() +
                    sizeOf(new Object[page.getPositionCount()]);
        }

        long rowHeapsSizeInBytes = 0;
        for (int count : rowCounts) {
            if (count > 0) {
                rowHeapsSizeInBytes += new TestRowHeap(count).getEstimatedSizeInBytes();
            }
        }

        long expectedSizeInBytes = INSTANCE_SIZE +
                groupByHash.getEstimatedSize() +
                referencedPagesSizeInBytes +
                rowHeapsSizeInBytes +
                pageReferencesSizeInBytes +
                groupedRowsSizeInBytes +
                emptyPageReferenceSlotsSizeInBytes;
        assertEquals(actualSizeInBytes, expectedSizeInBytes);
    }

    // this class is for memory tracking comparison
    private static class TestRowHeap
            extends ObjectHeapPriorityQueue<Object>
    {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(TestRowHeap.class).instanceSize();
        // each Row is with two integers
        private static final long ROW_ENTRY_SIZE = 2 * Integer.BYTES + OBJECT_OVERHEAD;

        private TestRowHeap(int count)
        {
            super((left, right) -> 0);
            for (int i = 0; i < count; i++) {
                enqueue(new Object());
            }
        }

        private long getEstimatedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(heap) + size() * ROW_ENTRY_SIZE;
        }
    }

    // this class is for memory tracking comparison
    private static class TestPageReference
    {
        // only need reference overhead
        private Object page;
        private Object reference;

        private int usedPositionCount;
    }
}
