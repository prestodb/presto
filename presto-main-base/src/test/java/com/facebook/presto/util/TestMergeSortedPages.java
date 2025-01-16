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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.PageWithPositionComparator;
import com.facebook.presto.operator.SimplePageWithPositionComparator;
import com.facebook.presto.operator.WorkProcessor;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_FIRST;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMergeSortedPages
{
    @Test
    public void testSingleStream()
            throws Exception
    {
        List<Type> types = ImmutableList.of(INTEGER, INTEGER);
        MaterializedResult actual = mergeSortedPages(
                types,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_FIRST, DESC_NULLS_FIRST),
                ImmutableList.of(
                        rowPagesBuilder(types)
                                .row(1, 4)
                                .row(2, 3)
                                .pageBreak()
                                .row(3, 2)
                                .row(4, 1)
                                .build()));
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
                .row(1, 4)
                .row(2, 3)
                .row(3, 2)
                .row(4, 1)
                .build();
        assertEquals(actual, expected);
    }

    @Test
    public void testSimpleTwoStreams()
            throws Exception
    {
        List<Type> types = ImmutableList.of(INTEGER);
        MaterializedResult actual = mergeSortedPages(
                types,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_FIRST),
                ImmutableList.of(
                        rowPagesBuilder(types)
                                .row(1)
                                .row(3)
                                .pageBreak()
                                .row(5)
                                .row(7)
                                .build(),
                        rowPagesBuilder(types)
                                .row(2)
                                .row(4)
                                .pageBreak()
                                .row(6)
                                .row(8)
                                .build()));
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
                .row(1)
                .row(2)
                .row(3)
                .row(4)
                .row(5)
                .row(6)
                .row(7)
                .row(8)
                .build();
        assertEquals(actual, expected);
    }

    @Test
    public void testMultipleStreams()
            throws Exception
    {
        List<Type> types = ImmutableList.of(INTEGER, INTEGER, INTEGER);
        MaterializedResult actual = mergeSortedPages(
                types,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_FIRST, DESC_NULLS_FIRST),
                ImmutableList.of(
                        rowPagesBuilder(types)
                                .row(1, 1, 2)
                                .pageBreak()
                                .pageBreak()
                                .row(8, 1, 1)
                                .row(19, 1, 3)
                                .row(27, 1, 4)
                                .row(41, 2, 5)
                                .pageBreak()
                                .row(55, 1, 2)
                                .row(89, 1, 3)
                                .row(100, 2, 6)
                                .row(100, 2, 8)
                                .row(101, 1, 4)
                                .row(202, 1, 3)
                                .row(399, 2, 2)
                                .pageBreak()
                                .row(400, 1, 1)
                                .row(401, 1, 7)
                                .pageBreak()
                                .row(402, 1, 6)
                                .build(),
                        rowPagesBuilder(types)
                                .pageBreak()
                                .row(2, 1, 2)
                                .row(8, 1, 1)
                                .row(19, 1, 3)
                                .row(25, 1, 4)
                                .row(26, 2, 5)
                                .pageBreak()
                                .row(56, 1, 2)
                                .row(66, 1, 3)
                                .row(77, 1, 4)
                                .row(88, 1, 3)
                                .row(99, 1, 1)
                                .pageBreak()
                                .row(99, 2, 2)
                                .row(100, 1, 7)
                                .build(),
                        rowPagesBuilder(types)
                                .row(8, 1, 1)
                                .row(88, 1, 3)
                                .pageBreak()
                                .row(89, 1, 3)
                                .pageBreak()
                                .row(90, 1, 3)
                                .pageBreak()
                                .row(91, 1, 4)
                                .row(92, 2, 5)
                                .pageBreak()
                                .row(93, 1, 2)
                                .row(94, 1, 3)
                                .row(95, 1, 4)
                                .row(97, 1, 3)
                                .row(98, 2, 2)
                                .row(100, 1, 7)
                                .build()));
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
                .row(1, 1, 2)
                .row(2, 1, 2)
                .row(8, 1, 1)
                .row(8, 1, 1)
                .row(8, 1, 1)
                .row(19, 1, 3)
                .row(19, 1, 3)
                .row(25, 1, 4)
                .row(26, 2, 5)
                .row(27, 1, 4)
                .row(41, 2, 5)
                .row(55, 1, 2)
                .row(56, 1, 2)
                .row(66, 1, 3)
                .row(77, 1, 4)
                .row(88, 1, 3)
                .row(88, 1, 3)
                .row(89, 1, 3)
                .row(89, 1, 3)
                .row(90, 1, 3)
                .row(91, 1, 4)
                .row(92, 2, 5)
                .row(93, 1, 2)
                .row(94, 1, 3)
                .row(95, 1, 4)
                .row(97, 1, 3)
                .row(98, 2, 2)
                .row(99, 1, 1)
                .row(99, 2, 2)
                .row(100, 2, 6)
                .row(100, 2, 8)
                .row(100, 1, 7)
                .row(100, 1, 7)
                .row(101, 1, 4)
                .row(202, 1, 3)
                .row(399, 2, 2)
                .row(400, 1, 1)
                .row(401, 1, 7)
                .row(402, 1, 6)
                .build();
        assertEquals(actual, expected);
    }

    @Test
    public void testEmptyStreams()
            throws Exception
    {
        List<Type> types = ImmutableList.of(INTEGER, BIGINT, DOUBLE);
        MaterializedResult actual = mergeSortedPages(
                types,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST),
                ImmutableList.of(
                        rowPagesBuilder(types)
                                .pageBreak()
                                .pageBreak()
                                .build(),
                        rowPagesBuilder(types)
                                .pageBreak()
                                .build(),
                        rowPagesBuilder(types)
                                .pageBreak()
                                .build(),
                        rowPagesBuilder(types)
                                .build()));
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
                .build();
        assertEquals(actual, expected);
    }

    @Test
    public void testDifferentTypes()
            throws Exception
    {
        List<Type> types = ImmutableList.of(DOUBLE, VARCHAR, INTEGER);
        MaterializedResult actual = mergeSortedPages(
                types,
                ImmutableList.of(2, 0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_FIRST, ASC_NULLS_FIRST),
                ImmutableList.of(
                        rowPagesBuilder(types)
                                .row(16.0, "a1", 16)
                                .row(8.0, "b1", 16)
                                .pageBreak()
                                .row(4.0, "c1", 16)
                                .row(4.0, "d1", 16)
                                .row(null, "d1", 8)
                                .row(16.0, "a1", 8)
                                .row(16.0, "b1", 8)
                                .row(16.0, "c1", 4)
                                .row(8.0, "d1", 4)
                                .row(16.0, "a1", 2)
                                .row(null, "a1", null)
                                .row(16.0, "a1", null)
                                .build(),
                        rowPagesBuilder(types)
                                .row(15.0, "a2", 17)
                                .row(9.0, "b2", 17)
                                .pageBreak()
                                .row(5.0, "c2", 17)
                                .row(5.0, "d2", 17)
                                .row(null, "d2", 8)
                                .row(17.0, "a0", 8)
                                .row(17.0, "b0", 8)
                                .row(17.0, "c0", 5)
                                .row(9.0, "d0", 5)
                                .row(17.0, "a0", 3)
                                .row(null, "a0", null)
                                .row(17.0, "a0", null)
                                .build()));
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
                .row(15.0, "a2", 17)
                .row(9.0, "b2", 17)
                .row(5.0, "c2", 17)
                .row(5.0, "d2", 17)
                .row(16.0, "a1", 16)
                .row(8.0, "b1", 16)
                .row(4.0, "c1", 16)
                .row(4.0, "d1", 16)
                .row(null, "d1", 8)
                .row(null, "d2", 8)
                .row(17.0, "a0", 8)
                .row(17.0, "b0", 8)
                .row(16.0, "a1", 8)
                .row(16.0, "b1", 8)
                .row(17.0, "c0", 5)
                .row(9.0, "d0", 5)
                .row(16.0, "c1", 4)
                .row(8.0, "d1", 4)
                .row(17.0, "a0", 3)
                .row(16.0, "a1", 2)
                .row(null, "a0", null)
                .row(null, "a1", null)
                .row(17.0, "a0", null)
                .row(16.0, "a1", null)
                .build();
        assertEquals(actual, expected);
    }

    @Test
    public void testSortingYields()
            throws Exception
    {
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        yieldSignal.forceYieldForTesting();

        List<Type> types = ImmutableList.of(INTEGER);
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                ImmutableList.of(WorkProcessor.fromIterable(rowPagesBuilder(types)
                        .row(1)
                        .build())),
                new SimplePageWithPositionComparator(types, ImmutableList.of(0), ImmutableList.of(DESC_NULLS_LAST)),
                ImmutableList.of(0),
                types,
                (pageBuilder, pageWithPosition) -> pageBuilder.isFull(),
                false,
                newSimpleAggregatedMemoryContext().newAggregatedMemoryContext(),
                yieldSignal);

        // yield signal is on
        assertFalse(mergedPages.process());
        yieldSignal.resetYieldForTesting();

        // page is produced
        assertTrue(mergedPages.process());
        assertFalse(mergedPages.isFinished());

        Page page = mergedPages.getResult();
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
                .row(1)
                .build();
        assertEquals(toMaterializedResult(TEST_SESSION, types, ImmutableList.of(page)), expected);

        // merge source finished
        assertTrue(mergedPages.process());
        assertTrue(mergedPages.isFinished());
    }

    @Test
    public void testMergeSortYieldingProgresses()
            throws Exception
    {
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        yieldSignal.forceYieldForTesting();
        List<Type> types = ImmutableList.of(INTEGER);
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                ImmutableList.of(WorkProcessor.fromIterable(rowPagesBuilder(types).build())),
                new SimplePageWithPositionComparator(types, ImmutableList.of(0), ImmutableList.of(DESC_NULLS_LAST)),
                ImmutableList.of(0),
                types,
                (pageBuilder, pageWithPosition) -> pageBuilder.isFull(),
                false,
                newSimpleAggregatedMemoryContext().newAggregatedMemoryContext(),
                yieldSignal);
        // yield signal is on
        assertFalse(mergedPages.process());
        // processor finishes computations (yield signal is still on, but previous process() call yielded)
        assertTrue(mergedPages.process());
        assertTrue(mergedPages.isFinished());
    }

    private static MaterializedResult mergeSortedPages(
            List<Type> types,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            List<List<Page>> sortedPages)
            throws Exception
    {
        List<WorkProcessor<Page>> pageProducers = sortedPages.stream()
                .map(WorkProcessor::fromIterable)
                .collect(toImmutableList());
        PageWithPositionComparator comparator = new SimplePageWithPositionComparator(types, sortChannels, sortOrder);

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newAggregatedMemoryContext();
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                pageProducers,
                comparator,
                types,
                memoryContext,
                new DriverYieldSignal());

        assertTrue(mergedPages.process());

        if (mergedPages.isFinished()) {
            return toMaterializedResult(TEST_SESSION, types, ImmutableList.of());
        }

        Page page = mergedPages.getResult();
        assertTrue(mergedPages.process());
        assertTrue(mergedPages.isFinished());
        assertEquals(memoryContext.getBytes(), 0L);

        return toMaterializedResult(TEST_SESSION, types, ImmutableList.of(page));
    }
}
