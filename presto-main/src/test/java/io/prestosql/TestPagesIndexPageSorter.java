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
package io.prestosql;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.prestosql.operator.PagesIndex;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_FIRST;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestPagesIndexPageSorter
{
    private static final PagesIndexPageSorter sorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));

    @Test
    public void testPageSorter()
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARCHAR);
        List<Integer> sortChannels = Ints.asList(0);
        List<SortOrder> sortOrders = ImmutableList.of(ASC_NULLS_FIRST);

        List<Page> inputPages = RowPagesBuilder.rowPagesBuilder(types)
                .row(2L, 1.1, "d")
                .row(1L, 2.2, "c")
                .pageBreak()
                .row(-2L, 2.2, "b")
                .row(-12L, 2.2, "a")
                .build();

        List<Page> expectedPages = RowPagesBuilder.rowPagesBuilder(types)
                .row(-12L, 2.2, "a")
                .row(-2L, 2.2, "b")
                .pageBreak()
                .row(1L, 2.2, "c")
                .row(2L, 1.1, "d")
                .build();

        assertSorted(inputPages, expectedPages, types, sortChannels, sortOrders, 100);
    }

    @Test
    public void testPageSorterMultipleChannels()
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARCHAR);
        List<Integer> sortChannels = Ints.asList(0, 1, 2);
        List<SortOrder> sortOrders = Collections.nCopies(sortChannels.size(), ASC_NULLS_FIRST);

        List<Page> inputPages = RowPagesBuilder.rowPagesBuilder(types)
                .row(2L, 1.1, "d")
                .row(1L, 2.2, "c")
                .pageBreak()
                .row(1L, 2.2, "b")
                .row(1L, 2.2, "a")
                .pageBreak()
                .row(1L, 2.2, null)
                .row(1L, null, "z")
                .row(1L, null, null)
                .build();

        List<Page> expectedPages = RowPagesBuilder.rowPagesBuilder(types)
                .row(1L, null, null)
                .row(1L, null, "z")
                .row(1L, 2.2, null)
                .row(1L, 2.2, "a")
                .row(1L, 2.2, "b")
                .row(1L, 2.2, "c")
                .row(2L, 1.1, "d")
                .build();
        assertSorted(inputPages, expectedPages, types, sortChannels, sortOrders, 100);
    }

    @Test
    public void testPageSorterSorted()
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARCHAR);
        List<Integer> sortChannels = Ints.asList(0);
        List<SortOrder> sortOrders = ImmutableList.of(ASC_NULLS_FIRST);

        List<Page> inputPages = RowPagesBuilder.rowPagesBuilder(types)
                .row(-12L, 2.2, "a")
                .row(-2L, 2.2, "b")
                .pageBreak()
                .row(1L, 2.2, "d")
                .row(2L, 1.1, "c")
                .build();

        List<Page> expectedPages = RowPagesBuilder.rowPagesBuilder(types)
                .row(-12L, 2.2, "a")
                .row(-2L, 2.2, "b")
                .row(1L, 2.2, "d")
                .row(2L, 1.1, "c")
                .build();

        assertSorted(inputPages, expectedPages, types, sortChannels, sortOrders, 100);
    }

    @Test
    public void testPageSorterForceExpansion()
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARCHAR);
        List<Integer> sortChannels = Ints.asList(0);
        List<SortOrder> sortOrders = ImmutableList.of(ASC_NULLS_FIRST);

        List<Page> inputPages = RowPagesBuilder.rowPagesBuilder(types)
                .row(2L, 1.1, "c")
                .row(1L, 2.2, "d")
                .pageBreak()
                .row(-2L, 2.2, "b")
                .row(-12L, 2.2, "a")
                .build();

        List<Page> expectedPages = RowPagesBuilder.rowPagesBuilder(types)
                .row(-12L, 2.2, "a")
                .row(-2L, 2.2, "b")
                .pageBreak()
                .row(1L, 2.2, "d")
                .row(2L, 1.1, "c")
                .build();

        assertSorted(inputPages, expectedPages, types, sortChannels, sortOrders, 2);
    }

    private static void assertSorted(List<Page> inputPages, List<Page> expectedPages, List<Type> types, List<Integer> sortChannels, List<SortOrder> sortOrders, int expectedPositions)
    {
        long[] sortedAddresses = sorter.sort(types, inputPages, sortChannels, sortOrders, expectedPositions);
        List<Page> outputPages = createOutputPages(types, inputPages, sortedAddresses);

        MaterializedResult expected = toMaterializedResult(TEST_SESSION, types, expectedPages);
        MaterializedResult actual = toMaterializedResult(TEST_SESSION, types, outputPages);
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    private static List<Page> createOutputPages(List<Type> types, List<Page> inputPages, long[] sortedAddresses)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        pageBuilder.reset();
        for (long address : sortedAddresses) {
            int index = sorter.decodePageIndex(address);
            int position = sorter.decodePositionIndex(address);

            Page page = inputPages.get(index);
            for (int i = 0; i < types.size(); i++) {
                Type type = types.get(i);
                type.appendTo(page.getBlock(i), position, pageBuilder.getBlockBuilder(i));
            }
            pageBuilder.declarePosition();
        }
        return ImmutableList.of(pageBuilder.build());
    }
}
