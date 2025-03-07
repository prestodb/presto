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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPagesIndex
{
    @Test
    public void testEstimatedSize()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);

        PagesIndex pagesIndex = newPagesIndex(types, 30, false);
        long initialEstimatedSize = pagesIndex.getEstimatedSize().toBytes();
        assertTrue(initialEstimatedSize > 0, format("Initial estimated size must be positive, got %s", initialEstimatedSize));

        pagesIndex.addPage(somePage(types));
        long estimatedSizeWithOnePage = pagesIndex.getEstimatedSize().toBytes();
        assertTrue(estimatedSizeWithOnePage > initialEstimatedSize, "Estimated size should grow after adding a page");

        pagesIndex.addPage(somePage(types));
        long estimatedSizeWithTwoPages = pagesIndex.getEstimatedSize().toBytes();
        assertEquals(
                estimatedSizeWithTwoPages,
                initialEstimatedSize + (estimatedSizeWithOnePage - initialEstimatedSize) * 2,
                "Estimated size should grow linearly as long as we don't pass expectedPositions");

        pagesIndex.compact();
        long estimatedSizeAfterCompact = pagesIndex.getEstimatedSize().toBytes();
        // We can expect compact to reduce size because VARCHAR sequence pages are compactable.
        assertTrue(estimatedSizeAfterCompact < estimatedSizeWithTwoPages, format(
                "Compact should reduce (or retain) size, but changed from %s to %s",
                estimatedSizeWithTwoPages,
                estimatedSizeAfterCompact));
    }

    @Test
    public void testEagerCompact()
    {
        List<Type> types = ImmutableList.of(VARCHAR);

        PagesIndex lazyCompactPagesIndex = newPagesIndex(types, 50, false);
        PagesIndex eagerCompactPagesIndex = newPagesIndex(types, 50, true);

        for (int i = 0; i < 5; i++) {
            lazyCompactPagesIndex.addPage(somePage(types));
            eagerCompactPagesIndex.addPage(somePage(types));

            // We can expect eagerCompactPagesIndex retained less data than lazyCompactPagesIndex because
            // the pages used in the test (VARCHAR sequence pages) are compactable.
            assertTrue(
                    eagerCompactPagesIndex.getEstimatedSize().toBytes() < lazyCompactPagesIndex.getEstimatedSize().toBytes(),
                    "Expect eagerCompactPagesIndex retained less data than lazyCompactPagesIndex after adding the page, because the pages used in the test are compactable.");
        }

        lazyCompactPagesIndex.compact();
        assertEquals(lazyCompactPagesIndex.getEstimatedSize(), eagerCompactPagesIndex.getEstimatedSize());
    }

    private static PagesIndex newPagesIndex(List<Type> types, int expectedPositions, boolean eagerCompact)
    {
        return new PagesIndex.TestingFactory(eagerCompact).newPagesIndex(types, expectedPositions);
    }

    private static Page somePage(List<Type> types)
    {
        int[] initialValues = new int[types.size()];
        Arrays.setAll(initialValues, i -> 100 * i);
        return createSequencePage(types, 7, initialValues);
    }

    public static Object[][] cartesianProduct(Object[][]... args)
    {
        return Lists.cartesianProduct(Arrays.stream(args)
                        .map(ImmutableList::copyOf)
                        .collect(toImmutableList()))
                .stream()
                .map(list -> list.stream()
                        .flatMap(Stream::of)
                        .toArray(Object[]::new))
                .toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] testGetEstimatedLookupSourceSizeInBytesProvider()
    {
        return cartesianProduct(
                new Object[][] {{Optional.empty()}, {Optional.of(0)}, {Optional.of(1)}},
                new Object[][] {{0}, {1}});
    }

    @Test(dataProvider = "testGetEstimatedLookupSourceSizeInBytesProvider")
    public void testGetEstimatedLookupSourceSizeInBytes(Optional<Integer> sortChannel, int joinChannel)
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
        PagesIndex pagesIndex = newPagesIndex(types, 50, false);
        int pageCount = 100;
        for (int i = 0; i < pageCount; i++) {
            pagesIndex.addPage(somePage(types));
        }
        long pageIndexSize = pagesIndex.getEstimatedSize().toBytes();
        long estimatedMemoryRequiredToCreateLookupSource = pagesIndex.getEstimatedMemoryRequiredToCreateLookupSource(sortChannel);
        assertThat(estimatedMemoryRequiredToCreateLookupSource).isGreaterThan(pageIndexSize);
        long estimatedLookupSourceSize = estimatedMemoryRequiredToCreateLookupSource -
                // subtract size of page positions
                sizeOfIntArray(pageCount);

        JoinFilterFunctionFactory filterFunctionFactory = (session, addresses, pages) -> (JoinFilterFunction) (leftPosition, rightPosition, rightPage) -> false;
        LookupSource lookupSource = pagesIndex.createLookupSourceSupplier(
                TEST_SESSION,
                ImmutableList.of(joinChannel),
                OptionalInt.empty(),
                sortChannel.map(channel -> filterFunctionFactory),
                sortChannel,
                ImmutableList.of(filterFunctionFactory),
                Optional.of(ImmutableList.of(0, 1))).get();
        long actualLookupSourceSize = lookupSource.getInMemorySizeInBytes();
        assertThat(estimatedLookupSourceSize).isGreaterThanOrEqualTo(actualLookupSourceSize);
        assertThat(estimatedLookupSourceSize).isCloseTo(actualLookupSourceSize, withPercentage(2));
    }
}
