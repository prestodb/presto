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
import com.facebook.presto.common.array.AdaptiveLongBigArray;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;

public class TestPositionLinks
{
    private static final Page TEST_PAGE = getOnlyElement(RowPagesBuilder.rowPagesBuilder(BIGINT).addSequencePage(20, 0).build());

    @Test
    public void testArrayPositionLinks()
    {
        PositionLinks.FactoryBuilder factoryBuilder = ArrayPositionLinks.builder(1000);

        assertEquals(factoryBuilder.link(1, 0), 1);
        assertEquals(factoryBuilder.link(2, 1), 2);
        assertEquals(factoryBuilder.link(3, 2), 3);

        assertEquals(factoryBuilder.link(11, 10), 11);
        assertEquals(factoryBuilder.link(12, 11), 12);

        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of());

        assertEquals(positionLinks.start(3, 0, TEST_PAGE), 3);
        assertEquals(positionLinks.next(3, 0, TEST_PAGE), 2);
        assertEquals(positionLinks.next(2, 0, TEST_PAGE), 1);
        assertEquals(positionLinks.next(1, 0, TEST_PAGE), 0);

        assertEquals(positionLinks.start(4, 0, TEST_PAGE), 4);
        assertEquals(positionLinks.next(4, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(12, 0, TEST_PAGE), 12);
        assertEquals(positionLinks.next(12, 0, TEST_PAGE), 11);
        assertEquals(positionLinks.next(11, 0, TEST_PAGE), 10);
    }

    @Test
    public void testSortedPositionLinks()
    {
        JoinFilterFunction filterFunction = (leftAddress, rightPosition, rightPage) ->
                BIGINT.getLong(TEST_PAGE.getBlock(0), leftAddress) > 4;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunction));

        assertEquals(positionLinks.start(0, 0, TEST_PAGE), 5);
        assertEquals(positionLinks.next(5, 0, TEST_PAGE), 6);
        assertEquals(positionLinks.next(6, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(7, 0, TEST_PAGE), 7);
        assertEquals(positionLinks.next(7, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(8, 0, TEST_PAGE), 8);
        assertEquals(positionLinks.next(8, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(9, 0, TEST_PAGE), 9);
        assertEquals(positionLinks.next(9, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(10, 0, TEST_PAGE), 10);
        assertEquals(positionLinks.next(10, 0, TEST_PAGE), 11);
        assertEquals(positionLinks.next(11, 0, TEST_PAGE), 12);
        assertEquals(positionLinks.next(12, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(13, 0, TEST_PAGE), 13);
        assertEquals(positionLinks.next(13, 0, TEST_PAGE), -1);
    }

    @Test
    public void testSortedPositionLinksAllMatch()
    {
        JoinFilterFunction filterFunction = (leftAddress, rightPosition, rightPage) ->
                BIGINT.getLong(rightPage.getBlock(0), leftAddress) >= 0;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunction));

        assertEquals(positionLinks.start(0, 0, TEST_PAGE), 0);
        assertEquals(positionLinks.next(0, 0, TEST_PAGE), 1);
        assertEquals(positionLinks.next(1, 0, TEST_PAGE), 2);
        assertEquals(positionLinks.next(2, 0, TEST_PAGE), 3);
        assertEquals(positionLinks.next(3, 0, TEST_PAGE), 4);
        assertEquals(positionLinks.next(4, 0, TEST_PAGE), 5);
        assertEquals(positionLinks.next(5, 0, TEST_PAGE), 6);
        assertEquals(positionLinks.next(6, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(7, 0, TEST_PAGE), 7);
        assertEquals(positionLinks.next(7, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(8, 0, TEST_PAGE), 8);
        assertEquals(positionLinks.next(8, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(9, 0, TEST_PAGE), 9);
        assertEquals(positionLinks.next(9, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(10, 0, TEST_PAGE), 10);
        assertEquals(positionLinks.next(10, 0, TEST_PAGE), 11);
        assertEquals(positionLinks.next(11, 0, TEST_PAGE), 12);
        assertEquals(positionLinks.next(12, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(13, 0, TEST_PAGE), 13);
        assertEquals(positionLinks.next(13, 0, TEST_PAGE), -1);
    }

    @Test
    public void testSortedPositionLinksForRangePredicates()
    {
        JoinFilterFunction filterFunctionOne = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(TEST_PAGE.getBlock(0), leftAddress) > 4;

        JoinFilterFunction filterFunctionTwo = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(TEST_PAGE.getBlock(0), leftAddress) <= 11;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunctionOne, filterFunctionTwo));

        assertEquals(positionLinks.start(0, 0, TEST_PAGE), 5);
        assertEquals(positionLinks.next(4, 0, TEST_PAGE), 5);
        assertEquals(positionLinks.next(5, 0, TEST_PAGE), 6);
        assertEquals(positionLinks.next(6, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(7, 0, TEST_PAGE), 7);
        assertEquals(positionLinks.next(7, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(8, 0, TEST_PAGE), 8);
        assertEquals(positionLinks.next(8, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(9, 0, TEST_PAGE), 9);
        assertEquals(positionLinks.next(9, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(10, 0, TEST_PAGE), 10);
        assertEquals(positionLinks.next(10, 0, TEST_PAGE), 11);
        assertEquals(positionLinks.next(11, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(13, 0, TEST_PAGE), -1);
    }

    @Test
    public void testSortedPositionLinksForRangePredicatesPrefixMatch()
    {
        JoinFilterFunction filterFunctionOne = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), leftAddress) >= 0;

        JoinFilterFunction filterFunctionTwo = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), leftAddress) <= 11;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunctionOne, filterFunctionTwo));

        assertEquals(positionLinks.start(0, 0, TEST_PAGE), 0);
        assertEquals(positionLinks.next(0, 0, TEST_PAGE), 1);
        assertEquals(positionLinks.next(1, 0, TEST_PAGE), 2);
        assertEquals(positionLinks.next(2, 0, TEST_PAGE), 3);
        assertEquals(positionLinks.next(3, 0, TEST_PAGE), 4);
        assertEquals(positionLinks.next(4, 0, TEST_PAGE), 5);
        assertEquals(positionLinks.next(5, 0, TEST_PAGE), 6);
        assertEquals(positionLinks.next(6, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(7, 0, TEST_PAGE), 7);
        assertEquals(positionLinks.next(7, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(8, 0, TEST_PAGE), 8);
        assertEquals(positionLinks.next(8, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(9, 0, TEST_PAGE), 9);
        assertEquals(positionLinks.next(9, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(10, 0, TEST_PAGE), 10);
        assertEquals(positionLinks.next(10, 0, TEST_PAGE), 11);
        assertEquals(positionLinks.next(11, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(13, 0, TEST_PAGE), -1);
    }

    @Test
    public void testSortedPositionLinksForRangePredicatesSuffixMatch()
    {
        JoinFilterFunction filterFunctionOne = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), leftAddress) > 4;

        JoinFilterFunction filterFunctionTwo = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), leftAddress) < 100;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunctionOne, filterFunctionTwo));

        assertEquals(positionLinks.start(0, 0, TEST_PAGE), 5);
        assertEquals(positionLinks.next(4, 0, TEST_PAGE), 5);
        assertEquals(positionLinks.next(5, 0, TEST_PAGE), 6);
        assertEquals(positionLinks.next(6, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(7, 0, TEST_PAGE), 7);
        assertEquals(positionLinks.next(7, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(8, 0, TEST_PAGE), 8);
        assertEquals(positionLinks.next(8, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(9, 0, TEST_PAGE), 9);
        assertEquals(positionLinks.next(9, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(10, 0, TEST_PAGE), 10);
        assertEquals(positionLinks.next(10, 0, TEST_PAGE), 11);
        assertEquals(positionLinks.next(11, 0, TEST_PAGE), 12);
        assertEquals(positionLinks.next(12, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(13, 0, TEST_PAGE), 13);
        assertEquals(positionLinks.next(13, 0, TEST_PAGE), -1);
    }

    @Test
    public void testReverseSortedPositionLinks()
    {
        JoinFilterFunction filterFunction = (leftAddress, rightPosition, rightPage) ->
                BIGINT.getLong(TEST_PAGE.getBlock(0), leftAddress) < 4;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunction));

        assertEquals(positionLinks.start(0, 0, TEST_PAGE), 0);
        assertEquals(positionLinks.next(0, 0, TEST_PAGE), 1);
        assertEquals(positionLinks.next(1, 0, TEST_PAGE), 2);
        assertEquals(positionLinks.next(2, 0, TEST_PAGE), 3);
        assertEquals(positionLinks.next(3, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(10, 0, TEST_PAGE), -1);
    }

    @Test
    public void testReverseSortedPositionLinksAllMatch()
    {
        JoinFilterFunction filterFunction = (leftAddress, rightPosition, rightPage) ->
                BIGINT.getLong(rightPage.getBlock(0), leftAddress) < 13;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunction));

        assertEquals(positionLinks.start(0, 0, TEST_PAGE), 0);
        assertEquals(positionLinks.next(0, 0, TEST_PAGE), 1);
        assertEquals(positionLinks.next(1, 0, TEST_PAGE), 2);
        assertEquals(positionLinks.next(2, 0, TEST_PAGE), 3);
        assertEquals(positionLinks.next(3, 0, TEST_PAGE), 4);
        assertEquals(positionLinks.next(4, 0, TEST_PAGE), 5);
        assertEquals(positionLinks.next(5, 0, TEST_PAGE), 6);
        assertEquals(positionLinks.next(6, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(7, 0, TEST_PAGE), 7);
        assertEquals(positionLinks.next(7, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(8, 0, TEST_PAGE), 8);
        assertEquals(positionLinks.next(8, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(9, 0, TEST_PAGE), 9);
        assertEquals(positionLinks.next(9, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(10, 0, TEST_PAGE), 10);
        assertEquals(positionLinks.next(10, 0, TEST_PAGE), 11);
        assertEquals(positionLinks.next(11, 0, TEST_PAGE), 12);
        assertEquals(positionLinks.next(12, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(13, 0, TEST_PAGE), -1);
    }

    private static PositionLinks.FactoryBuilder buildSortedPositionLinks()
    {
        SortedPositionLinks.FactoryBuilder builder = SortedPositionLinks.builder(
                1000,
                pagesHashStrategy(),
                addresses());

        /*
         * Built sorted positions links
         *
         * [0] -> [1,2,3,4,5,6]
         * [10] -> [11,12]
         */

        assertEquals(builder.link(4, 5), 4);
        assertEquals(builder.link(6, 4), 4);
        assertEquals(builder.link(2, 4), 2);
        assertEquals(builder.link(3, 2), 2);
        assertEquals(builder.link(0, 2), 0);
        assertEquals(builder.link(1, 0), 0);

        assertEquals(builder.link(10, 11), 10);
        assertEquals(builder.link(12, 10), 10);

        return builder;
    }

    private static PagesHashStrategy pagesHashStrategy()
    {
        return new SimplePagesHashStrategy(
                ImmutableList.of(BIGINT),
                ImmutableList.of(),
                ImmutableList.of(ImmutableList.of(TEST_PAGE.getBlock(0))),
                ImmutableList.of(),
                OptionalInt.empty(),
                Optional.of(0),
                MetadataManager.createTestMetadataManager().getFunctionAndTypeManager(),
                new FeaturesConfig().isGroupByUsesEqualTo());
    }

    private static AdaptiveLongBigArray addresses()
    {
        AdaptiveLongBigArray addresses = new AdaptiveLongBigArray();
        addresses.ensureCapacity(TEST_PAGE.getPositionCount());
        for (int i = 0; i < TEST_PAGE.getPositionCount(); ++i) {
            addresses.set(i, encodeSyntheticAddress(0, i));
        }
        return addresses;
    }
}
