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
import com.facebook.presto.sql.planner.SortExpressionExtractor.SortExpression;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
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

        PositionLinks positionLinks = factoryBuilder.build().create(Optional.empty());

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
        JoinFilterFunction filterFunction = new JoinFilterFunction()
        {
            @Override
            public boolean filter(int leftAddress, int rightPosition, Page rightPage)
            {
                return BIGINT.getLong(rightPage.getBlock(0), leftAddress) > 4;
            }

            @Override
            public Optional<Integer> getSortChannel()
            {
                throw new UnsupportedOperationException();
            }
        };

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(Optional.of(filterFunction));

        assertEquals(positionLinks.start(0, 0, TEST_PAGE), 5);
        assertEquals(positionLinks.next(5, 0, TEST_PAGE), 6);
        assertEquals(positionLinks.next(6, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(10, 0, TEST_PAGE), 10);
        assertEquals(positionLinks.next(10, 0, TEST_PAGE), 11);
        assertEquals(positionLinks.next(11, 0, TEST_PAGE), 12);
        assertEquals(positionLinks.next(12, 0, TEST_PAGE), -1);
    }

    @Test
    public void testReverseSortedPositionLinks()
    {
        JoinFilterFunction filterFunction = new JoinFilterFunction()
        {
            @Override
            public boolean filter(int leftAddress, int rightPosition, Page rightPage)
            {
                return BIGINT.getLong(rightPage.getBlock(0), leftAddress) < 4;
            }

            @Override
            public Optional<Integer> getSortChannel()
            {
                throw new UnsupportedOperationException();
            }
        };

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(Optional.of(filterFunction));

        assertEquals(positionLinks.start(0, 0, TEST_PAGE), 0);
        assertEquals(positionLinks.next(0, 0, TEST_PAGE), 1);
        assertEquals(positionLinks.next(1, 0, TEST_PAGE), 2);
        assertEquals(positionLinks.next(2, 0, TEST_PAGE), 3);
        assertEquals(positionLinks.next(3, 0, TEST_PAGE), -1);

        assertEquals(positionLinks.start(10, 0, TEST_PAGE), -1);
    }

    private static PositionLinks.FactoryBuilder buildSortedPositionLinks()
    {
        SortedPositionLinks.FactoryBuilder builder = SortedPositionLinks.builder(
                1000,
                pagesHashStrategy(),
                addresses());

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
                Optional.empty(),
                Optional.of(new SortExpression(0)));
    }

    private static LongArrayList addresses()
    {
        LongArrayList addresses = new LongArrayList();
        for (int i = 0; i < TEST_PAGE.getPositionCount(); ++i) {
            addresses.add(encodeSyntheticAddress(0, i));
        }
        return addresses;
    }
}
