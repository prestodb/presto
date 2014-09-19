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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.RowPageBuilder.rowPageBuilder;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestRaptorPageSource
{
    private static final ImmutableList<Type> TYPES = ImmutableList.<Type>of(VARCHAR, BIGINT);

    @Test
    public void testAlignment()
            throws Exception
    {
        List<Page> expected = rowPagesBuilder(VARCHAR, BIGINT)
                .row("alice", 0)
                .row("bob", 1)
                .row("charlie", 2)
                .row("dave", 3)
                .pageBreak()
                .row("alice", 4)
                .row("bob", 5)
                .row("charlie", 6)
                .row("dave", 7)
                .pageBreak()
                .row("alice", 8)
                .row("bob", 9)
                .row("charlie", 10)
                .row("dave", 11)
                .build();

        RaptorPageSource raptorDataSource = createRaptorDataSource();
        List<Page> actual = new ArrayList<>();
        while (!raptorDataSource.isFinished()) {
            Page nextPage = raptorDataSource.getNextPage();
            if (nextPage != null) {
                actual.add(nextPage);
            }
        }

        assertEquals(actual.size(), expected.size());

        for (int i = 0; i < actual.size(); i++) {
            assertPageEquals(TYPES, actual.get(i), expected.get(i));
        }
    }

    @Test
    public void testFinish()
            throws Exception
    {
        RaptorPageSource raptorDataSource = createRaptorDataSource();

        // verify initial state
        assertEquals(raptorDataSource.isFinished(), false);

        // read first page
        assertPageEquals(TYPES, raptorDataSource.getNextPage(), rowPageBuilder(TYPES)
                .row("alice", 0)
                .row("bob", 1)
                .row("charlie", 2)
                .row("dave", 3)
                .build());

        // verify state
        assertEquals(raptorDataSource.isFinished(), false);

        // read second page
        assertPageEquals(TYPES, raptorDataSource.getNextPage(), rowPageBuilder(TYPES)
                .row("alice", 4)
                .row("bob", 5)
                .row("charlie", 6)
                .row("dave", 7)
                .build());

        // verify state
        assertEquals(raptorDataSource.isFinished(), false);

        // finish
        raptorDataSource.close();

        // verify state
        assertEquals(raptorDataSource.isFinished(), true);
        assertEquals(raptorDataSource.getNextPage(), null);
    }

    private static RaptorPageSource createRaptorDataSource()
    {
        Iterable<Block> channel0 = ImmutableList.of(
                createStringsBlock("alice", "bob", "charlie", "dave"),
                createStringsBlock("alice", "bob", "charlie", "dave"),
                createStringsBlock("alice", "bob", "charlie", "dave"));

        Iterable<Block> channel1 = ImmutableList.of(createLongSequenceBlock(0, 12));

        return new RaptorPageSource(ImmutableList.of(channel0, channel1));
    }
}
