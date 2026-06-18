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
package com.facebook.presto.execution;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.airlift.testing.Assertions.assertLessThanOrEqual;
import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createRandomDictionaryBlock;
import static com.facebook.presto.block.BlockAssertions.createSlicesBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.slice.Slices.wrappedBuffer;

public class TestPageSplitterUtil
{
    @Test
    public void testSplitPage()
    {
        int positionCount = 10;
        int maxPageSizeInBytes = 100;
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, BIGINT);

        Page largePage = createSequencePage(types, positionCount, 0, 1, 1);
        List<Page> pages = splitPage(largePage, maxPageSizeInBytes);

        assertGreaterThan(pages.size(), 1);
        assertPageSize(pages, maxPageSizeInBytes);
        assertPositionCount(pages, positionCount);
        MaterializedResult actual = toMaterializedResult(TEST_SESSION, types, pages);
        MaterializedResult expected = toMaterializedResult(TEST_SESSION, types, ImmutableList.of(largePage));
        assertEquals(actual, expected);
    }

    private static void assertPageSize(List<Page> pages, long maxPageSizeInBytes)
    {
        for (Page page : pages) {
            assertLessThanOrEqual(page.getSizeInBytes(), maxPageSizeInBytes);
        }
    }

    private static void assertPositionCount(List<Page> pages, int positionCount)
    {
        int totalPositionCount = 0;
        for (Page page : pages) {
            totalPositionCount += page.getPositionCount();
        }
        assertEquals(totalPositionCount, positionCount);
    }

    @Test
    private void testSplitPageNonDecreasingPageSize()
    {
        int positionCount = 100;
        int maxPageSizeInBytes = 1;
        List<Type> types = ImmutableList.of(VARCHAR);

        Slice expectedValue = wrappedBuffer("test".getBytes());
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 1, expectedValue.length());
        blockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
        Block rleBlock = new RunLengthEncodedBlock(blockBuilder.build(), positionCount);
        Page initialPage = new Page(rleBlock);
        List<Page> pages = splitPage(initialPage, maxPageSizeInBytes);

        // the page should only be split in half as the recursion should terminate
        // after seeing that the size of the Page doesn't decrease
        assertEquals(pages.size(), 2);
        Page first = pages.get(0);
        Page second = pages.get(1);

        // the size of the pages will remain the same and should be greater than the maxPageSizeInBytes
        assertGreaterThan((int) first.getSizeInBytes(), maxPageSizeInBytes);
        assertGreaterThan((int) second.getSizeInBytes(), maxPageSizeInBytes);
        assertPositionCount(pages, positionCount);
        MaterializedResult actual = toMaterializedResult(TEST_SESSION, types, pages);
        MaterializedResult expected = toMaterializedResult(TEST_SESSION, types, ImmutableList.of(initialPage));
        assertEquals(actual, expected);
    }

    @Test
    public void testSplitDictionaryBlock()
    {
        // create a huge dictionary with a single entry
        Block dictionary = createSlicesBlock(Slices.utf8Slice(new String(new char[1_000_000])));

        // make every row in the block identical to the single entry in the dictionary
        DictionaryBlock dictionaryBlock = createRandomDictionaryBlock(dictionary, 50000);

        List<Page> pages = splitPage(new Page(dictionaryBlock), 1000);
        assertEquals(pages.size(), 2);
    }
}
