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

import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.execution.buffer.PageSplitterUtil;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static org.testng.Assert.assertEquals;

public class TestPageSplitterUtil
{
    @Test
    public void testSplitPage()
            throws Exception
    {
        int positionCount = 10;
        int maxPageSizeInBytes = 100;
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, BIGINT);

        Page largePage = SequencePageBuilder.createSequencePage(types, positionCount, 0, 1, 1);
        List<Page> pages = PageSplitterUtil.splitPage(largePage, maxPageSizeInBytes);

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
}
