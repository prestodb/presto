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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.page.PagesSerde;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readPages;
import static com.facebook.presto.spi.page.PagesSerdeUtil.writePages;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestPagesSerde
{
    @Test
    public void testRoundTrip()
    {
        PagesSerde serde = new TestingPagesSerdeFactory().createPagesSerde();
        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(null, 5);
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        Block expectedBlock = expectedBlockBuilder.build();

        Page expectedPage = new Page(expectedBlock, expectedBlock, expectedBlock);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writePages(serde, sliceOutput, expectedPage, expectedPage, expectedPage);

        List<Type> types = ImmutableList.of(VARCHAR, VARCHAR, VARCHAR);
        Iterator<Page> pageIterator = readPages(serde, sliceOutput.slice().getInput());
        assertPageEquals(types, pageIterator.next(), expectedPage);
        assertPageEquals(types, pageIterator.next(), expectedPage);
        assertPageEquals(types, pageIterator.next(), expectedPage);
        assertFalse(pageIterator.hasNext());
    }

    @Test
    public void testBigintSerializedSize()
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, 5);

        // empty page
        Page page = new Page(builder.build());
        int pageSize = serializedSize(ImmutableList.of(BIGINT), page);
        assertEquals(pageSize, 56); // page overhead ideally 35 but since a 0 sized block will be a RLEBlock we have an overhead of 13

        // page with one value
        BIGINT.writeLong(builder, 123);
        pageSize = 35; // Now we have moved to the normal block implementation so the page size overhead is 35
        page = new Page(builder.build());
        int firstValueSize = serializedSize(ImmutableList.of(BIGINT), page) - pageSize;
        assertEquals(firstValueSize, 17); // value size + value overhead

        // page with two values
        BIGINT.writeLong(builder, 456);
        page = new Page(builder.build());
        int secondValueSize = serializedSize(ImmutableList.of(BIGINT), page) - (pageSize + firstValueSize);
        assertEquals(secondValueSize, 8); // value size (value overhead is shared with previous value)
    }

    @Test
    public void testVarcharSerializedSize()
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 5);

        // empty page
        Page page = new Page(builder.build());
        int pageSize = serializedSize(ImmutableList.of(VARCHAR), page);
        assertEquals(pageSize, 52); // page overhead

        // page with one value
        VARCHAR.writeString(builder, "alice");
        page = new Page(builder.build());
        int firstValueSize = serializedSize(ImmutableList.of(VARCHAR), page) - pageSize;
        assertEquals(firstValueSize, 4 + 5); // length + "alice"

        // page with two values
        VARCHAR.writeString(builder, "bob");
        page = new Page(builder.build());
        int secondValueSize = serializedSize(ImmutableList.of(VARCHAR), page) - (pageSize + firstValueSize);
        assertEquals(secondValueSize, 4 + 3); // length + "bob" (null shared with first entry)
    }

    private static int serializedSize(List<? extends Type> types, Page expectedPage)
    {
        PagesSerde serde = new TestingPagesSerdeFactory().createPagesSerde();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writePages(serde, sliceOutput, expectedPage);
        Slice slice = sliceOutput.slice();

        Iterator<Page> pageIterator = readPages(serde, slice.getInput());
        if (pageIterator.hasNext()) {
            assertPageEquals(types, pageIterator.next(), expectedPage);
        }
        else {
            assertEquals(expectedPage.getPositionCount(), 0);
        }
        assertFalse(pageIterator.hasNext());

        return slice.length();
    }
}
