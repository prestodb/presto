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
package com.facebook.presto.serde;

import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Iterator;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.serde.PagesSerde.readPages;
import static com.facebook.presto.serde.PagesSerde.writePages;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingBlockEncodingManager.createTestingBlockEncodingManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestPagesSerde
{
    @Test
    public void testRoundTrip()
    {
        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        Block expectedBlock = expectedBlockBuilder.build();

        Page expectedPage = new Page(expectedBlock, expectedBlock, expectedBlock);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writePages(createTestingBlockEncodingManager(), sliceOutput, expectedPage, expectedPage, expectedPage);
        Iterator<Page> pageIterator = readPages(createTestingBlockEncodingManager(), sliceOutput.slice().getInput());
        assertPageEquals(pageIterator.next(), expectedPage);
        assertPageEquals(pageIterator.next(), expectedPage);
        assertPageEquals(pageIterator.next(), expectedPage);
        assertFalse(pageIterator.hasNext());
    }

    @Test
    public void testBigintSerializedSize()
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus());

        // empty page
        Page page = new Page(builder.build());
        int pageSize = serializedSize(page);
        assertEquals(pageSize, 41); // page overhead

        // page with one value
        BIGINT.writeLong(builder, 123);
        page = new Page(builder.build());
        int firstValueSize = serializedSize(page) - pageSize;
        assertEquals(firstValueSize, 9); // value size + value overhead

        // page with two values
        BIGINT.writeLong(builder, 456);
        page = new Page(builder.build());
        int secondValueSize = serializedSize(page) - (pageSize + firstValueSize);
        assertEquals(secondValueSize, 8); // value size (value overhead is shared with previous value)
    }

    @Test
    public void testVarcharSerializedSize()
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());

        // empty page
        Page page = new Page(builder.build());
        int pageSize = serializedSize(page);
        assertEquals(pageSize, 45); // page overhead

        // page with one value
        VARCHAR.writeString(builder, "alice");
        page = new Page(builder.build());
        int firstValueSize = serializedSize(page) - pageSize;
        assertEquals(firstValueSize, 4 + 5 + 1); // length + "alice" + null

        // page with two values
        VARCHAR.writeString(builder, "bob");
        page = new Page(builder.build());
        int secondValueSize = serializedSize(page) - (pageSize + firstValueSize);
        assertEquals(secondValueSize, 4  + 3); // length + "bob" (null shared with first entry)
    }

    private static int serializedSize(Page expectedPage)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writePages(createTestingBlockEncodingManager(), sliceOutput, expectedPage);
        Slice slice = sliceOutput.slice();

        Iterator<Page> pageIterator = readPages(createTestingBlockEncodingManager(), slice.getInput());
        assertPageEquals(pageIterator.next(), expectedPage);
        assertFalse(pageIterator.hasNext());

        return slice.length();
    }
}
