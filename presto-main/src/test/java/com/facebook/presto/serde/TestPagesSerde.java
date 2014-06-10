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
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Iterator;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.serde.PagesSerde.readPages;
import static com.facebook.presto.serde.PagesSerde.writePages;
import static com.facebook.presto.testing.TestingBlockEncodingManager.createTestingBlockEncodingManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestPagesSerde
{
    @Test
    public void testRoundTrip()
    {
        Block expectedBlock = VARCHAR.createBlockBuilder(new BlockBuilderStatus())
                .appendSlice(Slices.utf8Slice("alice"))
                .appendSlice(Slices.utf8Slice("bob"))
                .appendSlice(Slices.utf8Slice("charlie"))
                .appendSlice(Slices.utf8Slice("dave"))
                .build();
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
        assertEquals(pageSize, 26); // page overhead

        // page with one value
        page = new Page(builder.appendLong(123).build());
        int firstValueSize = serializedSize(page) - pageSize;
        assertEquals(firstValueSize, 8 + 1); // value size + value overhead

        // page with two values
        page = new Page(builder.appendLong(456).build());
        int secondValueSize = serializedSize(page) - (pageSize + firstValueSize);
        assertEquals(secondValueSize, 8 + 1); // value size + value overhead
    }

    @Test
    public void testVarcharSerializedSize()
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());

        // empty page
        Page page = new Page(builder.build());
        int pageSize = serializedSize(page);
        assertEquals(pageSize, 27); // page overhead

        // page with one value
        page = new Page(builder.appendSlice(Slices.utf8Slice("alice")).build());
        int firstValueSize = serializedSize(page) - pageSize;
        assertEquals(firstValueSize, 4 + 5 + 5); // "alice" + value overhead

        // page with two values
        page = new Page(builder.appendSlice(Slices.utf8Slice("bob")).build());
        int secondValueSize = serializedSize(page) - (pageSize + firstValueSize);
        assertEquals(secondValueSize, 4 + 3 + 5); // "bob" + value overhead
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
