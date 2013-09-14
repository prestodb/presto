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

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.Page;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import java.util.Iterator;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.serde.PagesSerde.readPages;
import static com.facebook.presto.serde.PagesSerde.writePages;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static org.testng.Assert.assertFalse;

public class TestPagesSerde
{
    @Test
    public void testRoundTrip()
    {
        UncompressedBlock expectedBlock = new BlockBuilder(SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build();
        Page expectedPage = new Page(expectedBlock, expectedBlock, expectedBlock);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writePages(sliceOutput, expectedPage, expectedPage, expectedPage);
        Iterator<Page> pageIterator = readPages(sliceOutput.slice().getInput());
        assertPageEquals(pageIterator.next(), expectedPage);
        assertPageEquals(pageIterator.next(), expectedPage);
        assertPageEquals(pageIterator.next(), expectedPage);
        assertFalse(pageIterator.hasNext());
    }
}
