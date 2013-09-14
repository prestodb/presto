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
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;
import org.testng.annotations.Test;

import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestUncompressedBlockCursor
        extends AbstractTestBlockCursor
{
    @Override
    protected Block createExpectedValues()
    {
        return new BlockBuilder(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64))
                .append("apple")
                .append(11)
                .append("apple")
                .append(12)
                .append("apple")
                .append(13)
                .append("banana")
                .append(14)
                .append("banana")
                .append(15)
                .append("banana")
                .append(16)
                .append("banana")
                .append(17)
                .append("banana")
                .append(18)
                .append("cherry")
                .append(19)
                .append("cherry")
                .append(20)
                .append("date")
                .append(21)
                .build();
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createExpectedValues().cursor(), UncompressedBlockCursor.class);
    }
}
