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
package com.facebook.presto.connector.thrift.util;

import com.facebook.presto.block.AbstractTestBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestConcatBlock
        extends AbstractTestBlock
{
    @Test
    public void testConcatedSize()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(10);
        ConcatBlock concatBlock = new ConcatBlock(ImmutableList.of(createDictionaryBlock(expectedValues, 100), createDictionaryBlock(expectedValues, 111)));
        assertEquals(concatBlock.getPositionCount(), 211);
    }

    @Test
    public void testBasicGetPositions()
    {
        Slice[] expectedValues = createExpectedValues(10);
        Block dictionaryBlock1 = new DictionaryBlock(makeSliceArrayBlock(expectedValues), new int[] {0, 2, 4});
        Block dictionaryBlock2 = new DictionaryBlock(makeSliceArrayBlock(expectedValues), new int[] {1, 3, 5});
        Block dictionaryBlock3 = new DictionaryBlock(makeSliceArrayBlock(expectedValues), new int[] {5, 7, 9});
        ConcatBlock concatBlock = new ConcatBlock(ImmutableList.of(dictionaryBlock1, dictionaryBlock2, dictionaryBlock3));

        assertBlock(concatBlock, new Slice[] {expectedValues[0], expectedValues[2], expectedValues[4],
                                                   expectedValues[1], expectedValues[3], expectedValues[5],
                                                   expectedValues[5], expectedValues[7], expectedValues[9]});
    }

    private static DictionaryBlock createDictionaryBlock(Slice[] expectedValues, int positionCount)
    {
        int dictionarySize = expectedValues.length;
        int[] ids = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(makeSliceArrayBlock(expectedValues), ids);
    }

    private static Block makeSliceArrayBlock(Slice[] values)
    {
        return new SliceArrayBlock(values.length, values);
    }
}
