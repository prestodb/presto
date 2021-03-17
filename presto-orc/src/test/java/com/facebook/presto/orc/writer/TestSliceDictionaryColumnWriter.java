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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestSliceDictionaryColumnWriter
{
    @Test
    public void testChunkLength()
    {
        int numEntries = 10;
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, numEntries);

        // Over allocate dictionary indexes but only use the required limit.
        int[] dictionaryIndexes = new int[numEntries + 10];
        Arrays.fill(dictionaryIndexes, 1);
        blockBuilder.appendNull();
        dictionaryIndexes[0] = 0;

        String string = "";
        for (int i = 1; i < numEntries; i++) {
            string += "a";
            VARCHAR.writeSlice(blockBuilder, utf8Slice(string));
            dictionaryIndexes[i] = numEntries - i;
        }

        // A dictionary block of size 10, 1st element -> null, 2nd element size -> 9....9th element size -> 1
        // Pass different maxChunkSize and different offset and verify if it computes the chunk lengths correctly.
        Block elementBlock = blockBuilder.build();
        int length = SliceDictionaryColumnWriter.getChunkLength(0, dictionaryIndexes, numEntries, elementBlock, 10);
        assertEquals(length, 2);

        length = SliceDictionaryColumnWriter.getChunkLength(0, dictionaryIndexes, numEntries, elementBlock, 1_000_000);
        assertEquals(length, numEntries);

        length = SliceDictionaryColumnWriter.getChunkLength(0, dictionaryIndexes, numEntries, elementBlock, 20);
        assertEquals(length, 3);

        length = SliceDictionaryColumnWriter.getChunkLength(1, dictionaryIndexes, numEntries, elementBlock, 9 + 8 + 7);
        assertEquals(length, 3);

        length = SliceDictionaryColumnWriter.getChunkLength(2, dictionaryIndexes, numEntries, elementBlock, 0);
        assertEquals(length, 1);

        length = SliceDictionaryColumnWriter.getChunkLength(9, dictionaryIndexes, numEntries, elementBlock, 0);
        assertEquals(length, 1);
    }
}
