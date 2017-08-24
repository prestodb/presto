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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.block.TestingSession.SESSION;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDictionaryBlockEncoding
{
    @Test
    public void testRoundTrip()
    {
        int positionCount = 40;

        // build dictionary
        BlockBuilder dictionaryBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 4);
        VARCHAR.writeString(dictionaryBuilder, "alice");
        VARCHAR.writeString(dictionaryBuilder, "bob");
        VARCHAR.writeString(dictionaryBuilder, "charlie");
        VARCHAR.writeString(dictionaryBuilder, "dave");
        Block dictionary = dictionaryBuilder.build();

        // build ids
        int[] ids = new int[positionCount];
        for (int i = 0; i < 40; i++) {
            ids[i] = i % 4;
        }

        BlockEncoding blockEncoding = new DictionaryBlockEncoding(new VariableWidthBlockEncoding());
        DictionaryBlock dictionaryBlock = new DictionaryBlock(dictionary, ids);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncoding.writeBlock(sliceOutput, dictionaryBlock);
        Block actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());

        assertTrue(actualBlock instanceof DictionaryBlock);
        DictionaryBlock actualDictionaryBlock = (DictionaryBlock) actualBlock;
        assertBlockEquals(VARCHAR, actualDictionaryBlock.getDictionary(), dictionary);
        for (int position = 0; position < actualDictionaryBlock.getPositionCount(); position++) {
            assertEquals(actualDictionaryBlock.getId(position), ids[position]);
        }
        assertEquals(actualDictionaryBlock.getDictionarySourceId(), dictionaryBlock.getDictionarySourceId());
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }
}
