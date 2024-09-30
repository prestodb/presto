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
package com.facebook.presto.common.block;

import com.facebook.presto.common.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.common.block.TestVariableWidthBlockEncoding.PROPERTIES;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDictionaryBlockEncoding
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

    @Test
    public void testRoundTrip()
    {
        // build dictionary
        BlockBuilder dictionaryBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(dictionaryBuilder, "alice");
        VARCHAR.writeString(dictionaryBuilder, "bob");
        VARCHAR.writeString(dictionaryBuilder, "charlie");
        VARCHAR.writeString(dictionaryBuilder, "dave");
        Block dictionary = dictionaryBuilder.build();

        // build ids
        int positionCount = 40;
        int[] ids = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % 4;
        }

        DictionaryBlock dictionaryBlock = new DictionaryBlock(dictionary, ids);
        assertEquals(positionCount, dictionaryBlock.getPositionCount());

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, dictionaryBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());

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
            assertEquals(type.getObjectValue(PROPERTIES, actual, position), type.getObjectValue(PROPERTIES, expected, position));
        }
    }

    @Test
    public void testTooFewPositions()
    {
        // build dictionary
        BlockBuilder dictionaryBuilder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeString(dictionaryBuilder, "alice");
        Block dictionary = dictionaryBuilder.build();

        int positionCount = 40;
        int[] ids = new int[positionCount];

        try {
            new DictionaryBlock(ids.length - 1, dictionary, ids);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
            assertNotNull(expected.getMessage());
        }
    }
}
