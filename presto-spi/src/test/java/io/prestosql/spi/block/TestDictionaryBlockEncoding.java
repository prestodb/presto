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
package io.prestosql.spi.block;

import io.airlift.slice.DynamicSliceOutput;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDictionaryBlockEncoding
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde(new TestingTypeManager());

    @Test
    public void testRoundTrip()
    {
        int positionCount = 40;

        // build dictionary
        BlockBuilder dictionaryBuilder = VARCHAR.createBlockBuilder(null, 4);
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

        DictionaryBlock dictionaryBlock = new DictionaryBlock(dictionary, ids);

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
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }
}
