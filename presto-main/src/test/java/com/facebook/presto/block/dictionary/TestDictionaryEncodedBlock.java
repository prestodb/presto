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
package com.facebook.presto.block.dictionary;

import com.facebook.presto.block.AbstractTestBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class TestDictionaryEncodedBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Slice[] dictionary = createDictionary(10);
        for (int positionCount = 0; positionCount < 20; positionCount++) {
            Integer[] ids = createIds(dictionary.length, positionCount);
            assertDictionaryEncodedBlock(dictionary, ids);
            assertDictionaryEncodedBlock(dictionary, (Integer[]) alternatingNullValues(ids));
        }
    }

    private static void assertDictionaryEncodedBlock(Slice[] dictionary, Integer[] ids)
    {
        VariableWidthBlockBuilder dictionaryBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus());
        for (Slice expectedValue : dictionary) {
            dictionaryBlockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
        }
        Block dictionaryBlock = dictionaryBlockBuilder.build();

        Slice[] expectedValues = new Slice[ids.length];
        BlockBuilder idsBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus());
        for (int i = 0; i < ids.length; i++) {
            Integer id = ids[i];
            if (id == null) {
                idsBlockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(idsBlockBuilder, id);
                expectedValues[i] = dictionary[id];
            }
        }
        Block idsBlock = idsBlockBuilder.build();

        assertBlock(new DictionaryEncodedBlock(dictionaryBlock, idsBlock), expectedValues);
    }

    private static Slice[] createDictionary(int size)
    {
        Slice[] expectedValues = new Slice[size];
        for (int position = 0; position < size; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    private static Integer[] createIds(int dictionarySize, int positionCount)
    {
        Integer[] ids = new Integer[positionCount];
        for (int position = 0; position < positionCount; position++) {
            ids[position] = positionCount % dictionarySize;
        }
        return ids;
    }
}
