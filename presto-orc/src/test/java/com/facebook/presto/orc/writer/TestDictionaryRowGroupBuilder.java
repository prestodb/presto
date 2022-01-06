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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestDictionaryRowGroupBuilder
{
    private static final int MAX_DICTIONARY_INDEX = 10_000;

    private final Random random = new Random();

    private byte[] getByteIndexes(DictionaryRowGroupBuilder rowGroupBuilder)
    {
        byte[][] byteSegments = rowGroupBuilder.getByteSegments();
        assertNotNull(byteSegments);
        assertEquals(1, byteSegments.length);

        assertNull(rowGroupBuilder.getShortSegments());
        assertNull(rowGroupBuilder.getIntegerSegments());
        assertNotNull(byteSegments[0]);
        return byteSegments[0];
    }

    private short[] getShortIndexes(DictionaryRowGroupBuilder rowGroupBuilder)
    {
        short[][] shortSegments = rowGroupBuilder.getShortSegments();
        assertNotNull(shortSegments);
        assertEquals(1, shortSegments.length);

        assertNull(rowGroupBuilder.getByteSegments());
        assertNull(rowGroupBuilder.getIntegerSegments());
        assertNotNull(shortSegments[0]);
        return shortSegments[0];
    }

    private int[] getIntegerIndexes(DictionaryRowGroupBuilder rowGroupBuilder)
    {
        int[][] integerSegments = rowGroupBuilder.getIntegerSegments();
        assertNotNull(integerSegments);
        assertEquals(1, integerSegments.length);

        assertNull(rowGroupBuilder.getByteSegments());
        assertNull(rowGroupBuilder.getShortSegments());
        assertNotNull(integerSegments[0]);
        return integerSegments[0];
    }

    @Test
    public void testEmptyDictionary()
    {
        DictionaryRowGroupBuilder rowGroupBuilder = new DictionaryRowGroupBuilder();
        rowGroupBuilder.addIndexes(-1, new int[0], 0);

        byte[] byteIndexes = getByteIndexes(rowGroupBuilder);
        assertEquals(0, byteIndexes.length);
    }

    @Test
    public void testByteIndexes()
    {
        int[] dictionaryIndexes = new int[MAX_DICTIONARY_INDEX];
        for (int i = 0; i < MAX_DICTIONARY_INDEX; i++) {
            dictionaryIndexes[i] = random.nextInt(Byte.MAX_VALUE + 1);
        }

        for (int length : ImmutableList.of(0, 10, MAX_DICTIONARY_INDEX)) {
            DictionaryRowGroupBuilder rowGroupBuilder = new DictionaryRowGroupBuilder();
            rowGroupBuilder.addIndexes(Byte.MAX_VALUE, dictionaryIndexes, length);
            byte[] byteIndexes = getByteIndexes(rowGroupBuilder);
            assertEquals(length, byteIndexes.length);
            for (int i = 0; i < length; i++) {
                assertEquals(dictionaryIndexes[i], byteIndexes[i]);
            }
        }
    }

    @Test
    public void testShortIndexes()
    {
        int[] dictionaryIndexes = new int[MAX_DICTIONARY_INDEX];
        for (int i = 0; i < MAX_DICTIONARY_INDEX; i++) {
            dictionaryIndexes[i] = random.nextInt(Short.MAX_VALUE + 1);
        }

        for (int length : ImmutableList.of(0, 10, MAX_DICTIONARY_INDEX)) {
            DictionaryRowGroupBuilder rowGroupBuilder = new DictionaryRowGroupBuilder();
            rowGroupBuilder.addIndexes(Short.MAX_VALUE, dictionaryIndexes, length);
            short[] shortIndexes = getShortIndexes(rowGroupBuilder);
            assertEquals(length, shortIndexes.length);
            for (int i = 0; i < length; i++) {
                assertEquals(dictionaryIndexes[i], shortIndexes[i]);
            }
        }
    }

    @Test
    public void testIntegerIndexes()
    {
        int[] dictionaryIndexes = new int[MAX_DICTIONARY_INDEX];
        for (int i = 0; i < MAX_DICTIONARY_INDEX; i++) {
            dictionaryIndexes[i] = random.nextInt(Integer.MAX_VALUE);
        }

        for (int length : ImmutableList.of(0, 10, MAX_DICTIONARY_INDEX)) {
            DictionaryRowGroupBuilder rowGroupBuilder = new DictionaryRowGroupBuilder();
            rowGroupBuilder.addIndexes(Integer.MAX_VALUE, dictionaryIndexes, length);
            int[] intIndexes = getIntegerIndexes(rowGroupBuilder);
            assertEquals(length, intIndexes.length);
            for (int i = 0; i < length; i++) {
                assertEquals(dictionaryIndexes[i], intIndexes[i]);
            }
        }
    }
}
