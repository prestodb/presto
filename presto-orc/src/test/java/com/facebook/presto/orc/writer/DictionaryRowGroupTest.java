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

import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class DictionaryRowGroupTest
{
    private static final int MAX_DICTIONARY_INDEX = 10_000;

    private final Random random = new Random();

    private static ColumnStatistics getColumnStatistics()
    {
        return new ColumnStatistics(10L, null);
    }

    private byte[] getByteIndexes(DictionaryRowGroup rowGroup)
    {
        assertNotNull(rowGroup.getByteIndexes());
        assertNull(rowGroup.getShortIndexes());
        assertNull(rowGroup.getIntegerIndexes());
        return rowGroup.getByteIndexes();
    }

    private short[] getShortIndexes(DictionaryRowGroup rowGroup)
    {
        assertNotNull(rowGroup.getShortIndexes());
        assertNull(rowGroup.getByteIndexes());
        assertNull(rowGroup.getIntegerIndexes());
        return rowGroup.getShortIndexes();
    }

    private int[] getIntegerIndexes(DictionaryRowGroup rowGroup)
    {
        assertNotNull(rowGroup.getIntegerIndexes());
        assertNull(rowGroup.getByteIndexes());
        assertNull(rowGroup.getShortIndexes());
        return rowGroup.getIntegerIndexes();
    }

    @Test
    public void testEmptyDictionary()
    {
        DictionaryRowGroup rowGroup = new DictionaryRowGroup(-1, new int[0], 0, getColumnStatistics());
        assertEquals(rowGroup.getValueCount(), 0);
        byte[] byteIndexes = getByteIndexes(rowGroup);
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
            DictionaryRowGroup rowGroup = new DictionaryRowGroup(Byte.MAX_VALUE, dictionaryIndexes, length, getColumnStatistics());
            byte[] byteIndexes = getByteIndexes(rowGroup);
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
            DictionaryRowGroup rowGroup = new DictionaryRowGroup(Short.MAX_VALUE, dictionaryIndexes, length, getColumnStatistics());
            short[] shortIndexes = getShortIndexes(rowGroup);
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
            DictionaryRowGroup rowGroup = new DictionaryRowGroup(Integer.MAX_VALUE, dictionaryIndexes, length, getColumnStatistics());
            int[] intIndexes = getIntegerIndexes(rowGroup);
            assertEquals(length, intIndexes.length);
            for (int i = 0; i < length; i++) {
                assertEquals(dictionaryIndexes[i], intIndexes[i]);
            }
        }
    }
}
