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

import static com.facebook.airlift.testing.Assertions.assertLessThan;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Arrays.fill;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestDictionaryRowGroupBuilder
{
    private static final int MAX_DICTIONARY_INDEX = 10_000;
    private static final int NULL_INDEX_ENTRY = -1;

    private final Random random = new Random();

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
        int[] dictionaryIndexes = createIndexArray(Byte.MAX_VALUE + 1, MAX_DICTIONARY_INDEX);

        for (int length : ImmutableList.of(0, 10, dictionaryIndexes.length)) {
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
        int[] dictionaryIndexes = createIndexArray(Short.MAX_VALUE + 1, MAX_DICTIONARY_INDEX);

        for (int length : ImmutableList.of(0, 10, dictionaryIndexes.length)) {
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
        int[] dictionaryIndexes = createIndexArray(Integer.MAX_VALUE, MAX_DICTIONARY_INDEX);

        for (int length : ImmutableList.of(0, 10, dictionaryIndexes.length)) {
            DictionaryRowGroupBuilder rowGroupBuilder = new DictionaryRowGroupBuilder();
            rowGroupBuilder.addIndexes(Integer.MAX_VALUE, dictionaryIndexes, length);
            int[] intIndexes = getIntegerIndexes(rowGroupBuilder);
            assertEquals(length, intIndexes.length);
            for (int i = 0; i < length; i++) {
                assertEquals(dictionaryIndexes[i], intIndexes[i]);
            }
        }
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testDecreasingMaxThrows()
    {
        DictionaryRowGroupBuilder rowGroupBuilder = new DictionaryRowGroupBuilder();
        rowGroupBuilder.addIndexes(5, new int[0], 0);
        rowGroupBuilder.addIndexes(3, new int[1], 1);
    }

    @Test
    public void testNullDictionary()
    {
        int[] indexes = new int[MAX_DICTIONARY_INDEX];
        fill(indexes, NULL_INDEX_ENTRY);
        DictionaryRowGroupBuilder rowGroupBuilder = new DictionaryRowGroupBuilder();
        rowGroupBuilder.addIndexes(NULL_INDEX_ENTRY, indexes, indexes.length);

        byte[] byteIndexes = getByteIndexes(rowGroupBuilder);
        compareIntAndByteArrays(indexes, byteIndexes);

        // Adding 0 element list should be ignored.
        rowGroupBuilder.addIndexes(-1, indexes, 0);
        byteIndexes = getByteIndexes(rowGroupBuilder);
        compareIntAndByteArrays(indexes, byteIndexes);
    }

    @Test
    public void testMultipleSegments()
    {
        int byteSegmentLength = 7;
        int shortSegmentLength = 6;
        int intSegmentLength = 17;

        int[][] segments = new int[byteSegmentLength + shortSegmentLength + intSegmentLength][];

        DictionaryRowGroupBuilder rowGroupBuilder = new DictionaryRowGroupBuilder();
        long emptyRetainedSizeInBytes = rowGroupBuilder.getRetainedSizeInBytes();
        int index = 0;

        for (int i = 0; i < byteSegmentLength; i++, index++) {
            segments[index] = createIndexArray(Byte.MAX_VALUE + 1, 1_000 + i * 5);
            rowGroupBuilder.addIndexes(Byte.MAX_VALUE, segments[index], 1_000 + i * 3);
        }

        for (int i = 0; i < shortSegmentLength; i++, index++) {
            segments[index] = createIndexArray(Short.MAX_VALUE + 1, 1_000 + i * 5);
            rowGroupBuilder.addIndexes(Short.MAX_VALUE, segments[index], 1_000 + i * 3);
        }

        for (int i = 0; i < intSegmentLength; i++, index++) {
            segments[index] = createIndexArray(Integer.MAX_VALUE, 1_000 + i * 5);
            rowGroupBuilder.addIndexes(Integer.MAX_VALUE, segments[index], 1_000 + i * 3);
        }

        byte[][] byteSegments = rowGroupBuilder.getByteSegments();
        short[][] shortSegments = rowGroupBuilder.getShortSegments();
        int[][] intSegments = rowGroupBuilder.getIntegerSegments();

        long indexSize = verifySegments(byteSegmentLength, shortSegmentLength, intSegmentLength, segments, byteSegments, shortSegments, intSegments);
        assertEquals(indexSize, rowGroupBuilder.getIndexRetainedBytes());
        long retainedBytesBeforeReset = rowGroupBuilder.getRetainedSizeInBytes();

        rowGroupBuilder.reset();
        assertEquals(0, rowGroupBuilder.getIndexRetainedBytes());
        assertNull(rowGroupBuilder.getByteSegments());
        assertNull(rowGroupBuilder.getShortSegments());
        assertNull(rowGroupBuilder.getIntegerSegments());
        long retainedBytesAfterReset = rowGroupBuilder.getRetainedSizeInBytes();
        assertLessThan(retainedBytesAfterReset, retainedBytesBeforeReset);
        assertEquals(emptyRetainedSizeInBytes, retainedBytesAfterReset);
    }

    private void compareIntAndByteArrays(int[] indexes, byte[] byteIndexes)
    {
        assertEquals(indexes.length, byteIndexes.length);
        for (int i = 0; i < byteIndexes.length; i++) {
            assertEquals(indexes[i], byteIndexes[i]);
        }
    }

    private int[] createIndexArray(int maxValue, int length)
    {
        int[] dictionaryIndexes = new int[length];
        for (int i = 0; i < length; i++) {
            if (random.nextBoolean()) {
                dictionaryIndexes[i] = NULL_INDEX_ENTRY;
            }
            else {
                dictionaryIndexes[i] = random.nextInt(maxValue);
            }
        }
        return dictionaryIndexes;
    }

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

    private long verifySegments(int byteSegmentLength, int shortSegmentLength, int intSegmentLength, int[][] segments, byte[][] byteSegments, short[][] shortSegments, int[][] intSegments)
    {
        int index = 0;
        long totalSize = 0;
        assertEquals(byteSegmentLength, byteSegments.length);
        for (int i = 0; i < byteSegments.length; i++, index++) {
            assertEquals(1000 + i * 3, byteSegments[i].length);
            totalSize += sizeOf(byteSegments[i]);
            for (int j = 0; j < byteSegments[i].length; j++) {
                assertEquals(segments[index][j], byteSegments[i][j]);
            }
        }

        assertEquals(shortSegmentLength, shortSegments.length);
        for (int i = 0; i < shortSegments.length; i++, index++) {
            assertEquals(1000 + i * 3, shortSegments[i].length);
            totalSize += sizeOf(shortSegments[i]);
            for (int j = 0; j < shortSegments[i].length; j++) {
                assertEquals(segments[index][j], shortSegments[i][j]);
            }
        }

        assertEquals(intSegmentLength, intSegments.length);
        for (int i = 0; i < intSegments.length; i++, index++) {
            assertEquals(1000 + i * 3, intSegments[i].length);
            totalSize += sizeOf(intSegments[i]);
            for (int j = 0; j < intSegments[i].length; j++) {
                assertEquals(segments[index][j], intSegments[i][j]);
            }
        }
        return totalSize;
    }
}
