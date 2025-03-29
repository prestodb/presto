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

package com.facebook.presto.operator;

import com.google.common.primitives.Bytes;
import io.airlift.slice.ByteArrays;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.MoreByteArrays.fill;
import static com.facebook.presto.operator.MoreByteArrays.setBytes;
import static com.facebook.presto.operator.MoreByteArrays.setInts;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class TestMoreByteArrays
{
    private static final int POSITIONS_PER_PAGE = 10_000;

    private TestMoreByteArrays()
    {}

    @Test
    public static void testFill()
    {
        byte[] destination = new byte[POSITIONS_PER_PAGE];
        int filledBytes = fill(destination, 0, POSITIONS_PER_PAGE, (byte) 5);

        assertEquals(filledBytes, POSITIONS_PER_PAGE);
        assertCopied(
                nCopies(POSITIONS_PER_PAGE, (byte) 5),
                destination,
                ARRAY_BYTE_INDEX_SCALE,
                MoreByteArrays::getByte);
    }

    @Test
    public static void testSetBytes()
    {
        byte[] destination = new byte[POSITIONS_PER_PAGE];

        byte[] source = new byte[POSITIONS_PER_PAGE];
        ThreadLocalRandom.current().nextBytes(source);

        int setBytes = setBytes(destination, 0, source, 0, POSITIONS_PER_PAGE);

        assertEquals(setBytes, POSITIONS_PER_PAGE);
        assertCopied(Bytes.asList(source), destination, ARRAY_BYTE_INDEX_SCALE, MoreByteArrays::getByte);
    }

    @Test
    public static void testSetInts()
    {
        byte[] destination = new byte[POSITIONS_PER_PAGE * ARRAY_INT_INDEX_SCALE];

        int copiedBytes = setInts(destination, 0, IntStream.range(0, POSITIONS_PER_PAGE).toArray(), 0, POSITIONS_PER_PAGE);

        assertEquals(copiedBytes, POSITIONS_PER_PAGE * ARRAY_INT_INDEX_SCALE);
        assertCopied(
                IntStream.range(0, POSITIONS_PER_PAGE).boxed().collect(toImmutableList()),
                destination,
                ARRAY_INT_INDEX_SCALE,
                ByteArrays::getInt);
    }

    private static <T> void assertCopied(List<T> expected, byte[] actual, int elementSize, BiFunction<byte[], Integer, T> readFunction)
    {
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(readFunction.apply(actual, index * elementSize), expected.get(index));
        }
    }
}
