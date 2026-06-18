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
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

public class TestUncheckedByteArrays
{
    private static final int POSITIONS_PER_PAGE = 10_000;

    private TestUncheckedByteArrays()
    {}

    @Test
    public static void testSetByte()
    {
        byte[] source = new byte[POSITIONS_PER_PAGE];
        ThreadLocalRandom.current().nextBytes(source);

        testSequential(Bytes.asList(source), ARRAY_BYTE_INDEX_SCALE, UncheckedByteArrays::setByteUnchecked, UncheckedByteArrays::getByteUnchecked);
        testRandom(Bytes.asList(source), ARRAY_BYTE_INDEX_SCALE, UncheckedByteArrays::setByteUnchecked, UncheckedByteArrays::getByteUnchecked);
    }

    @Test
    public static void testSetShort()
    {
        List<Short> source = IntStream.range(0, POSITIONS_PER_PAGE).mapToObj(i -> (short) ThreadLocalRandom.current().nextInt(256)).collect(Collectors.toList());

        testSequential(source, ARRAY_SHORT_INDEX_SCALE, UncheckedByteArrays::setShortUnchecked, UncheckedByteArrays::getShortUnchecked);
        testRandom(source, ARRAY_SHORT_INDEX_SCALE, UncheckedByteArrays::setShortUnchecked, UncheckedByteArrays::getShortUnchecked);
    }

    @Test
    public static void testSetInt()
    {
        List<Integer> source = IntStream.range(0, POSITIONS_PER_PAGE).boxed().collect(Collectors.toList());

        testSequential(source, ARRAY_INT_INDEX_SCALE, UncheckedByteArrays::setIntUnchecked, UncheckedByteArrays::getIntUnchecked);
        testRandom(source, ARRAY_INT_INDEX_SCALE, UncheckedByteArrays::setIntUnchecked, UncheckedByteArrays::getIntUnchecked);
    }

    @Test
    public static void testSetLong()
    {
        List<Long> source = LongStream.range(0, POSITIONS_PER_PAGE).boxed().collect(Collectors.toList());

        testSequential(source, ARRAY_LONG_INDEX_SCALE, UncheckedByteArrays::setLongUnchecked, UncheckedByteArrays::getLongUnchecked);
        testRandom(source, ARRAY_LONG_INDEX_SCALE, UncheckedByteArrays::setLongUnchecked, UncheckedByteArrays::getLongUnchecked);
    }

    private static <T> void testSequential(List<T> source, int elementSize, TriFunction<byte[], Integer, T> writeFunction, BiFunction<byte[], Integer, T> readFunction)
    {
        byte[] destination = new byte[source.size() * elementSize];

        int index = 0;
        for (T value : source) {
            index = writeFunction.apply(destination, index, value);
        }

        assertEquals(index, POSITIONS_PER_PAGE * elementSize);
        assertCopied(source, destination, elementSize, readFunction);
    }

    private static <T> void testRandom(List<T> source, int elementSize, TriFunction<byte[], Integer, T> writeFunction, BiFunction<byte[], Integer, T> readFunction)
    {
        byte[] destination = new byte[source.size() * elementSize];
        List<T> expected = new ArrayList<>();

        List<Integer> positions = IntStream.range(0, POSITIONS_PER_PAGE).boxed().collect(Collectors.toList());
        Collections.shuffle(positions);

        int index = 0;
        for (int i = 0; i < POSITIONS_PER_PAGE; i++) {
            int position = positions.get(i);
            index = writeFunction.apply(destination, index, source.get(position));
            expected.add(source.get(position));
        }

        assertEquals(index, POSITIONS_PER_PAGE * elementSize);
        assertCopied(expected, destination, elementSize, readFunction);
    }

    private static <T> void assertCopied(List<T> expected, byte[] actual, int elementSize, BiFunction<byte[], Integer, T> readFunction)
    {
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(readFunction.apply(actual, index * elementSize), expected.get(index));
        }
    }

    @FunctionalInterface
    private interface TriFunction<T, U, S>
    {
        int apply(T t, U u, S s);
    }
}
