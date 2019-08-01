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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.facebook.presto.operator.UncheckedByteArrays.fill;
import static com.facebook.presto.operator.UncheckedByteArrays.getByte;
import static com.facebook.presto.operator.UncheckedByteArrays.getInt;
import static com.facebook.presto.operator.UncheckedByteArrays.getLong;
import static com.facebook.presto.operator.UncheckedByteArrays.getShort;
import static com.facebook.presto.operator.UncheckedByteArrays.setByte;
import static com.facebook.presto.operator.UncheckedByteArrays.setBytes;
import static com.facebook.presto.operator.UncheckedByteArrays.setInt;
import static com.facebook.presto.operator.UncheckedByteArrays.setInts;
import static com.facebook.presto.operator.UncheckedByteArrays.setLong;
import static com.facebook.presto.operator.UncheckedByteArrays.setShort;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

public class TestUncheckedByteArrays
{
    private static final int POSITIONS_PER_PAGE = 10000;
    private static final Random RANDOM = new Random(0);

    private TestUncheckedByteArrays()
    {}

    @Test
    public static void testSetByte()
    {
        final byte[] destination = new byte[POSITIONS_PER_PAGE];

        byte[] sourceBytes = new byte[POSITIONS_PER_PAGE];
        RANDOM.nextBytes(sourceBytes);
        List<Byte> source = Bytes.asList(sourceBytes);
        testSequential(
                source,
                ARRAY_BYTE_INDEX_SCALE,
                destination,
                (bytes, index, value) -> setByte((byte[]) bytes, (int) index, (byte) value),
                (bytes, index) -> getByte((byte[]) bytes, (int) index));

        source = new ArrayList<Byte>(Collections.nCopies(POSITIONS_PER_PAGE, (byte) 0));
        testRandom(
                source,
                ARRAY_BYTE_INDEX_SCALE,
                destination,
                (bytes, index, value) -> setByte((byte[]) bytes, (int) index, (byte) value),
                (bytes, index) -> getByte((byte[]) bytes, (int) index));
    }

    @Test
    public static void testSetShort()
    {
        final byte[] destination = new byte[POSITIONS_PER_PAGE * ARRAY_SHORT_INDEX_SCALE];

        List<Short> source = IntStream.range(0, POSITIONS_PER_PAGE).mapToObj(i -> (short) RANDOM.nextInt(256)).collect(Collectors.toList());
        testSequential(
                source,
                ARRAY_SHORT_INDEX_SCALE,
                destination,
                (bytes, index, value) -> setShort((byte[]) bytes, (int) index, (short) value),
                (bytes, index) -> getShort((byte[]) bytes, (int) index));

        source = new ArrayList<Short>(Collections.nCopies(POSITIONS_PER_PAGE, (short) 0));
        testRandom(
                source,
                ARRAY_SHORT_INDEX_SCALE,
                destination,
                (bytes, index, value) -> setShort((byte[]) bytes, (int) index, (short) value),
                (bytes, index) -> getShort((byte[]) bytes, (int) index));
    }

    @Test
    public static void testSetInt()
    {
        final byte[] destination = new byte[POSITIONS_PER_PAGE * ARRAY_INT_INDEX_SCALE];

        List<Integer> source = IntStream.range(0, POSITIONS_PER_PAGE).boxed().collect(Collectors.toList());
        testSequential(
                source,
                ARRAY_INT_INDEX_SCALE,
                destination,
                (bytes, index, value) -> setInt((byte[]) bytes, (int) index, (int) value),
                (bytes, index) -> getInt((byte[]) bytes, (int) index));

        source = new ArrayList<Integer>(Collections.nCopies(POSITIONS_PER_PAGE, 0));
        testRandom(
                source,
                ARRAY_INT_INDEX_SCALE,
                destination,
                (bytes, index, value) -> setInt((byte[]) bytes, (int) index, (int) value),
                (bytes, index) -> getInt((byte[]) bytes, (int) index));
    }

    @Test
    public static void testSetLong()
    {
        final byte[] destination = new byte[POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE];

        List<Long> source = LongStream.range(0, POSITIONS_PER_PAGE).boxed().collect(Collectors.toList());
        testSequential(
                source,
                ARRAY_LONG_INDEX_SCALE,
                destination,
                (bytes, index, value) -> setLong((byte[]) bytes, (int) index, (long) value),
                (bytes, index) -> getLong((byte[]) bytes, (int) index));

        source = new ArrayList<Long>(Collections.nCopies(POSITIONS_PER_PAGE, 0L));
        testRandom(
                source,
                ARRAY_LONG_INDEX_SCALE,
                destination,
                (bytes, index, value) -> setLong((byte[]) bytes, (int) index, (long) value),
                (bytes, index) -> getLong((byte[]) bytes, (int) index));
    }

    @Test
    public static void testFill()
    {
        final byte[] destination = new byte[POSITIONS_PER_PAGE];
        int filledBytes = fill(destination, 0, POSITIONS_PER_PAGE, (byte) 5);

        assertEquals(filledBytes, POSITIONS_PER_PAGE);

        ArrayList<Byte> expected = new ArrayList<Byte>(Collections.nCopies(POSITIONS_PER_PAGE, (byte) 5));
        assertCopied(expected, destination, ARRAY_BYTE_INDEX_SCALE, (bytes, index) -> getByte((byte[]) bytes, (int) index));
    }

    @Test
    public static void testSetBytes()
    {
        final byte[] destination = new byte[POSITIONS_PER_PAGE];

        byte[] source = new byte[POSITIONS_PER_PAGE];
        RANDOM.nextBytes(source);

        int setBytes = setBytes(destination, 0, source, 0, POSITIONS_PER_PAGE);

        assertEquals(setBytes, POSITIONS_PER_PAGE);

        assertCopied(Bytes.asList(source), destination, ARRAY_BYTE_INDEX_SCALE, (bytes, index) -> getByte((byte[]) bytes, (int) index));
    }

    @Test
    public static void testSetInts()
    {
        final byte[] destination = new byte[POSITIONS_PER_PAGE * ARRAY_INT_INDEX_SCALE];

        int copiedBytes = setInts(destination, 0, IntStream.range(0, POSITIONS_PER_PAGE).toArray(), 0, POSITIONS_PER_PAGE);

        assertEquals(copiedBytes, POSITIONS_PER_PAGE * ARRAY_INT_INDEX_SCALE);

        List<Integer> expected = IntStream.range(0, POSITIONS_PER_PAGE).boxed().collect(Collectors.toList());
        assertCopied(expected, destination, ARRAY_INT_INDEX_SCALE, (bytes, index) -> getInt((byte[]) bytes, (int) index));
    }

    private static <T> void testSequential(List<T> source, int elementSize, byte[] destination, TriFunction writeFunction, BiFunction readFunction)
    {
        int index = 0;
        for (int i = 0; i < source.size(); i++) {
            index = writeFunction.apply(destination, index, (T) source.get(i));
        }
        assertEquals(index, POSITIONS_PER_PAGE * elementSize);
        assertCopied(source, destination, elementSize, readFunction);
    }

    // source should contain identical elements.
    private static <T> void testRandom(List<T> source, int elementSize, byte[] destination, TriFunction writeFunction, BiFunction readFunction)
    {
        Arrays.fill(destination, (byte) 0);

        int index = 0;
        for (int i = 0; i < source.size(); i++) {
            int newIndex = writeFunction.apply(destination, index, (T) source.get(i));
            if (i % 2 == 0) {
                index = newIndex;
            }
        }

        assertEquals(index, POSITIONS_PER_PAGE / 2 * elementSize);
        assertCopied(source.subList(0, POSITIONS_PER_PAGE / 2), Arrays.copyOf(destination, POSITIONS_PER_PAGE / 2), elementSize, readFunction);
        assertCopied(source.subList(POSITIONS_PER_PAGE / 2 + 1, POSITIONS_PER_PAGE), Arrays.copyOf(destination, POSITIONS_PER_PAGE / 2), elementSize, readFunction);
    }

    private static <T> boolean assertCopied(List<T> expected, byte[] actual, int elementSize, BiFunction readFunction)
    {
        for (int index = 0; index < actual.length; index++) {
            if ((readFunction.apply(actual, index) != expected.get(index))) {
                return false;
            }
            index += elementSize;
        }
        return true;
    }

    @FunctionalInterface
    private interface TriFunction<T, U, S>
    {
        Integer apply(T t, U u, S s);
    }
}
