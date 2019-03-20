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

import io.airlift.slice.SliceOutput;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static java.nio.charset.StandardCharsets.UTF_8;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_BASE_OFFSET;

public class ByteArrayUtils
{
    static Unsafe unsafe;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ByteArrayUtils() {}

    public static int putLongValuesToBuffer(long[] values, int[] positions, int positionsOffset, int batchSize, int offsetBase, byte[] target, int targetIndex)
    {
        //if (sizeOf(target) < targetIndex + batchSize * ARRAY_LONG_INDEX_SCALE || targetIndex < 0) {
        if (target.length < targetIndex - ARRAY_BYTE_BASE_OFFSET + batchSize * ARRAY_LONG_INDEX_SCALE || targetIndex < 0) {
            throw new IndexOutOfBoundsException();
        }

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            // Note that positions is logical row number. It may not start from 0 in the values array.
            unsafe.putLong(target, (long) targetIndex, values[positions[i] + offsetBase]);
            targetIndex += ARRAY_LONG_INDEX_SCALE;
        }
        return targetIndex;
    }

    public static int putShortValuesToBuffer(short[] values, int[] positions, int positionsOffset, int batchSize, int offsetBase, byte[] target, int targetIndex)
    {
        //if (sizeOf(target) < targetIndex + positionCount * ARRAY_LONG_INDEX_SCALE || targetIndex < 0) {
        if (target.length < targetIndex - ARRAY_BYTE_BASE_OFFSET + batchSize * ARRAY_SHORT_BASE_OFFSET || targetIndex < 0) {
            throw new IndexOutOfBoundsException();
        }

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            // Note that positions is logical row number. It may not start from 0 in the values array.
            unsafe.putShort(target, (long) targetIndex, values[positions[i] + offsetBase]);
            targetIndex += ARRAY_SHORT_BASE_OFFSET;
        }
        return targetIndex;
    }

    public static int encodeNullsAsBits(boolean[] nulls, int[] positions, int positionCount, int offsetBase, byte[] target, int targetIndex)
    {
        // TODO: implement the logic just write the null bits. The boolean indicator shall not be added here.
        return targetIndex;
    }

    public static void setLong(byte[] bytes, int index, long value)
    {
        unsafe.putLong(bytes, (long) index, value);
    }

    public static void setShort(byte[] bytes, int index, short value)
    {
        unsafe.putShort(bytes, (long) index, value);
    }

    public static void setInt(byte[] bytes, int index, int value)
    {
        unsafe.putInt(bytes, (long) index, value);
    }

    static void writeLengthPrefixedString(SliceOutput output, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }
}
