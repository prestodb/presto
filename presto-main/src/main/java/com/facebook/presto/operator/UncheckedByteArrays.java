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

import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

public class UncheckedByteArrays
{
    private static final Unsafe unsafe;

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

    private UncheckedByteArrays()
    {}

    public static byte getByte(byte[] bytes, int index)
    {
        return unsafe.getByte(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET);
    }

    public static int setByte(byte[] bytes, int index, byte value)
    {
        unsafe.putByte(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, value);
        return index + ARRAY_BYTE_INDEX_SCALE;
    }

    public static short getShort(byte[] bytes, int index)
    {
        return unsafe.getShort(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET);
    }

    public static int setShort(byte[] bytes, int index, short value)
    {
        unsafe.putShort(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, value);
        return index + ARRAY_SHORT_INDEX_SCALE;
    }

    public static int getInt(byte[] bytes, int index)
    {
        return unsafe.getInt(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET);
    }

    public static int setInt(byte[] bytes, int index, int value)
    {
        unsafe.putInt(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, value);
        return index + ARRAY_INT_INDEX_SCALE;
    }

    public static long getLong(byte[] bytes, int index)
    {
        return unsafe.getLong(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET);
    }

    public static int setLong(byte[] bytes, int index, long value)
    {
        unsafe.putLong(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, value);
        return index + ARRAY_LONG_INDEX_SCALE;
    }

    public static int fill(byte[] bytes, int index, int length, byte value)
    {
        unsafe.setMemory(bytes, index + ARRAY_BYTE_BASE_OFFSET, length, value);
        return index + length;
    }

    public static int setBytes(byte[] bytes, int index, byte[] values, int offset, int length)
    {
        requireNonNull(bytes, "bytes is null");
        requireNonNull(values, "values is null");

        checkPositionIndex(index, length, bytes.length);
        checkPositionIndex(offset, length, values.length);

        // The performance of one copy and two copies (one big chunk at 8 bytes boundary + rest) are about the same.
        unsafe.copyMemory(values, (long) offset + ARRAY_BYTE_BASE_OFFSET, bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, length);
        return index + length;
    }

    public static int setInts(byte[] bytes, int index, int[] values, int offset, int length)
    {
        requireNonNull(bytes, "bytes is null");
        requireNonNull(values, "values is null");

        checkPositionIndex(index, length * ARRAY_INT_INDEX_SCALE, bytes.length);
        checkPositionIndex(offset, length, values.length);

        for (int i = offset; i < offset + length; i++) {
            unsafe.putInt(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, values[i]);
            index += ARRAY_INT_INDEX_SCALE;
        }
        return index;
    }

    public static void checkPositionIndex(int start, int length, int size)
    {
        if (start < 0 || length < 0 || start + length > size) {
            throw new IndexOutOfBoundsException();
        }
    }
}
