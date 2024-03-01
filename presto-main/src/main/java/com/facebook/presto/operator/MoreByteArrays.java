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

import com.facebook.presto.spi.api.Experimental;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

@Experimental
public class MoreByteArrays
{
    private static final Unsafe unsafe;

    static {
        try {
            // fetch theUnsafe object
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

    public static byte getByte(byte[] bytes, int index)
    {
        checkValidRange(index, SIZE_OF_BYTE, bytes.length);
        return unsafe.getByte(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET);
    }

    public static int fill(byte[] bytes, int index, int length, byte value)
    {
        requireNonNull(bytes, "bytes is null");
        checkValidRange(index, length, bytes.length);

        unsafe.setMemory(bytes, index + ARRAY_BYTE_BASE_OFFSET, length, value);
        return index + length;
    }

    public static int setBytes(byte[] bytes, int index, byte[] values, int offset, int length)
    {
        requireNonNull(bytes, "bytes is null");
        requireNonNull(values, "values is null");

        checkValidRange(index, length, bytes.length);
        checkValidRange(offset, length, values.length);

        // The performance of one copy and two copies (one big chunk at 8 bytes boundary + rest) are about the same.
        unsafe.copyMemory(values, (long) offset + ARRAY_BYTE_BASE_OFFSET, bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, length);
        return index + length;
    }

    public static int setInts(byte[] bytes, int index, int[] values, int offset, int length)
    {
        requireNonNull(bytes, "bytes is null");
        requireNonNull(values, "values is null");

        checkValidRange(index, length * ARRAY_INT_INDEX_SCALE, bytes.length);
        checkValidRange(offset, length, values.length);

        for (int i = offset; i < offset + length; i++) {
            unsafe.putInt(bytes, (long) index + ARRAY_BYTE_BASE_OFFSET, values[i]);
            index += ARRAY_INT_INDEX_SCALE;
        }
        return index;
    }

    private static void checkValidRange(int start, int length, int size)
    {
        if (start < 0 || length < 0 || start + length > size) {
            throw new IndexOutOfBoundsException(format("Invalid start %s and length %s with array size %s", start, length, size));
        }
    }

    private MoreByteArrays()
    {}
}
