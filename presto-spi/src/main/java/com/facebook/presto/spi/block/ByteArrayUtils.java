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

import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Math.min;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

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

    // Modified from io.airlift.slice.Slice.
    public static int memcmp(byte[] array1, int offset1, int length1, byte[] array2, int offset2, int length2)
    {
        long address1 = ARRAY_BYTE_BASE_OFFSET + offset1;
        long address2 = ARRAY_BYTE_BASE_OFFSET + offset2;

        int compareLength = min(length1, length2);
        while (compareLength >= SIZE_OF_LONG) {
            long thisLong = unsafe.getLong(array1, address1);
            long thatLong = unsafe.getLong(array2, address2);

            if (thisLong != thatLong) {
                return longBytesToLong(thisLong) < longBytesToLong(thatLong) ? -1 : 1;
            }

            address1 += SIZE_OF_LONG;
            address2 += SIZE_OF_LONG;
            compareLength -= SIZE_OF_LONG;
        }

        while (compareLength > 0) {
            byte thisByte = unsafe.getByte(array1, address1);
            byte thatByte = unsafe.getByte(array2, address2);

            int v = compareUnsignedBytes(thisByte, thatByte);
            if (v != 0) {
                return v;
            }
            address1++;
            address2++;
            compareLength--;
        }
        return Integer.compare(length1, length2);
    }

    private static int compareUnsignedBytes(byte thisByte, byte thatByte)
    {
        return unsignedByteToInt(thisByte) - unsignedByteToInt(thatByte);
    }

    private static int unsignedByteToInt(byte thisByte)
    {
        return thisByte & 0xFF;
    }

    /**
     * Turns a long representing a sequence of 8 bytes read in little-endian order
     * into a number that when compared produces the same effect as comparing the
     * original sequence of bytes lexicographically
     */
    private static long longBytesToLong(long bytes)
    {
        return Long.reverseBytes(bytes) ^ Long.MIN_VALUE;
    }
}
