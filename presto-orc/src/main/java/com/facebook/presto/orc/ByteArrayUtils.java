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
package com.facebook.presto.orc;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Math.min;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class ByteArrayUtils
{
    // Constant from MurMur hash.
    private static final long M = 0xc6a4a7935bd1e995L;

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

    private ByteArrayUtils() {}

    // Adapted from io.airlift.slice.Slice
    public static int compareRanges(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength)
    {
        int minLength = min(leftLength, rightLength);
        int i = 0;
        if (minLength >= SIZE_OF_LONG) {
            while (i < minLength) {
                long leftLong = unsafe.getLong(left, (long) ARRAY_BYTE_BASE_OFFSET + leftOffset + i);
                long rightLong = unsafe.getLong(right, (long) ARRAY_BYTE_BASE_OFFSET + rightOffset + i);

                if (leftLong != rightLong) {
                    return longBytesToLong(leftLong) < longBytesToLong(rightLong) ? -1 : 1;
                }

                i += SIZE_OF_LONG;
            }
            i -= SIZE_OF_LONG;
        }

        while (i < minLength) {
            int leftByte = Byte.toUnsignedInt(left[leftOffset + i]);
            int rightByte = Byte.toUnsignedInt(right[rightOffset + i]);

            if (leftByte != rightByte) {
                return leftByte - rightByte;
            }
            i++;
        }

        return Integer.compare(leftLength, rightLength);
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

    public static long hash(byte[] bytes, int offset, int length)
    {
        // Adaptation of Knuth multiplicative hash. Multiplication,
        // shift and xor. This is approx 3x less arithmetic than
        // Murmur hash.
        long seed = 1;
        int i = 0;
        for (; i + 8 <= length; i += 8) {
            seed = (seed * M * unsafe.getLong(bytes, (long) ARRAY_BYTE_BASE_OFFSET + offset + i)) ^ (seed >> 27);
        }
        long lastWord = 0;
        for (; i < length; i++) {
            lastWord = bytes[offset + i] | (lastWord << 8);
        }
        return (seed * M * lastWord) ^ (seed << 27);
    }
}
