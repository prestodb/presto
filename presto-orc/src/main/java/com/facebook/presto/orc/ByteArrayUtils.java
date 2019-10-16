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
        long leftAddress = ARRAY_BYTE_BASE_OFFSET + leftOffset;
        long rightAddress = ARRAY_BYTE_BASE_OFFSET + rightOffset;

        int lengthToCompare = min(leftLength, rightLength);
        while (lengthToCompare >= SIZE_OF_LONG) {
            long leftLong = unsafe.getLong(left, leftAddress);
            long rightLong = unsafe.getLong(right, rightAddress);

            if (leftLong != rightLong) {
                return longBytesToLong(leftLong) < longBytesToLong(rightLong) ? -1 : 1;
            }

            leftAddress += SIZE_OF_LONG;
            rightAddress += SIZE_OF_LONG;
            lengthToCompare -= SIZE_OF_LONG;
        }

        while (lengthToCompare > 0) {
            int compareResult = compareUnsignedBytes(unsafe.getByte(left, leftAddress), unsafe.getByte(right, rightAddress));
            if (compareResult != 0) {
                return compareResult;
            }
            leftAddress++;
            rightAddress++;
            lengthToCompare--;
        }

        return Integer.compare(leftLength, rightLength);
    }

    private static int compareUnsignedBytes(byte thisByte, byte thatByte)
    {
        return Byte.toUnsignedInt(thisByte) - Byte.toUnsignedInt(thatByte);
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
