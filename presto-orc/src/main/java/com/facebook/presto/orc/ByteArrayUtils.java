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
import static java.lang.Long.rotateLeft;
import static java.lang.Math.min;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class ByteArrayUtils
{
    // Constants from XXHash64
    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;

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
        long seed = 1;
        int i = 0;
        for (; i + 8 <= length; i += 8) {
            seed = mix(seed, unsafe.getLong(bytes, (long) ARRAY_BYTE_BASE_OFFSET + offset + i));
        }
        long lastWord = 0;
        for (; i < length; i++) {
            lastWord = bytes[offset + i] | (lastWord << 8);
        }
        return mix(seed, lastWord);
    }

    private static long mix(long current, long value)
    {
        return rotateLeft(current + value * PRIME64_2, 31) * PRIME64_1;
    }
}
