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
package com.facebook.presto.iceberg.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Utility class for Z-Order byte operations.
 * Converts various types to lexicographically ordered byte representations
 * and interleaves bits for Z-Order curve computation.
 *
 * <p>Based on Apache Iceberg's ZOrderByteUtils implementation:
 * https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/util/ZOrderByteUtils.java
 */
public final class ZOrderByteUtils
{
    public static final int PRIMITIVE_BUFFER_SIZE = 8;

    private ZOrderByteUtils() {}

    /**
     * Converts a byte value to ordered bytes.
     */
    public static Slice tinyintToOrderedBytes(byte val)
    {
        return wholeNumberOrderedBytes(val);
    }

    /**
     * Converts a short value to ordered bytes.
     */
    public static Slice shortToOrderedBytes(short val)
    {
        return wholeNumberOrderedBytes(val);
    }

    /**
     * Converts an int value to ordered bytes.
     */
    public static Slice intToOrderedBytes(int val)
    {
        return wholeNumberOrderedBytes(val);
    }

    /**
     * Converts a long value to ordered bytes.
     */
    public static Slice longToOrderedBytes(long val)
    {
        return wholeNumberOrderedBytes(val);
    }

    /**
     * Signed longs do not have their bytes in magnitude order because of the sign bit.
     * To fix this, flip the sign bit so that all negatives are ordered before positives.
     */
    private static Slice wholeNumberOrderedBytes(long val)
    {
        ByteBuffer buffer = ByteBuffer.allocate(PRIMITIVE_BUFFER_SIZE);
        buffer.putLong(val ^ 0x8000000000000000L);
        return Slices.wrappedBuffer(buffer.array());
    }

    /**
     * Converts a float value to ordered bytes.
     */
    public static Slice floatToOrderedBytes(float val)
    {
        return floatingPointOrderedBytes(val);
    }

    /**
     * Converts a double value to ordered bytes.
     */
    public static Slice doubleToOrderedBytes(double val)
    {
        return floatingPointOrderedBytes(val);
    }

    /**
     * IEEE 754: "If two floating-point numbers in the same format are ordered (say, x < y),
     * they are ordered the same way when their bits are reinterpreted as sign-magnitude integers."
     *
     * <p>Which means doubles can be treated as sign magnitude integers which can then be converted
     * into lexicographically comparable bytes.
     */
    private static Slice floatingPointOrderedBytes(double val)
    {
        ByteBuffer buffer = ByteBuffer.allocate(PRIMITIVE_BUFFER_SIZE);
        long lval = Double.doubleToLongBits(val);
        lval ^= ((lval >> (Integer.SIZE - 1)) | Long.MIN_VALUE);
        buffer.putLong(lval);
        return Slices.wrappedBuffer(buffer.array());
    }

    /**
     * Converts a boolean value to ordered bytes.
     */
    public static Slice booleanToOrderedBytes(boolean val)
    {
        byte[] bytes = new byte[PRIMITIVE_BUFFER_SIZE];
        bytes[0] = (byte) (val ? -127 : 0);
        return Slices.wrappedBuffer(bytes);
    }

    /**
     * Strings are lexicographically sortable BUT different byte array lengths will ruin the
     * Z-Ordering. This implementation uses a set size for all output byte representations,
     * truncating longer strings and right padding with 0 for shorter strings.
     */
    public static Slice stringToOrderedBytes(Slice val, int length)
    {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) 0x00);

        if (val != null) {
            byte[] inputBytes = val.getBytes();
            int copyLength = Math.min(inputBytes.length, length);
            System.arraycopy(inputBytes, 0, bytes, 0, copyLength);
        }

        return Slices.wrappedBuffer(bytes);
    }

    /**
     * Return a Slice with the given bytes truncated to length, or filled with 0's to length
     * depending on whether the given bytes are larger or smaller than the given length.
     */
    public static Slice byteTruncateOrFill(Slice val, int length)
    {
        byte[] bytes = new byte[length];

        if (val == null) {
            Arrays.fill(bytes, (byte) 0x00);
            return Slices.wrappedBuffer(bytes);
        }

        byte[] inputBytes = val.getBytes();
        if (inputBytes.length < length) {
            System.arraycopy(inputBytes, 0, bytes, 0, inputBytes.length);
            Arrays.fill(bytes, inputBytes.length, length, (byte) 0x00);
        }
        else {
            System.arraycopy(inputBytes, 0, bytes, 0, length);
        }

        return Slices.wrappedBuffer(bytes);
    }

    /**
     * Interleave bits using a naive loop. Variable length inputs are allowed but to get a consistent
     * ordering it is required that every column contribute the same number of bytes in each invocation.
     * Bits are interleaved from all columns that have a bit available at that position.
     * Once a column has no more bits to produce it is skipped in the interleaving.
     *
     * @param columnsBinary an array of ordered byte representations of the columns being ZOrdered
     * @param interleavedSize the number of bytes to use in the output
     * @return the column bytes interleaved
     */
    public static Slice interleaveBits(Slice[] columnsBinary, int interleavedSize)
    {
        byte[] interleavedBytes = new byte[interleavedSize];
        Arrays.fill(interleavedBytes, (byte) 0x00);

        int sourceColumn = 0;
        int sourceByte = 0;
        int sourceBit = 7;
        int interleaveByte = 0;
        int interleaveBit = 7;

        while (interleaveByte < interleavedSize) {
            // Get the source column bytes
            byte[] sourceBytes = columnsBinary[sourceColumn].getBytes();

            // Take the source bit from source byte and move it to the output bit position
            interleavedBytes[interleaveByte] |=
                    (sourceBytes[sourceByte] & 1 << sourceBit) >>> sourceBit << interleaveBit;
            --interleaveBit;

            // Check if an output byte has been completed
            if (interleaveBit == -1) {
                // Move to the next output byte
                interleaveByte++;
                // Move to the highest order bit of the new output byte
                interleaveBit = 7;
            }

            // Check if the last output byte has been completed
            if (interleaveByte == interleavedSize) {
                break;
            }

            // Find the next source bit to interleave
            do {
                // Move to next column
                ++sourceColumn;
                if (sourceColumn == columnsBinary.length) {
                    // If the last source column was used, reset to next bit of first column
                    sourceColumn = 0;
                    --sourceBit;
                    if (sourceBit == -1) {
                        // If the last bit of the source byte was used, reset to the highest bit of the next byte
                        sourceByte++;
                        sourceBit = 7;
                    }
                }
            }
            while (columnsBinary[sourceColumn].length() <= sourceByte);
        }

        return Slices.wrappedBuffer(interleavedBytes);
    }
}
