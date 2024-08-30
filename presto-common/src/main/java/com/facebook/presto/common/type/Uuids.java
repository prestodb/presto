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
package com.facebook.presto.common.type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility methods for working with UUIDs in Presto.
 */
public class Uuids
{
    private Uuids() {}

    /**
     * Converts raw UUID data represented by a 16-byte array in big-endian
     * format to Presto's in-memory representation of two longs
     *
     * @param bytes input byte array
     * @return a length 2 long array containing the two values which represent
     * a UUID.
     */
    public static long[] rawUuidValuesFromBigEndian(byte[] bytes)
    {
        long[] result = new long[2];
        rawUuidValuesFromBigEndian(result, bytes, 0);
        return result;
    }

    /**
     * Converts raw UUID data represented by a byte array starting at index 0 in
     * big-endian format to Presto's in-memory representation of two longs and
     * places them into a buffer at dstIndex and distIndex + 1
     *
     * @param buffer the buffer to store the converted values
     * @param bytes the input bytes
     * @param dstIndex the starting destination index in the result buffer
     */
    public static void rawUuidValuesFromBigEndian(long[] buffer, byte[] bytes, int dstIndex)
    {
        rawUuidValuesFromBigEndian(buffer, bytes, 0, dstIndex);
    }

    /**
     * Converts raw UUID data represented by a byte array starting at a given
     * index in big-endian format to Presto's in-memory representation of two
     * longs and places them into a buffer at dstIndex and distIndex + 1
     *
     * @param buffer the buffer to store the converted values
     * @param bytes the input bytes
     * @param srcIndex the index in the byte array to start reading values
     * @param dstIndex the starting destination index in the result buffer
     */
    public static void rawUuidValuesFromBigEndian(long[] buffer, byte[] bytes, int srcIndex, int dstIndex)
    {
        ByteBuffer buf = ByteBuffer.wrap(bytes)
                .order(ByteOrder.BIG_ENDIAN);
        buf.position(srcIndex);
        buffer[dstIndex] = buf.getLong();
        buffer[dstIndex + 1] = buf.getLong();
    }
}
