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
package com.facebook.presto.operator.scalar;

import io.airlift.slice.Slice;

/**
 * Ideas for fast counting based on
 * <a href="http://www.daemonology.net/blog/2008-06-05-faster-utf8-strlen.html">Even faster UTF-8 character counting</a>.
 */
final class UnicodeUtil
{
    private UnicodeUtil()
    {
    }

    private static final int TOP_MASK32 = 0x80808080;
    private static final long TOP_MASK64 = 0x8080808080808080L;

    private static final int ONE_MULTIPLIER32 = 0x01010101;
    private static final long ONE_MULTIPLIER64 = 0x0101010101010101L;

    /**
     * Counts the code points within UTF8 encoded slice.
     */
    static int countCodePoints(final Slice string)
    {
        return countCodePoints(string, string.length());
    }

    /**
     * Counts the code points within UTF8 encoded slice up to {@code end}.
     */
    static int countCodePoints(final Slice string, int end)
    {
        //
        // Quick exit if empty string
        if (end == 0) {
            return 0;
        }

        int count = 0;
        int i = 0;
        //
        // Length rounded to 8 bytes
        final int length8 = end & 0x7FFFFFF8;
        for (; i < length8; i += 8) {
            //
            // Fetch 8 bytes as long
            long i64 = string.getLong(i);
            //
            // Count bytes which are NOT the start of a code point
            i64 = ((i64 & TOP_MASK64) >> 7) & (~i64 >> 6);
            count += (i64 * ONE_MULTIPLIER64) >> 56;
        }
        //
        // Enough bytes left for 32 bits?
        if (i + 4 < end) {
            //
            // Fetch 4 bytes as integer
            int i32 = string.getInt(i);
            //
            // Count bytes which are NOT the start of a code point
            i32 = ((i32 & TOP_MASK32) >> 7) & (~i32 >> 6);
            count += (i32 * ONE_MULTIPLIER32) >> 24;

            i += 4;
        }
        //
        // Do the rest one by one
        for (; i < end; i++) {
            int i8 = string.getByte(i) & 0xff;
            //
            // Count bytes which are NOT the start of a code point
            count += (i8 >> 7) & (~i8 >> 6);
        }

        assert count <= end;
        return end - count;
    }

    /**
     * Finds the index of the first byte of the code point at a position.
     */
    static int findUtf8IndexOfCodePointPosition(final Slice string, final int codePointPosition)
    {
        //
        // Quick exit if we are sure that the position is after the end
        if (string.length() <= codePointPosition) {
            return string.length();
        }

        int correctIndex = codePointPosition;
        int i = 0;
        //
        // Length rounded to 8 bytes
        final int length8 = string.length() & 0x7FFFFFF8;
        //
        // While we have enough bytes left and we need at least 8 characters process 8 bytes at once
        while (i < length8 && correctIndex >= i + 8) {
            //
            // Fetch 8 bytes as long
            long i64 = string.getLong(i);
            //
            // Count bytes which are NOT the start of a code point
            i64 = ((i64 & TOP_MASK64) >> 7) & (~i64 >> 6);
            correctIndex += ((i64 * ONE_MULTIPLIER64) >> 56);

            i += 8;
        }
        //
        // Length rounded to 4 bytes
        final int length4 = string.length() & 0x7FFFFFFC;
        //
        // While we have enough bytes left and we need at least 4 characters process 4 bytes at once
        while (i < length4 && correctIndex >= i + 4) {
            //
            // Fetch 4 bytes as integer
            int i32 = string.getInt(i);
            //
            // Count bytes which are NOT the start of a code point
            i32 = ((i32 & TOP_MASK32) >> 7) & (~i32 >> 6);
            correctIndex += ((i32 * ONE_MULTIPLIER32) >> 24);

            i += 4;
        }
        //
        // Do the rest one by one, always check the last byte to find the end of the code point
        while (i < string.length()) {
            int i8 = string.getByte(i) & 0xff;
            //
            // Count bytes which are NOT the start of a code point
            correctIndex += ((i8 >> 7) & (~i8 >> 6));
            if (i == correctIndex) {
                return i;
            }

            i++;
        }

        assert i == string.length();
        return string.length();
    }

    /**
     * Find substring within UTF-8 encoding string.
     */
    static int findUtf8IndexOfString(final Slice string, int start, int end, final Slice substring)
    {
        if (substring.length() > end - start) {
            return -1;
        }

        for (int i = start; i <= (end - substring.length()); i++) {
            if (string.equals(i, substring.length(), substring, 0, substring.length())) {
                return i;
            }
        }

        return -1;
    }
}
