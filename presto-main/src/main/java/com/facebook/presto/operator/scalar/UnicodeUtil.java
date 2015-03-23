/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.facebook.presto.operator.scalar;


import io.airlift.slice.Slice;

/**
 * Ideas for fast counting based on
 * <a href="http://www.daemonology.net/blog/2008-06-05-faster-utf8-strlen.html">Even faster UTF-8 character counting</a>.
 */
final class UnicodeUtil {

    private UnicodeUtil() {
    }

    private final static int TOP_MASK32 = 0x80808080;
    private final static long TOP_MASK64 = 0x8080808080808080L;

    private final static int ONE_MULTIPLIER32 = 0x01010101;
    private final static long ONE_MULTIPLIER64 = 0x0101010101010101L;

    /**
     * Counts the code points within UTF8 encoded slice.
     */
    static int countCodePoints(final Slice slice) {
        //
        // Quick exit if empty string
        if (slice.length() == 0) {
            return 0;
        }

        int count = 0;
        int i = 0;
        //
        // Length rounded to 8 bytes
        final int length8 = slice.length() & 0x7FFFFFF8;
        for (; i < length8; i += 8) {
            //
            // Fetch 8 bytes as long
            long i64 = slice.getLong(i);
            //
            // Count bytes which are NOT the start of a code point
            i64 = ((i64 & TOP_MASK64) >> 7) & (~i64 >> 6);
            count += (i64 * ONE_MULTIPLIER64) >> 56;
        }
        //
        // Enough bytes left for 32 bits?
        if (i + 4 < slice.length()) {
            //
            // Fetch 4 bytes as integer
            int i32 = slice.getInt(i);
            //
            // Count bytes which are NOT the start of a code point
            i32 = ((i32 & TOP_MASK32) >> 7) & (~i32 >> 6);
            count += (i32 * ONE_MULTIPLIER32) >> 24;

            i += 4;
        }
        //
        // Do the rest one by one
        for (; i < slice.length(); i++) {
            int i8 = slice.getByte(i) & 0xff;
            //
            // Count bytes which are NOT the start of a code point
            count += (i8 >> 7) & (~i8 >> 6);
        }

        assert count <= slice.length();
        return slice.length() - count;
    }
}
