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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.common.type.Varchars.byteCount;
import static com.facebook.presto.common.type.Varchars.truncateToLength;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class VarcharsTest
{
    @Test
    public void testTruncateToLength()
    {
        // Single byte code points
        assertEquals(truncateToLength(Slices.utf8Slice("abc"), 0), Slices.utf8Slice(""));
        assertEquals(truncateToLength(Slices.utf8Slice("abc"), 1), Slices.utf8Slice("a"));
        assertEquals(truncateToLength(Slices.utf8Slice("abc"), 4), Slices.utf8Slice("abc"));
        assertEquals(truncateToLength(Slices.utf8Slice("abcde"), 5), Slices.utf8Slice("abcde"));
        // 2 bytes code points
        assertEquals(truncateToLength(Slices.utf8Slice("абв"), 0), Slices.utf8Slice(""));
        assertEquals(truncateToLength(Slices.utf8Slice("абв"), 1), Slices.utf8Slice("а"));
        assertEquals(truncateToLength(Slices.utf8Slice("абв"), 4), Slices.utf8Slice("абв"));
        assertEquals(truncateToLength(Slices.utf8Slice("абвгд"), 5), Slices.utf8Slice("абвгд"));
        // 4 bytes code points
        assertEquals(truncateToLength(Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"), 0),
                Slices.utf8Slice(""));
        assertEquals(truncateToLength(Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"), 1),
                Slices.utf8Slice("\uD841\uDF0E"));
        assertEquals(truncateToLength(Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79"), 4),
                Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79"));
        assertEquals(truncateToLength(Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"), 5),
                Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"));

        assertEquals(truncateToLength(Slices.utf8Slice("abc"), createVarcharType(1)), Slices.utf8Slice("a"));
        assertEquals(truncateToLength(Slices.utf8Slice("abc"), (Type) createVarcharType(1)), Slices.utf8Slice("a"));
    }

    @Test
    public void testByteCount()
    {
        // Single byte code points
        assertByteCount("abc", 0, 0, 1, "");
        assertByteCount("abc", 0, 1, 0, "");
        assertByteCount("abc", 1, 1, 1, "b");
        assertByteCount("abc", 1, 1, 2, "b");
        assertByteCount("abc", 1, 2, 1, "b");
        assertByteCount("abc", 1, 2, 2, "bc");
        assertByteCount("abc", 1, 2, 3, "bc");
        assertByteCount("abc", 0, 3, 1, "a");
        assertByteCount("abc", 0, 3, 5, "abc");
        assertByteCountFailure("abc", 4, 5, 1);
        assertByteCountFailure("abc", 5, 0, 1);
        assertByteCountFailure("abc", -1, 1, 1);
        assertByteCountFailure("abc", 1, -1, 1);
        assertByteCountFailure("abc", 1, 1, -1);

        // 2 bytes code points
        assertByteCount("абв", 0, 0, 1, "");
        assertByteCount("абв", 0, 1, 0, "");
        assertByteCount("абв", 0, 2, 1, "а");
        assertByteCount("абв", 0, 4, 1, "а");
        assertByteCount("абв", 0, 1, 1, utf8Slice("а").getBytes(0, 1));
        assertByteCount("абв", 2, 2, 2, "б");
        assertByteCount("абв", 2, 2, 0, "");
        assertByteCount("абв", 0, 3, 5, utf8Slice("аб").getBytes(0, 3));
        assertByteCountFailure("абв", 8, 5, 1);
        // we do not check if the offset is in the middle of a code point
        assertByteCount("абв", 1, 1, 5, utf8Slice("а").getBytes(1, 1));
        assertByteCount("абв", 2, 1, 5, utf8Slice("б").getBytes(0, 1));

        // 3 bytes code points
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 0, 2, "");
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 1, 1, utf8Slice("\u6000").getBytes(0, 1));
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 2, 1, utf8Slice("\u6000").getBytes(0, 2));
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 3, 1, "\u6000");
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 6, 1, "\u6000");
        assertByteCount("\u6000\u6001\u6002\u6003", 6, 2, 4, utf8Slice("\u6002").getBytes(0, 2));
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 12, 6, "\u6000\u6001\u6002\u6003");
        // we do not check if the offset is in the middle of a code point
        assertByteCount("\u6000\u6001\u6002\u6003", 1, 6, 2, utf8Slice("\u6000\u6001\u6002").getBytes(1, 6));
        assertByteCount("\u6000\u6001\u6002\u6003", 2, 6, 2, utf8Slice("\u6000\u6001\u6002").getBytes(2, 6));
        assertByteCount("\u6000\u6001\u6002\u6003", 3, 6, 2, utf8Slice("\u6000\u6001\u6002").getBytes(3, 6));
        assertByteCountFailure("\u6000\u6001\u6002\u6003", 21, 0, 1);

        // invalid code points; always return the original lengths unless code point count is 0
        assertByteCount(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 2, 0, new byte[] {});
        assertByteCount(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 2, 1, new byte[] {(byte) 0x81, (byte) 0x81});
        assertByteCount(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 2, 3, new byte[] {(byte) 0x81, (byte) 0x81});
    }

    private static void assertByteCountFailure(String string, int offset, int length, int codePointCount)
    {
        try {
            byteCount(utf8Slice(string), offset, length, codePointCount);
            fail("Expected exception");
        }
        catch (IllegalArgumentException expected) {
        }
    }

    private static void assertByteCount(String actual, int offset, int length, int codePointCount, String expected)
    {
        assertByteCount(utf8Slice(actual).getBytes(), offset, length, codePointCount, utf8Slice(expected).getBytes());
    }

    private static void assertByteCount(String actual, int offset, int length, int codePointCount, byte[] expected)
    {
        assertByteCount(utf8Slice(actual).getBytes(), offset, length, codePointCount, expected);
    }

    private static void assertByteCount(byte[] actual, int offset, int length, int codePointCount, byte[] expected)
    {
        Slice slice = wrappedBuffer(actual);
        int truncatedLength = byteCount(slice, offset, length, codePointCount);
        byte[] bytes = slice.getBytes(offset, truncatedLength);
        assertEquals(bytes, expected);
    }
}
