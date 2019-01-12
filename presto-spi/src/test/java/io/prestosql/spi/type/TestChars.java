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
package io.prestosql.spi.type;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.spi.type.Chars.byteCountWithoutTrailingSpace;
import static io.prestosql.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestChars
{
    @Test
    public void testTruncateToLengthAndTrimSpaces()
    {
        assertEquals(utf8Slice("a"), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 1));
        assertEquals(utf8Slice("a"), truncateToLengthAndTrimSpaces(utf8Slice("a  "), 1));
        assertEquals(utf8Slice("a"), truncateToLengthAndTrimSpaces(utf8Slice("abc"), 1));
        assertEquals(utf8Slice(""), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 0));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c "), 3));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c "), 4));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c "), 5));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 3));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 4));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 5));
        assertEquals(utf8Slice(""), truncateToLengthAndTrimSpaces(utf8Slice("  "), 1));
        assertEquals(utf8Slice(""), truncateToLengthAndTrimSpaces(utf8Slice(""), 1));
    }

    @Test
    public void testByteCountWithoutTrailingSpaces()
    {
        // single byte code points
        assertByteCountWithoutTrailingSpace("abc def ", 1, 0, "");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 4, "abc");
        assertByteCountWithoutTrailingSpace("abc def ", 1, 3, "bc");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 3, "abc");
        assertByteCountWithoutTrailingSpace("abc def ", 1, 2, "bc");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 6, "abc de");
        assertByteCountWithoutTrailingSpace("abc def ", 1, 7, "bc def");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 7, "abc def");
        assertByteCountWithoutTrailingSpace("abc def ", 1, 6, "bc def");
        assertByteCountWithoutTrailingSpace("abc  def ", 3, 1, "");
        assertByteCountWithoutTrailingSpace("abc  def ", 3, 3, "  d");
        assertByteCountWithoutTrailingSpace("abc  def ", 3, 4, "  de");
        assertByteCountWithoutTrailingSpaceFailure("abc def ", 4, 9);
        assertByteCountWithoutTrailingSpaceFailure("abc def ", 12, 1);
        assertByteCountWithoutTrailingSpaceFailure("abc def ", -1, 1);
        assertByteCountWithoutTrailingSpaceFailure("abc def ", 1, -1);
        assertByteCountWithoutTrailingSpace("       ", 0, 4, "");
        assertByteCountWithoutTrailingSpace("       ", 0, 0, "");

        // invalid code points
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 2, new byte[] {(byte) 0x81, (byte) 0x81});
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 1, new byte[] {(byte) 0x81});
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 0, new byte[] {});
    }

    @Test
    public void testByteCountWithoutTrailingSpacesWithCodePointLimit()
    {
        // single byte code points
        assertByteCountWithoutTrailingSpace("abc def ", 1, 0, 1, "");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 3, 4, "abc");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 4, 4, "abc");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 4, 3, "abc");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 5, 4, "abc");

        // invalid code points
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) ' ', (byte) 0x81}, 0, 3, 3, new byte[] {(byte) 0x81, (byte) 0x81});
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) ' ', (byte) 0x81}, 0, 2, 3, new byte[] {(byte) 0x81, (byte) 0x81});
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) ' ', (byte) 0x81}, 0, 0, 3, new byte[] {});
    }

    private static void assertByteCountWithoutTrailingSpaceFailure(String string, int offset, int maxLength)
    {
        try {
            byteCountWithoutTrailingSpace(utf8Slice(string), offset, maxLength);
            fail("Expected exception");
        }
        catch (IllegalArgumentException expected) {
        }
    }

    private static void assertByteCountWithoutTrailingSpace(String actual, int offset, int length, String expected)
    {
        assertByteCountWithoutTrailingSpace(utf8Slice(actual).getBytes(), offset, length, utf8Slice(expected).getBytes());
    }

    private static void assertByteCountWithoutTrailingSpace(byte[] actual, int offset, int length, byte[] expected)
    {
        Slice slice = wrappedBuffer(actual);
        int trimmedLength = byteCountWithoutTrailingSpace(slice, offset, length);
        byte[] bytes = slice.getBytes(offset, trimmedLength);
        assertEquals(bytes, expected);
    }

    private static void assertByteCountWithoutTrailingSpace(String actual, int offset, int length, int codePointCount, String expected)
    {
        assertByteCountWithoutTrailingSpace(utf8Slice(actual).getBytes(), offset, length, codePointCount, utf8Slice(expected).getBytes());
    }

    private static void assertByteCountWithoutTrailingSpace(byte[] actual, int offset, int length, int codePointCount, byte[] expected)
    {
        Slice slice = wrappedBuffer(actual);
        int truncatedLength = byteCountWithoutTrailingSpace(slice, offset, length, codePointCount);
        byte[] bytes = slice.getBytes(offset, truncatedLength);
        assertEquals(bytes, expected);
    }
}
