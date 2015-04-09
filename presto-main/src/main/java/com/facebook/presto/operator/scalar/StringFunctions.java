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

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Ascii;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.ArrayType.toStackRepresentation;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class StringFunctions
{
    private StringFunctions()
    {
    }

    @Description("convert Unicode code point to a string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice chr(@SqlType(StandardTypes.BIGINT) long codepoint)
    {
        char[] utf16 = codePointChars(codepoint);
        ByteBuffer utf8 = UTF_8.encode(CharBuffer.wrap(utf16));
        return Slices.wrappedBuffer(utf8.array(), 0, utf8.limit());
    }

    private static char[] codePointChars(long codepoint)
    {
        try {
            return Character.toChars(Ints.checkedCast(codepoint));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Not a valid Unicode code point: " + codepoint);
        }
    }

    @Description("concatenates given strings")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concat(@SqlType(StandardTypes.VARCHAR) Slice str1, @SqlType(StandardTypes.VARCHAR) Slice str2)
    {
        Slice concat = Slices.allocate(str1.length() + str2.length());
        concat.setBytes(0, str1);
        concat.setBytes(str1.length(), str2);
        return concat;
    }

    @Description("length of the given string")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long length(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return UnicodeUtil.countCodePoints(slice);
    }

    @Description("greedily removes occurrences of a pattern in a string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice replace(@SqlType(StandardTypes.VARCHAR) Slice str, @SqlType(StandardTypes.VARCHAR) Slice search)
    {
        return replace(str, search, Slices.EMPTY_SLICE);
    }

    @Description("greedily replaces occurrences of a pattern with a string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice replace(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice search, @SqlType(StandardTypes.VARCHAR) Slice replace)
    {
        // Empty search returns original string
        if (UnicodeUtil.isEmpty(search)) {
            return string;
        }
        // Allocate a reasonable buffer
        Slice buffer = Slices.allocate(string.length());

        int index = 0;
        int indexBuffer = 0;
        while (index < string.length()) {
            final int matchIndex = UnicodeUtil.findUtf8IndexOfString(string, index, string.length(), search);
            // Found a match?
            if (matchIndex < 0) {
                // No match found so copy the rest of string
                final int bytesToCopy = string.length() - index;
                buffer = Slices.ensureSize(buffer, indexBuffer + bytesToCopy);
                buffer.setBytes(indexBuffer, string, index, bytesToCopy);
                indexBuffer += bytesToCopy;

                break;
            }

            final int bytesToCopy = matchIndex - index;
            buffer = Slices.ensureSize(buffer, indexBuffer + bytesToCopy + replace.length());
            // Non empty match?
            if (bytesToCopy > 0) {
                buffer.setBytes(indexBuffer, string, index, bytesToCopy);
                indexBuffer += bytesToCopy;
            }
            // Non empty replace?
            if (!UnicodeUtil.isEmpty(replace)) {
                buffer.setBytes(indexBuffer, replace);
                indexBuffer += replace.length();
            }
            // Continue searching after match
            index = matchIndex + search.length();
        }

        return Slices.copyOf(buffer, 0, indexBuffer);
    }

    @Description("reverses the given string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice reverse(@SqlType(StandardTypes.VARCHAR) Slice string)
    {
        final Slice reverse = Slices.allocate(string.length());

        for (int i = 0, j = string.length(); i < string.length(); ) {
            final int startByte = string.getUnsignedByte(i);
            final int codePointLength = UnicodeUtil.lengthOfCodePoint(startByte);

            if (codePointLength == 1) {
                // In the special case of length 1, we can set the byte directly
                j--;
                reverse.setByte(j, startByte);
            }
            else {
                j -= codePointLength;
                // TODO Illegal start bytes could end in IOOB. Discuss this in review if it is ok.
                reverse.setBytes(j, string, i, codePointLength);
            }

            i += codePointLength;
        }

        return reverse;
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("strpos")
    @SqlType(StandardTypes.BIGINT)
    public static long stringPosition(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice substring)
    {
        final int index = UnicodeUtil.findUtf8IndexOfString(string, 0, string.length(), substring);
        if (index < 0) {
            return 0;
        }
        else {
            return UnicodeUtil.countCodePoints(string, index) + 1;
        }
    }

    @Description("suffix starting at given index")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice substr(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.BIGINT) long start)
    {
        return substr(string, start, UnicodeUtil.countCodePoints(string));
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice substr(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length)
    {
        if ((start == 0) || (length <= 0)) {
            return Slices.EMPTY_SLICE;
        }

        final int stringLength = UnicodeUtil.countCodePoints(string);
        if (start > 0) {
            // make start zero-based
            start--;
        }
        else {
            // negative start is relative to end of string
            start += stringLength;
            if (start < 0) {
                return Slices.EMPTY_SLICE;
            }
        }

        if ((start + length) > stringLength) {
            length = stringLength - start;
        }

        if (start >= stringLength) {
            return Slices.EMPTY_SLICE;
        }
        // Find start and end withing UTF-8 bytes
        final int indexStart = UnicodeUtil.findUtf8IndexOfCodePointPosition(string, (int) start);
        final int indexEnd = UnicodeUtil.findUtf8IndexOfCodePointPosition(string, (int) (start + length));

        return string.slice(indexStart, indexEnd - indexStart);
    }

    @ScalarFunction
    @SqlType("array<varchar>")
    public static Slice split(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice delimiter)
    {
        return split(string, delimiter, string.length());
    }

    @ScalarFunction
    @SqlType("array<varchar>")
    public static Slice split(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice delimiter, @SqlType(StandardTypes.BIGINT) long limit)
    {
        checkCondition(limit > 0, INVALID_FUNCTION_ARGUMENT, "Limit must be positive");
        checkCondition(limit <= Integer.MAX_VALUE, INVALID_FUNCTION_ARGUMENT, "Limit is too large");
        checkCondition(!UnicodeUtil.isEmpty(delimiter), INVALID_FUNCTION_ARGUMENT, "The delimiter may not be the empty string");

        final List<Slice> parts = new ArrayList<>();

        int index = 0;
        while (index < string.length()) {
            final int splitIndex = UnicodeUtil.findUtf8IndexOfString(string, index, string.length(), delimiter);
            // Found split?
            if (splitIndex < 0) {
                break;
            }
            // Non empty split?
            if (splitIndex > index) {
                if (parts.size() == limit - 1) {
                    break;
                }

                parts.add(string.slice(index, splitIndex - index));
            }
            // Continue searching after delimiter
            index = splitIndex + delimiter.length();
        }
        // Non-empty rest of string?
        if (index < string.length()) {
            parts.add(string.slice(index, string.length() - index));
        }

        return toStackRepresentation(parts, VARCHAR);
    }

    @Nullable
    @Description("splits a string by a delimiter and returns the specified field (counting from one)")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice splitPart(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice delimiter, @SqlType(StandardTypes.BIGINT) long index)
    {
        checkCondition(index > 0, INVALID_FUNCTION_ARGUMENT, "Index must be greater than zero");
        // Empty delimiter? Then every character will be a split
        if (UnicodeUtil.isEmpty(delimiter)) {
            final int stringLength = UnicodeUtil.countCodePoints(string);
            if (index > stringLength) {
                // index is too big, null is returned
                return null;
            }

            final int indexStart = UnicodeUtil.findUtf8IndexOfCodePointPosition(string, (int) index - 1);
            // TODO Illegal start bytes could end in IOOB. Discuss this in review if it is ok.
            return string.slice(indexStart, UnicodeUtil.lengthOfCodePoint(string.getUnsignedByte(indexStart)));
        }

        int matchCount = 0;

        int p = 0;
        while (p < string.length()) {
            final int matchIndex = UnicodeUtil.findUtf8IndexOfString(string, p, string.length(), delimiter);
            // No match
            if (matchIndex < 0) {
                break;
            }
            // Reached the requested part?
            if (++matchCount == index) {
                return string.slice(p, matchIndex - p);
            }
            // Continue searching after the delimiter
            p = matchIndex + delimiter.length();
        }

        if (matchCount == index - 1) {
            // returns last section of the split
            return string.slice(p, string.length() - p);
        }
        // index is too big, null is returned
        return null;
    }

    @Description("removes spaces from the beginning of a string")
    @ScalarFunction("ltrim")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice leftTrim(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        int start = firstNonSpace(slice);
        return slice.slice(start, slice.length() - start);
    }

    @Description("removes spaces from the end of a string")
    @ScalarFunction("rtrim")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice rightTrim(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        int end = lastNonSpace(slice);
        return slice.slice(0, end + 1);
    }

    @Description("removes spaces from the beginning and end of a string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice trim(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        int start = firstNonSpace(slice);
        if (start == slice.length()) {
            return Slices.EMPTY_SLICE;
        }

        int end = lastNonSpace(slice);
        assert (end >= 0) && (end >= start);

        return slice.slice(start, (end - start) + 1);
    }

    private static int firstNonSpace(Slice slice)
    {
        for (int i = 0; i < slice.length(); i++) {
            if (slice.getByte(i) != ' ') {
                return i;
            }
        }
        return slice.length();
    }

    private static int lastNonSpace(Slice slice)
    {
        for (int i = slice.length() - 1; i >= 0; i--) {
            if (slice.getByte(i) != ' ') {
                return i;
            }
        }
        return -1;
    }

    @Description("converts the alphabets in a string to lower case")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice lower(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        Slice upper = Slices.allocate(slice.length());
        for (int i = 0; i < slice.length(); i++) {
            upper.setByte(i, Ascii.toLowerCase((char) slice.getByte(i)));
        }
        return upper;
    }

    @Description("converts all the alphabets in the string to upper case")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice upper(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        Slice upper = Slices.allocate(slice.length());
        for (int i = 0; i < slice.length(); i++) {
            upper.setByte(i, Ascii.toUpperCase((char) slice.getByte(i)));
        }
        return upper;
    }
}
