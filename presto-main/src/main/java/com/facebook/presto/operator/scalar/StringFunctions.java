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
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Ascii;
import com.google.common.base.Charsets;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import static com.google.common.base.Preconditions.checkArgument;

public final class StringFunctions
{
    private StringFunctions() {}

    @Description("convert unicode code point to a string")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice chr(@SqlType(BigintType.class) long codepoint)
    {
        char[] utf16 = Character.toChars((int) codepoint);
        ByteBuffer utf8 = Charsets.UTF_8.encode(CharBuffer.wrap(utf16));
        return Slices.wrappedBuffer(utf8.array(), 0, utf8.limit());
    }

    @Description("concatenates given strings")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice concat(@SqlType(VarcharType.class) Slice str1, @SqlType(VarcharType.class) Slice str2)
    {
        Slice concat = Slices.allocate(str1.length() + str2.length());
        concat.setBytes(0, str1);
        concat.setBytes(str1.length(), str2);
        return concat;
    }

    @Description("length of the given string")
    @ScalarFunction
    @SqlType(BigintType.class)
    public static long length(@SqlType(VarcharType.class) Slice slice)
    {
        return slice.length();
    }

    @Description("greedily removes occurrences of a pattern in a string")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice replace(@SqlType(VarcharType.class) Slice str, @SqlType(VarcharType.class) Slice search)
    {
        return replace(str, search, Slices.EMPTY_SLICE);
    }

    @Description("greedily replaces occurrences of a pattern with a string")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice replace(@SqlType(VarcharType.class) Slice str, @SqlType(VarcharType.class) Slice search, @SqlType(VarcharType.class) Slice replace)
    {
        String replaced = str.toString(Charsets.UTF_8).replace(
                search.toString(Charsets.UTF_8),
                replace.toString(Charsets.UTF_8));
        return Slices.copiedBuffer(replaced, Charsets.UTF_8);
    }

    @Description("reverses the given string")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice reverse(@SqlType(VarcharType.class) Slice slice)
    {
        Slice reverse = Slices.allocate(slice.length());
        for (int i = 0, j = slice.length() - 1; i < slice.length(); i++, j--) {
            reverse.setByte(j, slice.getByte(i));
        }
        return reverse;
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("strpos")
    @SqlType(BigintType.class)
    public static long stringPosition(@SqlType(VarcharType.class) Slice string, @SqlType(VarcharType.class) Slice substring)
    {
        if (substring.length() > string.length()) {
            return 0;
        }

        for (int i = 0; i <= (string.length() - substring.length()); i++) {
            if (string.equals(i, substring.length(), substring, 0, substring.length())) {
                return i + 1;
            }
        }

        return 0;
    }

    @Description("suffix starting at given index")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice substr(@SqlType(VarcharType.class) Slice slice, @SqlType(BigintType.class) long start)
    {
        return substr(slice, start, slice.length());
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice substr(@SqlType(VarcharType.class) Slice slice, @SqlType(BigintType.class) long start, @SqlType(BigintType.class) long length)
    {
        if ((start == 0) || (length <= 0)) {
            return Slices.EMPTY_SLICE;
        }

        if (start > 0) {
            // make start zero-based
            start--;
        }
        else {
            // negative start is relative to end of string
            start += slice.length();
            if (start < 0) {
                return Slices.EMPTY_SLICE;
            }
        }

        if ((start + length) > slice.length()) {
            length = slice.length() - start;
        }

        if (start >= slice.length()) {
            return Slices.EMPTY_SLICE;
        }

        return slice.slice((int) start, (int) length);
    }

    // TODO: Implement a more efficient string search
    @Nullable
    @Description("splits a string by a delimiter and returns the specified field (counting from one)")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice splitPart(@SqlType(VarcharType.class) Slice string, @SqlType(VarcharType.class) Slice delimiter, @SqlType(BigintType.class) long index)
    {
        checkArgument(index > 0, "Index must be greater than zero");

        if (delimiter.length() == 0) {
            if (index > string.length()) {
                // index is too big, null is returned
                return null;
            }
            return string.slice((int) (index - 1), 1);
        }

        int previousIndex = 0;
        int matchCount = 0;

        for (int i = 0; i <= (string.length() - delimiter.length()); i++) {
            if (string.equals(i, delimiter.length(), delimiter, 0, delimiter.length())) {
                matchCount++;
                if (matchCount == index) {
                    return string.slice(previousIndex, i - previousIndex);
                }
                // noinspection AssignmentToForLoopParameter
                i += (delimiter.length() - 1);
                previousIndex = i + 1;
            }
        }

        if (matchCount == index - 1) {
            // returns last section of the split
            return string.slice(previousIndex, string.length() - previousIndex);
        }

        // index is too big, null is returned
        return null;
    }

    @Description("removes spaces from the beginning of a string")
    @ScalarFunction("ltrim")
    @SqlType(VarcharType.class)
    public static Slice leftTrim(@SqlType(VarcharType.class) Slice slice)
    {
        int start = firstNonSpace(slice);
        return slice.slice(start, slice.length() - start);
    }

    @Description("removes spaces from the end of a string")
    @ScalarFunction("rtrim")
    @SqlType(VarcharType.class)
    public static Slice rightTrim(@SqlType(VarcharType.class) Slice slice)
    {
        int end = lastNonSpace(slice);
        return slice.slice(0, end + 1);
    }

    @Description("removes spaces from the beginning and end of a string")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice trim(@SqlType(VarcharType.class) Slice slice)
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
    @SqlType(VarcharType.class)
    public static Slice lower(@SqlType(VarcharType.class) Slice slice)
    {
        Slice upper = Slices.allocate(slice.length());
        for (int i = 0; i < slice.length(); i++) {
            upper.setByte(i, Ascii.toLowerCase((char) slice.getByte(i)));
        }
        return upper;
    }

    @Description("converts all the alphabets in the string to upper case")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice upper(@SqlType(VarcharType.class) Slice slice)
    {
        Slice upper = Slices.allocate(slice.length());
        for (int i = 0; i < slice.length(); i++) {
            upper.setByte(i, Ascii.toUpperCase((char) slice.getByte(i)));
        }
        return upper;
    }

    @Description("get the largest of the given values")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice greatest(@SqlType(VarcharType.class) Slice value1, @SqlType(VarcharType.class) Slice value2)
    {
        return value1.compareTo(value2) > 0 ? value1 : value2;
    }

    @Description("get the smallest of the given values")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice least(@SqlType(VarcharType.class) Slice value1, @SqlType(VarcharType.class) Slice value2)
    {
        return value1.compareTo(value2) < 0 ? value1 : value2;
    }
}
