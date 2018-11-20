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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.CodePointsType;
import com.facebook.presto.type.Constraint;
import com.facebook.presto.type.LiteralParameter;
import com.google.common.primitives.Ints;
import io.airlift.slice.InvalidCodePointException;
import io.airlift.slice.InvalidUtf8Exception;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;

import java.text.Normalizer;
import java.util.OptionalInt;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.Chars.padSpaces;
import static com.facebook.presto.spi.type.Chars.trimTrailingSpaces;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Failures.checkCondition;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.lengthOfCodePointSafe;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.airlift.slice.SliceUtf8.toLowerCase;
import static io.airlift.slice.SliceUtf8.toUpperCase;
import static io.airlift.slice.SliceUtf8.tryGetCodePointAt;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.SURROGATE;
import static java.lang.Math.toIntExact;

/**
 * Current implementation is based on code points from Unicode and does ignore grapheme cluster boundaries.
 * Therefore only some methods work correctly with grapheme cluster boundaries.
 */
public final class StringFunctions
{
    private StringFunctions() {}

    @Description("convert Unicode code point to a string")
    @ScalarFunction
    @SqlType("varchar(1)")
    public static Slice chr(@SqlType(StandardTypes.BIGINT) long codepoint)
    {
        try {
            return SliceUtf8.codePointToUtf8(Ints.saturatedCast(codepoint));
        }
        catch (InvalidCodePointException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Not a valid Unicode code point: " + codepoint, e);
        }
    }

    @Description("returns Unicode code point of a single character string")
    @ScalarFunction("codepoint")
    @SqlType(StandardTypes.INTEGER)
    public static long codepoint(@SqlType("varchar(1)") Slice slice)
    {
        checkCondition(countCodePoints(slice) == 1, INVALID_FUNCTION_ARGUMENT, "Input string must be a single character string");

        return getCodePointAt(slice, 0);
    }

    @Description("count of code points of the given string")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long length(@SqlType("varchar(x)") Slice slice)
    {
        return countCodePoints(slice);
    }

    @Description("count of code points of the given string")
    @ScalarFunction("length")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long charLength(@LiteralParameter("x") long x, @SqlType("char(x)") Slice slice)
    {
        return x;
    }

    @Description("greedily removes occurrences of a pattern in a string")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("varchar(x)")
    public static Slice replace(@SqlType("varchar(x)") Slice str, @SqlType("varchar(y)") Slice search)
    {
        return replace(str, search, Slices.EMPTY_SLICE);
    }

    @Description("greedily replaces occurrences of a pattern with a string")
    @ScalarFunction
    @LiteralParameters({"x", "y", "z", "u"})
    @Constraint(variable = "u", expression = "min(2147483647, x + z * (x + 1))")
    @SqlType("varchar(u)")
    public static Slice replace(@SqlType("varchar(x)") Slice str, @SqlType("varchar(y)") Slice search, @SqlType("varchar(z)") Slice replace)
    {
        // Empty search?
        if (search.length() == 0) {
            // With empty `search` we insert `replace` in front of every character and and the end
            Slice buffer = Slices.allocate((countCodePoints(str) + 1) * replace.length() + str.length());
            // Always start with replace
            buffer.setBytes(0, replace);
            int indexBuffer = replace.length();
            // After every code point insert `replace`
            int index = 0;
            while (index < str.length()) {
                int codePointLength = lengthOfCodePointSafe(str, index);
                // Append current code point
                buffer.setBytes(indexBuffer, str, index, codePointLength);
                indexBuffer += codePointLength;
                // Append `replace`
                buffer.setBytes(indexBuffer, replace);
                indexBuffer += replace.length();
                // Advance pointer to current code point
                index += codePointLength;
            }

            return buffer;
        }
        // Allocate a reasonable buffer
        Slice buffer = Slices.allocate(str.length());

        int index = 0;
        int indexBuffer = 0;
        while (index < str.length()) {
            int matchIndex = str.indexOf(search, index);
            // Found a match?
            if (matchIndex < 0) {
                // No match found so copy the rest of string
                int bytesToCopy = str.length() - index;
                buffer = Slices.ensureSize(buffer, indexBuffer + bytesToCopy);
                buffer.setBytes(indexBuffer, str, index, bytesToCopy);
                indexBuffer += bytesToCopy;

                break;
            }

            int bytesToCopy = matchIndex - index;
            buffer = Slices.ensureSize(buffer, indexBuffer + bytesToCopy + replace.length());
            // Non empty match?
            if (bytesToCopy > 0) {
                buffer.setBytes(indexBuffer, str, index, bytesToCopy);
                indexBuffer += bytesToCopy;
            }
            // Non empty replace?
            if (replace.length() > 0) {
                buffer.setBytes(indexBuffer, replace);
                indexBuffer += replace.length();
            }
            // Continue searching after match
            index = matchIndex + search.length();
        }

        return buffer.slice(0, indexBuffer);
    }

    @Description("reverse all code points in a given string")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice reverse(@SqlType("varchar(x)") Slice slice)
    {
        return SliceUtf8.reverse(slice);
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("strpos")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long stringPosition(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice substring)
    {
        if (substring.length() == 0) {
            return 1;
        }

        int index = string.indexOf(substring);
        if (index < 0) {
            return 0;
        }
        return countCodePoints(string, 0, index) + 1;
    }

    @Description("suffix starting at given index")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice substr(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start)
    {
        if ((start == 0) || utf8.length() == 0) {
            return Slices.EMPTY_SLICE;
        }

        int startCodePoint = Ints.saturatedCast(start);

        if (startCodePoint > 0) {
            int indexStart = offsetOfCodePoint(utf8, startCodePoint - 1);
            if (indexStart < 0) {
                // before beginning of string
                return Slices.EMPTY_SLICE;
            }
            int indexEnd = utf8.length();

            return utf8.slice(indexStart, indexEnd - indexStart);
        }

        // negative start is relative to end of string
        int codePoints = countCodePoints(utf8);
        startCodePoint += codePoints;

        // before beginning of string
        if (startCodePoint < 0) {
            return Slices.EMPTY_SLICE;
        }

        int indexStart = offsetOfCodePoint(utf8, startCodePoint);
        int indexEnd = utf8.length();

        return utf8.slice(indexStart, indexEnd - indexStart);
    }

    @Description("suffix starting at given index")
    @ScalarFunction("substr")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice charSubstr(@SqlType("char(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start)
    {
        return substr(utf8, start);
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice substr(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length)
    {
        if (start == 0 || (length <= 0) || (utf8.length() == 0)) {
            return Slices.EMPTY_SLICE;
        }

        int startCodePoint = Ints.saturatedCast(start);
        int lengthCodePoints = Ints.saturatedCast(length);

        if (startCodePoint > 0) {
            int indexStart = offsetOfCodePoint(utf8, startCodePoint - 1);
            if (indexStart < 0) {
                // before beginning of string
                return Slices.EMPTY_SLICE;
            }
            int indexEnd = offsetOfCodePoint(utf8, indexStart, lengthCodePoints);
            if (indexEnd < 0) {
                // after end of string
                indexEnd = utf8.length();
            }

            return utf8.slice(indexStart, indexEnd - indexStart);
        }

        // negative start is relative to end of string
        int codePoints = countCodePoints(utf8);
        startCodePoint += codePoints;

        // before beginning of string
        if (startCodePoint < 0) {
            return Slices.EMPTY_SLICE;
        }

        int indexStart = offsetOfCodePoint(utf8, startCodePoint);
        int indexEnd;
        if (startCodePoint + lengthCodePoints < codePoints) {
            indexEnd = offsetOfCodePoint(utf8, indexStart, lengthCodePoints);
        }
        else {
            indexEnd = utf8.length();
        }

        return utf8.slice(indexStart, indexEnd - indexStart);
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction("substr")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice charSubstr(@SqlType("char(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length)
    {
        return trimTrailingSpaces(substr(utf8, start, length));
    }

    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("array(varchar(x))")
    public static Block split(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice delimiter)
    {
        return split(string, delimiter, string.length() + 1);
    }

    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("array(varchar(x))")
    public static Block split(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice delimiter, @SqlType(StandardTypes.BIGINT) long limit)
    {
        checkCondition(limit > 0, INVALID_FUNCTION_ARGUMENT, "Limit must be positive");
        checkCondition(limit <= Integer.MAX_VALUE, INVALID_FUNCTION_ARGUMENT, "Limit is too large");
        checkCondition(delimiter.length() > 0, INVALID_FUNCTION_ARGUMENT, "The delimiter may not be the empty string");
        BlockBuilder parts = VARCHAR.createBlockBuilder(null, 1, string.length());
        // If limit is one, the last and only element is the complete string
        if (limit == 1) {
            VARCHAR.writeSlice(parts, string);
            return parts.build();
        }

        int index = 0;
        while (index < string.length()) {
            int splitIndex = string.indexOf(delimiter, index);
            // Found split?
            if (splitIndex < 0) {
                break;
            }
            // Add the part from current index to found split
            VARCHAR.writeSlice(parts, string, index, splitIndex - index);
            // Continue searching after delimiter
            index = splitIndex + delimiter.length();
            // Reached limit-1 parts so we can stop
            if (parts.getPositionCount() == limit - 1) {
                break;
            }
        }
        // Rest of string
        VARCHAR.writeSlice(parts, string, index, string.length() - index);

        return parts.build();
    }

    @SqlNullable
    @Description("splits a string by a delimiter and returns the specified field (counting from one)")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("varchar(x)")
    public static Slice splitPart(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice delimiter, @SqlType(StandardTypes.BIGINT) long index)
    {
        checkCondition(index > 0, INVALID_FUNCTION_ARGUMENT, "Index must be greater than zero");
        // Empty delimiter? Then every character will be a split
        if (delimiter.length() == 0) {
            int startCodePoint = toIntExact(index);

            int indexStart = offsetOfCodePoint(string, startCodePoint - 1);
            if (indexStart < 0) {
                // index too big
                return null;
            }
            int length = lengthOfCodePoint(string, indexStart);
            if (indexStart + length > string.length()) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid UTF-8 encoding");
            }
            return string.slice(indexStart, length);
        }

        int matchCount = 0;

        int previousIndex = 0;
        while (previousIndex < string.length()) {
            int matchIndex = string.indexOf(delimiter, previousIndex);
            // No match
            if (matchIndex < 0) {
                break;
            }
            // Reached the requested part?
            if (++matchCount == index) {
                return string.slice(previousIndex, matchIndex - previousIndex);
            }
            // Continue searching after the delimiter
            previousIndex = matchIndex + delimiter.length();
        }

        if (matchCount == index - 1) {
            // returns last section of the split
            return string.slice(previousIndex, string.length() - previousIndex);
        }

        // index is too big, null is returned
        return null;
    }

    @Description("removes whitespace from the beginning of a string")
    @ScalarFunction("ltrim")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice leftTrim(@SqlType("varchar(x)") Slice slice)
    {
        return SliceUtf8.leftTrim(slice);
    }

    @Description("removes whitespace from the beginning of a string")
    @ScalarFunction("ltrim")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice charLeftTrim(@SqlType("char(x)") Slice slice)
    {
        return SliceUtf8.leftTrim(slice);
    }

    @Description("removes whitespace from the end of a string")
    @ScalarFunction("rtrim")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice rightTrim(@SqlType("varchar(x)") Slice slice)
    {
        return SliceUtf8.rightTrim(slice);
    }

    @Description("removes whitespace from the end of a string")
    @ScalarFunction("rtrim")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice charRightTrim(@SqlType("char(x)") Slice slice)
    {
        return rightTrim(slice);
    }

    @Description("removes whitespace from the beginning and end of a string")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice trim(@SqlType("varchar(x)") Slice slice)
    {
        return SliceUtf8.trim(slice);
    }

    @Description("removes whitespace from the beginning and end of a string")
    @ScalarFunction("trim")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice charTrim(@SqlType("char(x)") Slice slice)
    {
        return trim(slice);
    }

    @Description("remove the longest string containing only given characters from the beginning of a string")
    @ScalarFunction("ltrim")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice leftTrim(@SqlType("varchar(x)") Slice slice, @SqlType(CodePointsType.NAME) int[] codePointsToTrim)
    {
        return SliceUtf8.leftTrim(slice, codePointsToTrim);
    }

    @Description("remove the longest string containing only given characters from the beginning of a string")
    @ScalarFunction("ltrim")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice charLeftTrim(@SqlType("char(x)") Slice slice, @SqlType(CodePointsType.NAME) int[] codePointsToTrim)
    {
        return leftTrim(slice, codePointsToTrim);
    }

    @Description("remove the longest string containing only given characters from the end of a string")
    @ScalarFunction("rtrim")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice rightTrim(@SqlType("varchar(x)") Slice slice, @SqlType(CodePointsType.NAME) int[] codePointsToTrim)
    {
        return SliceUtf8.rightTrim(slice, codePointsToTrim);
    }

    @Description("remove the longest string containing only given characters from the end of a string")
    @ScalarFunction("rtrim")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice charRightTrim(@SqlType("char(x)") Slice slice, @SqlType(CodePointsType.NAME) int[] codePointsToTrim)
    {
        return trimTrailingSpaces(rightTrim(slice, codePointsToTrim));
    }

    @Description("remove the longest string containing only given characters from the beginning and end of a string")
    @ScalarFunction("trim")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice trim(@SqlType("varchar(x)") Slice slice, @SqlType(CodePointsType.NAME) int[] codePointsToTrim)
    {
        return SliceUtf8.trim(slice, codePointsToTrim);
    }

    @Description("remove the longest string containing only given characters from the beginning and end of a string")
    @ScalarFunction("trim")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice charTrim(@SqlType("char(x)") Slice slice, @SqlType(CodePointsType.NAME) int[] codePointsToTrim)
    {
        return trimTrailingSpaces(trim(slice, codePointsToTrim));
    }

    @ScalarOperator(OperatorType.CAST)
    @LiteralParameters("x")
    @SqlType(CodePointsType.NAME)
    public static int[] castVarcharToCodePoints(@SqlType("varchar(x)") Slice slice)
    {
        return castToCodePoints(slice);
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(CodePointsType.NAME)
    @LiteralParameters("x")
    public static int[] castCharToCodePoints(@LiteralParameter("x") Long charLength, @SqlType("char(x)") Slice slice)
    {
        return castToCodePoints(padSpaces(slice, charLength.intValue()));
    }

    private static int[] castToCodePoints(Slice slice)
    {
        int[] codePoints = new int[safeCountCodePoints(slice)];
        int position = 0;
        for (int index = 0; index < codePoints.length; index++) {
            codePoints[index] = getCodePointAt(slice, position);
            position += lengthOfCodePoint(slice, position);
        }
        return codePoints;
    }

    private static int safeCountCodePoints(Slice slice)
    {
        int codePoints = 0;
        for (int position = 0; position < slice.length(); ) {
            int codePoint = tryGetCodePointAt(slice, position);
            if (codePoint < 0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid UTF-8 encoding in characters: " + slice.toStringUtf8());
            }
            position += lengthOfCodePoint(codePoint);
            codePoints++;
        }
        return codePoints;
    }

    @Description("converts the string to lower case")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice lower(@SqlType("varchar(x)") Slice slice)
    {
        return toLowerCase(slice);
    }

    @Description("converts the string to lower case")
    @ScalarFunction("lower")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice charLower(@SqlType("char(x)") Slice slice)
    {
        return lower(slice);
    }

    @Description("converts the string to upper case")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice upper(@SqlType("varchar(x)") Slice slice)
    {
        return toUpperCase(slice);
    }

    @Description("converts the string to upper case")
    @ScalarFunction("upper")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice charUpper(@SqlType("char(x)") Slice slice)
    {
        return upper(slice);
    }

    private static Slice pad(Slice text, long targetLength, Slice padString, int paddingOffset)
    {
        checkCondition(
                0 <= targetLength && targetLength <= Integer.MAX_VALUE,
                INVALID_FUNCTION_ARGUMENT,
                "Target length must be in the range [0.." + Integer.MAX_VALUE + "]");
        checkCondition(padString.length() > 0, INVALID_FUNCTION_ARGUMENT, "Padding string must not be empty");

        int textLength = countCodePoints(text);
        int resultLength = (int) targetLength;

        // if our target length is the same as our string then return our string
        if (textLength == resultLength) {
            return text;
        }

        // if our string is bigger than requested then truncate
        if (textLength > resultLength) {
            return SliceUtf8.substring(text, 0, resultLength);
        }

        // number of bytes in each code point
        int padStringLength = countCodePoints(padString);
        int[] padStringCounts = new int[padStringLength];
        for (int i = 0; i < padStringLength; ++i) {
            padStringCounts[i] = lengthOfCodePointSafe(padString, offsetOfCodePoint(padString, i));
        }

        // preallocate the result
        int bufferSize = text.length();
        for (int i = 0; i < resultLength - textLength; ++i) {
            bufferSize += padStringCounts[i % padStringLength];
        }

        Slice buffer = Slices.allocate(bufferSize);

        // fill in the existing string
        int countBytes = bufferSize - text.length();
        int startPointOfExistingText = (paddingOffset + countBytes) % bufferSize;
        buffer.setBytes(startPointOfExistingText, text);

        // assign the pad string while there's enough space for it
        int byteIndex = paddingOffset;
        for (int i = 0; i < countBytes / padString.length(); ++i) {
            buffer.setBytes(byteIndex, padString);
            byteIndex += padString.length();
        }

        // handle the tail: at most we assign padStringLength - 1 code points
        buffer.setBytes(byteIndex, padString.getBytes(0, paddingOffset + countBytes - byteIndex));
        return buffer;
    }

    @Description("pads a string on the left")
    @ScalarFunction("lpad")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice leftPad(@SqlType("varchar(x)") Slice text, @SqlType(StandardTypes.BIGINT) long targetLength, @SqlType("varchar(y)") Slice padString)
    {
        return pad(text, targetLength, padString, 0);
    }

    @Description("pads a string on the right")
    @ScalarFunction("rpad")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice rightPad(@SqlType("varchar(x)") Slice text, @SqlType(StandardTypes.BIGINT) long targetLength, @SqlType("varchar(y)") Slice padString)
    {
        return pad(text, targetLength, padString, text.length());
    }

    @Description("computes Levenshtein distance between two strings")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long levenshteinDistance(@SqlType("varchar(x)") Slice left, @SqlType("varchar(y)") Slice right)
    {
        int[] leftCodePoints = castToCodePoints(left);
        int[] rightCodePoints = castToCodePoints(right);

        if (leftCodePoints.length < rightCodePoints.length) {
            int[] tempCodePoints = leftCodePoints;
            leftCodePoints = rightCodePoints;
            rightCodePoints = tempCodePoints;
        }

        if (rightCodePoints.length == 0) {
            return leftCodePoints.length;
        }

        checkCondition(
                (leftCodePoints.length * (rightCodePoints.length - 1)) <= 1_000_000,
                INVALID_FUNCTION_ARGUMENT,
                "The combined inputs for Levenshtein distance are too large");

        int[] distances = new int[rightCodePoints.length];
        for (int i = 0; i < rightCodePoints.length; i++) {
            distances[i] = i + 1;
        }

        for (int i = 0; i < leftCodePoints.length; i++) {
            int leftUpDistance = distances[0];
            if (leftCodePoints[i] == rightCodePoints[0]) {
                distances[0] = i;
            }
            else {
                distances[0] = Math.min(i, distances[0]) + 1;
            }
            for (int j = 1; j < rightCodePoints.length; j++) {
                int leftUpDistanceNext = distances[j];
                if (leftCodePoints[i] == rightCodePoints[j]) {
                    distances[j] = leftUpDistance;
                }
                else {
                    distances[j] = Math.min(distances[j - 1], Math.min(leftUpDistance, distances[j])) + 1;
                }
                leftUpDistance = leftUpDistanceNext;
            }
        }

        return distances[rightCodePoints.length - 1];
    }

    @Description("computes Hamming distance between two strings")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long hammingDistance(@SqlType("varchar(x)") Slice left, @SqlType("varchar(y)") Slice right)
    {
        int distance = 0;
        int leftPosition = 0;
        int rightPosition = 0;
        while (leftPosition < left.length() && rightPosition < right.length()) {
            int codePointLeft = tryGetCodePointAt(left, leftPosition);
            int codePointRight = tryGetCodePointAt(right, rightPosition);

            // if both code points are invalid, we do not care if they are equal
            // the following code treats them as equal if they happen to be of the same length
            if (codePointLeft != codePointRight) {
                distance++;
            }

            leftPosition += codePointLeft > 0 ? lengthOfCodePoint(codePointLeft) : -codePointLeft;
            rightPosition += codePointRight > 0 ? lengthOfCodePoint(codePointRight) : -codePointRight;
        }

        checkCondition(
                leftPosition == left.length() && rightPosition == right.length(),
                INVALID_FUNCTION_ARGUMENT,
                "The input strings to hamming_distance function must have the same length");

        return distance;
    }

    @Description("transforms the string to normalized form")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice normalize(@SqlType("varchar(x)") Slice slice, @SqlType("varchar(y)") Slice form)
    {
        Normalizer.Form targetForm;
        try {
            targetForm = Normalizer.Form.valueOf(form.toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Normalization form must be one of [NFD, NFC, NFKD, NFKC]");
        }
        return utf8Slice(Normalizer.normalize(slice.toStringUtf8(), targetForm));
    }

    @Description("decodes the UTF-8 encoded string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUtf8(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return SliceUtf8.fixInvalidUtf8(slice);
    }

    @Description("decodes the UTF-8 encoded string")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUtf8(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType("varchar(x)") Slice replacementCharacter)
    {
        int count = countCodePoints(replacementCharacter);
        if (count > 1) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Replacement character string must empty or a single character");
        }

        OptionalInt replacementCodePoint;
        if (count == 1) {
            try {
                replacementCodePoint = OptionalInt.of(getCodePointAt(replacementCharacter, 0));
            }
            catch (InvalidUtf8Exception e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid replacement character");
            }
        }
        else {
            replacementCodePoint = OptionalInt.empty();
        }
        return SliceUtf8.fixInvalidUtf8(slice, replacementCodePoint);
    }

    @Description("decodes the UTF-8 encoded string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUtf8(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.BIGINT) long replacementCodePoint)
    {
        if (replacementCodePoint > MAX_CODE_POINT || Character.getType((int) replacementCodePoint) == SURROGATE) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid replacement character");
        }
        return SliceUtf8.fixInvalidUtf8(slice, OptionalInt.of((int) replacementCodePoint));
    }

    @Description("encodes the string to UTF-8")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice toUtf8(@SqlType("varchar(x)") Slice slice)
    {
        return slice;
    }

    // TODO: implement N arguments char concat
    @Description("concatenates given character strings")
    @ScalarFunction
    @LiteralParameters({"x", "y", "u"})
    @Constraint(variable = "u", expression = "x + y")
    @SqlType("char(u)")
    public static Slice concat(@LiteralParameter("x") Long x, @SqlType("char(x)") Slice left, @SqlType("char(y)") Slice right)
    {
        int rightLength = right.length();
        if (rightLength == 0) {
            return left;
        }

        Slice paddedLeft = padSpaces(left, x.intValue());
        int leftLength = paddedLeft.length();

        Slice result = Slices.allocate(leftLength + rightLength);
        result.setBytes(0, paddedLeft);
        result.setBytes(leftLength, right);

        return result;
    }
}
