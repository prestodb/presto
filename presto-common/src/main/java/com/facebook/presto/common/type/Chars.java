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

import static com.facebook.presto.common.type.Varchars.byteCount;
import static com.facebook.presto.common.type.Varchars.truncateToLength;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static java.util.Objects.requireNonNull;

public final class Chars
{
    private Chars() {}

    public static boolean isCharType(Type type)
    {
        return type instanceof CharType;
    }

    public static Slice padSpaces(Slice slice, Type type)
    {
        requireNonNull(type, "type is null");
        if (!isCharType(type)) {
            throw new IllegalArgumentException("type must be the instance of CharType");
        }
        return padSpaces(slice, CharType.class.cast(type));
    }

    public static Slice padSpaces(Slice slice, CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return padSpaces(slice, charType.getLength());
    }

    public static Slice padSpaces(Slice slice, int length)
    {
        int textLength = countCodePoints(slice);

        // if our string is bigger than requested then truncate
        if (textLength > length) {
            throw new IllegalArgumentException("pad length is smaller than slice length");
        }

        // if our target length is the same as our string then return our string
        if (textLength == length) {
            return slice;
        }

        // preallocate the result
        int bufferSize = slice.length() + length - textLength;
        Slice buffer = Slices.allocate(bufferSize);

        // fill in the existing string
        buffer.setBytes(0, slice);

        // fill padding spaces
        for (int i = slice.length(); i < bufferSize; ++i) {
            buffer.setByte(i, ' ');
        }

        return buffer;
    }

    public static Slice truncateToLengthAndTrimSpaces(Slice slice, Type type)
    {
        requireNonNull(type, "type is null");
        if (!isCharType(type)) {
            throw new IllegalArgumentException("type must be the instance of CharType");
        }
        return truncateToLengthAndTrimSpaces(slice, CharType.class.cast(type));
    }

    public static Slice truncateToLengthAndTrimSpaces(Slice slice, CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return truncateToLengthAndTrimSpaces(slice, charType.getLength());
    }

    public static Slice truncateToLengthAndTrimSpaces(Slice slice, int maxLength)
    {
        requireNonNull(slice, "slice is null");
        if (maxLength < 0) {
            throw new IllegalArgumentException("Max length must be greater or equal than zero");
        }
        return trimTrailingSpaces(truncateToLength(slice, maxLength));
    }

    public static Slice trimTrailingSpaces(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        return slice.slice(0, byteCountWithoutTrailingSpace(slice, 0, slice.length()));
    }

    public static int byteCountWithoutTrailingSpace(Slice slice, int offset, int length)
    {
        requireNonNull(slice, "slice is null");
        if (length < 0) {
            throw new IllegalArgumentException("length must be greater than or equal to zero");
        }
        if (offset < 0 || offset + length > slice.length()) {
            throw new IllegalArgumentException("invalid offset/length");
        }
        for (int i = length + offset; i > offset; i--) {
            if (slice.getByte(i - 1) != ' ') {
                return i - offset;
            }
        }
        return 0;
    }

    public static int byteCountWithoutTrailingSpace(Slice slice, int offset, int length, int codePointCount)
    {
        int truncatedLength = byteCount(slice, offset, length, codePointCount);
        return byteCountWithoutTrailingSpace(slice, offset, truncatedLength);
    }

    public static int compareChars(Slice left, Slice right)
    {
        if (left.length() < right.length()) {
            return compareCharsShorterToLonger(left, right);
        }
        else {
            return -compareCharsShorterToLonger(right, left);
        }
    }

    private static int compareCharsShorterToLonger(Slice shorter, Slice longer)
    {
        for (int i = 0; i < shorter.length(); ++i) {
            int result = compareUnsignedBytes(shorter.getByte(i), longer.getByte(i));
            if (result != 0) {
                return result;
            }
        }

        for (int i = shorter.length(); i < longer.length(); ++i) {
            int result = compareUnsignedBytes((byte) ' ', longer.getByte(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private static int compareUnsignedBytes(byte thisByte, byte thatByte)
    {
        return unsignedByteToInt(thisByte) - unsignedByteToInt(thatByte);
    }

    private static int unsignedByteToInt(byte thisByte)
    {
        return thisByte & 0xFF;
    }
}
