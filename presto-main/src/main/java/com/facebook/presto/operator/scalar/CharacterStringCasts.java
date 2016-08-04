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

import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.Chars;
import com.facebook.presto.type.FromLiteralParameter;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.Chars.trimSpacesAndTruncateToLength;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.airlift.slice.Slices.EMPTY_SLICE;

public final class CharacterStringCasts
{
    private CharacterStringCasts() {}

    @ScalarOperator(OperatorType.CAST)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToVarcharCast(@FromLiteralParameter("x") Long x, @FromLiteralParameter("y") Long y, @SqlType("varchar(x)") Slice slice)
    {
        if (x > y) {
            return truncateToLength(slice, y.intValue());
        }
        else {
            return slice;
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice charToCharCast(@FromLiteralParameter("x") Long x, @FromLiteralParameter("y") Long y, @SqlType("char(x)") Slice slice)
    {
        if (x > y) {
            return truncateToLength(slice, y.intValue());
        }
        else {
            return slice;
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToCharCast(@FromLiteralParameter("y") Long y, @SqlType("varchar(x)") Slice slice)
    {
        return trimSpacesAndTruncateToLength(slice, y.intValue());
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice charToVarcharCast(@FromLiteralParameter("y") Long y, @SqlType("char(x)") Slice slice)
    {
        int textLength = countCodePoints(slice);
        int resultLength = y.intValue();

        // if our target length is the same as our string then return our string
        if (textLength == resultLength) {
            return slice;
        }

        // if our string is bigger than requested then truncate
        if (textLength > resultLength) {
            return SliceUtf8.substring(slice, 0, resultLength);
        }

        // preallocate the result
        int bufferSize = slice.length() + resultLength - textLength;
        Slice buffer = Slices.allocate(bufferSize);

        // fill in the existing string
        buffer.setBytes(0, slice);

        // fill padding spaces
        for (int i = slice.length(); i < bufferSize; ++i) {
            buffer.setByte(i, ' ');
        }

        return buffer;
    }

    @ScalarOperator(OperatorType.SATURATED_FLOOR_CAST)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToCharSaturatedFloorCast(@FromLiteralParameter("y") Long y, @SqlType("varchar(x)") Slice slice)
    {
        Slice trimmedSlice = Chars.trimSpaces(slice);
        int trimmedTextLength = countCodePoints(trimmedSlice);
        int numberOfTrailingSpaces = slice.length() - trimmedSlice.length();

        if (trimmedTextLength + numberOfTrailingSpaces >= y) {
            return truncateToLength(trimmedSlice, y.intValue());
        }
        if (trimmedTextLength == 0) {
            return EMPTY_SLICE;
        }

        return trimmedSlice.slice(0, offsetOfCodePoint(trimmedSlice, trimmedTextLength - 1));
    }
}
