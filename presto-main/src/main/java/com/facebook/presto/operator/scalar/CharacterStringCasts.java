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
import com.facebook.presto.type.LiteralParameter;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.Chars.padSpaces;
import static com.facebook.presto.spi.type.Chars.trimSpaces;
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
    public static Slice varcharToVarcharCast(@LiteralParameter("x") Long x, @LiteralParameter("y") Long y, @SqlType("varchar(x)") Slice slice)
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
    public static Slice charToCharCast(@LiteralParameter("x") Long x, @LiteralParameter("y") Long y, @SqlType("char(x)") Slice slice)
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
    public static Slice varcharToCharCast(@LiteralParameter("y") Long y, @SqlType("varchar(x)") Slice slice)
    {
        return trimSpacesAndTruncateToLength(slice, y.intValue());
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice charToVarcharCast(@LiteralParameter("x") Long x, @LiteralParameter("y") Long y, @SqlType("char(x)") Slice slice)
    {
        if (x.intValue() <= y.intValue()) {
            return padSpaces(slice, x.intValue());
        }

        return padSpaces(truncateToLength(slice, y.intValue()), y.intValue());
    }

    @ScalarOperator(OperatorType.SATURATED_FLOOR_CAST)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    // This function returns Char(y) value that is smaller than original Varchar(x) value. However, it is not necessarily the largest
    // Char(y) value that is smaller than the original Varchar(x) value. This is fine though for usage in TupleDomainTranslator.
    public static Slice varcharToCharSaturatedFloorCast(@LiteralParameter("y") Long y, @SqlType("varchar(x)") Slice slice)
    {
        Slice trimmedSlice = trimSpaces(slice);
        int trimmedTextLength = countCodePoints(trimmedSlice);
        int numberOfTrailingSpaces = slice.length() - trimmedSlice.length();

        // if Varchar(x) value length (including spaces) is greater than y, we can just truncate it
        if (trimmedTextLength + numberOfTrailingSpaces >= y) {
            return truncateToLength(trimmedSlice, y.intValue());
        }
        if (trimmedTextLength == 0) {
            return EMPTY_SLICE;
        }

        // if Varchar(x) value length (including spaces) is smaller than y, we truncate all spaces
        // and also remove one additional trailing character to get smaller Char(y) value
        return trimmedSlice.slice(0, offsetOfCodePoint(trimmedSlice, trimmedTextLength - 1));
    }
}
