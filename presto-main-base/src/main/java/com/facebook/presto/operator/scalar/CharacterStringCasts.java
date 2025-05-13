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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.LiteralParameter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.Chars.padSpaces;
import static com.facebook.presto.common.type.Chars.truncateToLengthAndTrimSpaces;
import static com.facebook.presto.common.type.Varchars.truncateToLength;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.setCodePointAt;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;

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
        return truncateToLengthAndTrimSpaces(slice, y.intValue());
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
    public static Slice varcharToCharSaturatedFloorCast(@LiteralParameter("y") long y, @SqlType("varchar(x)") Slice slice)
    {
        List<Integer> codePoints = new ArrayList<>(toCodePoints(slice));

        // if Varchar(x) value length (including spaces) is greater than y, we can just truncate it
        if (codePoints.size() >= y) {
            // char(y) slice representation doesn't contain trailing spaces
            codePoints = trimTrailing(codePoints, ' ');
            List<Integer> codePointsTruncated = codePoints.stream()
                    .limit(y)
                    .collect(toImmutableList());
            return codePointsToSliceUtf8(codePointsTruncated);
        }

        /*
         * Value length is smaller than same-represented char(y) value because input varchar has length lower than y.
         * We decrement last character in input (in fact, we decrement last non-zero character) and pad the value with
         * max code point up to y characters.
         */
        codePoints = trimTrailing(codePoints, '\0');

        if (codePoints.isEmpty()) {
            // No non-zero characters in input and input is shorter than y. Input value is smaller than any char(4) casted back to varchar, so we return the smallest char(4) possible
            return codePointsToSliceUtf8(nCopies(toIntExact(y), (int) '\0'));
        }

        codePoints = new ArrayList<>(codePoints);
        codePoints.set(codePoints.size() - 1, codePoints.get(codePoints.size() - 1) - 1);
        codePoints.addAll(nCopies(toIntExact(y) - codePoints.size(), Character.MAX_CODE_POINT));

        verify(codePoints.get(codePoints.size() - 1) != ' '); // no trailing spaces to trim

        return codePointsToSliceUtf8(codePoints);
    }

    private static List<Integer> trimTrailing(List<Integer> codePoints, int codePointToTrim)
    {
        int endIndex = codePoints.size();
        while (endIndex > 0 && codePoints.get(endIndex - 1) == codePointToTrim) {
            endIndex--;
        }
        return ImmutableList.copyOf(codePoints.subList(0, endIndex));
    }

    private static List<Integer> toCodePoints(Slice slice)
    {
        ImmutableList.Builder<Integer> codePoints = ImmutableList.builder();
        for (int offset = 0; offset < slice.length(); ) {
            int codePoint = getCodePointAt(slice, offset);
            offset += lengthOfCodePoint(slice, offset);
            codePoints.add(codePoint);
        }
        return codePoints.build();
    }

    private static Slice codePointsToSliceUtf8(List<Integer> codePoints)
    {
        int length = codePoints.stream()
                .mapToInt(SliceUtf8::lengthOfCodePoint)
                .sum();

        Slice result = Slices.wrappedBuffer(new byte[length]);
        int offset = 0;
        for (int codePoint : codePoints) {
            setCodePointAt(codePoint, result, offset);
            offset += lengthOfCodePoint(codePoint);
        }

        return result;
    }
}
