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
package com.facebook.presto.type;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.likematcher.LikeMatcher;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Optional;

import static com.facebook.presto.common.type.Chars.padSpaces;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.facebook.presto.util.Failures.checkCondition;

public final class LikeFunctions
{
    private LikeFunctions() {}

    @ScalarFunction(value = "like", visibility = HIDDEN)
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean likeChar(@LiteralParameter("x") Long x, @SqlType("char(x)") Slice value, @SqlType(LikePatternType.NAME) LikeMatcher pattern)
    {
        return likeVarchar(padSpaces(value, x.intValue()), pattern);
    }

    // TODO: this should not be callable from SQL
    @ScalarFunction(value = "like", visibility = HIDDEN)
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean likeVarchar(@SqlType("varchar(x)") Slice value, @SqlType(LikePatternType.NAME) LikeMatcher matcher)
    {
        if (value.hasByteArray()) {
            return matcher.match(value.byteArray(), value.byteArrayOffset(), value.length());
        }
        else {
            return matcher.match(value.getBytes(), 0, value.length());
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @LiteralParameters("x")
    @SqlType(LikePatternType.NAME)
    public static LikeMatcher castVarcharToLikePattern(@SqlType("varchar(x)") Slice pattern)
    {
        return likePattern(pattern);
    }

    @ScalarOperator(OperatorType.CAST)
    @LiteralParameters("x")
    @SqlType(LikePatternType.NAME)
    public static LikeMatcher castCharToLikePattern(@LiteralParameter("x") Long charLength, @SqlType("char(x)") Slice pattern)
    {
        return likePattern(padSpaces(pattern, charLength.intValue()));
    }

    public static LikeMatcher likePattern(Slice pattern)
    {
        return LikeMatcher.compile(pattern.toStringUtf8(), Optional.empty(), false);
    }

    @ScalarFunction(visibility = HIDDEN)
    @LiteralParameters({"x", "y"})
    @SqlType(LikePatternType.NAME)
    public static LikeMatcher likePattern(@SqlType("varchar(x)") Slice pattern, @SqlType("varchar(y)") Slice escape)
    {
        try {
            return LikeMatcher.compile(pattern.toStringUtf8(), getEscapeChar(escape), false);
        }
        catch (RuntimeException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    public static boolean isLikePattern(Slice pattern, Slice escape)
    {
        String stringPattern = pattern.toStringUtf8();
        if (escape == null) {
            return stringPattern.contains("%") || stringPattern.contains("_");
        }

        String stringEscape = escape.toStringUtf8();
        checkCondition(stringEscape.length() == 1, INVALID_FUNCTION_ARGUMENT, "Escape string must be a single character");

        char escapeChar = stringEscape.charAt(0);
        boolean escaped = false;
        boolean isLikePattern = false;
        for (int currentChar : stringPattern.codePoints().toArray()) {
            if (!escaped && (currentChar == escapeChar)) {
                escaped = true;
            }
            else if (escaped) {
                checkEscape(currentChar == '%' || currentChar == '_' || currentChar == escapeChar);
                escaped = false;
            }
            else if ((currentChar == '%') || (currentChar == '_')) {
                isLikePattern = true;
            }
        }
        checkEscape(!escaped);
        return isLikePattern;
    }

    public static Slice unescapeLiteralLikePattern(Slice pattern, Slice escape)
    {
        if (escape == null) {
            return pattern;
        }

        String stringEscape = escape.toStringUtf8();
        char escapeChar = stringEscape.charAt(0);
        String stringPattern = pattern.toStringUtf8();
        StringBuilder unescapedPattern = new StringBuilder(stringPattern.length());
        boolean escaped = false;
        for (int currentChar : stringPattern.codePoints().toArray()) {
            if (!escaped && (currentChar == escapeChar)) {
                escaped = true;
            }
            else {
                unescapedPattern.append(Character.toChars(currentChar));
                escaped = false;
            }
        }
        return Slices.utf8Slice(unescapedPattern.toString());
    }

    private static void checkEscape(boolean condition)
    {
        checkCondition(condition, INVALID_FUNCTION_ARGUMENT, "Escape character must be followed by '%%', '_' or the escape character itself");
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static Optional<Character> getEscapeChar(Slice escape)
    {
        String escapeString = escape.toStringUtf8();
        if (escapeString.isEmpty()) {
            // escaping disabled
            return Optional.empty(); // invalid character
        }
        if (escapeString.length() == 1) {
            return Optional.of(escapeString.charAt(0));
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Escape string must be a single character");
    }
}
