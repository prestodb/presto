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

import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.RegexpType;
import com.facebook.presto.type.SqlType;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.type.ArrayType.toStackRepresentation;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RegexpFunctions
{
    private RegexpFunctions()
    {
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(RegexpType.NAME)
    public static Pattern castToRegexp(@SqlType(StandardTypes.VARCHAR) Slice pattern)
    {
        try {
            return Pattern.compile(pattern.toString(UTF_8));
        }
        catch (PatternSyntaxException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("returns substrings matching a regular expression")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean regexpLike(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) Pattern pattern)
    {
        return pattern.matcher(source.toString(UTF_8)).find();
    }

    @Description("removes substrings matching a regular expression")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpReplace(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) Pattern pattern)
    {
        return regexpReplace(source, pattern, Slices.EMPTY_SLICE);
    }

    @Description("replaces substrings matching a regular expression by given string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpReplace(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) Pattern pattern, @SqlType(StandardTypes.VARCHAR) Slice replacement)
    {
        Matcher matcher = pattern.matcher(source.toString(UTF_8));
        String replaced = matcher.replaceAll(replacement.toString(UTF_8));
        return Slices.copiedBuffer(replaced, UTF_8);
    }

    @Description("string(s) extracted using the given pattern")
    @ScalarFunction
    @SqlType("array<varchar>")
    public static Slice regexpExtractAll(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) Pattern pattern)
    {
        return regexpExtractAll(source, pattern, 0);
    }

    @Description("group(s) extracted using the given pattern")
    @ScalarFunction
    @SqlType("array<varchar>")
    public static Slice regexpExtractAll(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) Pattern pattern, @SqlType(StandardTypes.BIGINT) long group)
    {
        Matcher matcher = pattern.matcher(source.toString(UTF_8));
        validateGroup(group, matcher);
        List<String> matches = new ArrayList<>();
        while (matcher.find()) {
            matches.add(matcher.group(Ints.checkedCast(group)));
        }
        return toStackRepresentation(matches);
    }

    @Nullable
    @Description("string extracted using the given pattern")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpExtract(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) Pattern pattern)
    {
        return regexpExtract(source, pattern, 0);
    }

    @Nullable
    @Description("returns regex group of extracted string with a pattern")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpExtract(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) Pattern pattern, @SqlType(StandardTypes.BIGINT) long group)
    {
        Matcher matcher = pattern.matcher(source.toString(UTF_8));
        validateGroup(group, matcher);
        if (!matcher.find()) {
            return null;
        }
        String extracted = matcher.group(Ints.checkedCast(group));
        if (extracted == null) {
            return null;
        }
        return Slices.utf8Slice(extracted);
    }

    private static void validateGroup(long group, Matcher matcher)
    {
        if (group < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Group cannot be negative");
        }
        if (group > matcher.groupCount()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Pattern has %d groups. Cannot access group %d", matcher.groupCount(), group));
        }
    }
}
