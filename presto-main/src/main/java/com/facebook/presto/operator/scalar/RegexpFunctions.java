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
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.RegexpType;
import com.facebook.presto.type.SqlType;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class RegexpFunctions
{
    private RegexpFunctions()
    {
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(RegexpType.class)
    public static Pattern castToRegexp(@SqlType(VarcharType.class) Slice pattern)
    {
        try {
            return Pattern.compile(pattern.toString(UTF_8));
        }
        catch (PatternSyntaxException e) {
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT.toErrorCode(), e);
        }
    }

    @Description("returns substrings matching a regular expression")
    @ScalarFunction
    @SqlType(BooleanType.class)
    public static boolean regexpLike(@SqlType(VarcharType.class) Slice source, @SqlType(RegexpType.class) Pattern pattern)
    {
        return pattern.matcher(source.toString(UTF_8)).find();
    }

    @Description("removes substrings matching a regular expression")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice regexpReplace(@SqlType(VarcharType.class) Slice source, @SqlType(RegexpType.class) Pattern pattern)
    {
        return regexpReplace(source, pattern, Slices.EMPTY_SLICE);
    }

    @Description("replaces substrings matching a regular expression by given string")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice regexpReplace(@SqlType(VarcharType.class) Slice source, @SqlType(RegexpType.class) Pattern pattern, @SqlType(VarcharType.class) Slice replacement)
    {
        Matcher matcher = pattern.matcher(source.toString(UTF_8));
        String replaced = matcher.replaceAll(replacement.toString(UTF_8));
        return Slices.copiedBuffer(replaced, UTF_8);
    }

    @Nullable
    @Description("string extracted using the given pattern")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice regexpExtract(@SqlType(VarcharType.class) Slice source, @SqlType(RegexpType.class) Pattern pattern)
    {
        return regexpExtract(source, pattern, 0);
    }

    @Nullable
    @Description("returns regex group of extracted string with a pattern")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice regexpExtract(@SqlType(VarcharType.class) Slice source, @SqlType(RegexpType.class) Pattern pattern, @SqlType(BigintType.class) long group)
    {
        Matcher matcher = pattern.matcher(source.toString(UTF_8));
        if ((group < 0) || (group > matcher.groupCount())) {
            throw new IllegalArgumentException("invalid group count");
        }
        if (!matcher.find()) {
            return null;
        }
        String extracted = matcher.group(Ints.checkedCast(group));
        return Slices.copiedBuffer(extracted, UTF_8);
    }
}
