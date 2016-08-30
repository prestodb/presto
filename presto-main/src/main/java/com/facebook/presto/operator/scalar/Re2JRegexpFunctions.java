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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.Re2JRegexp;
import com.facebook.presto.type.Re2JRegexpType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public final class Re2JRegexpFunctions
{
    private Re2JRegexpFunctions()
    {
    }

    @Description("returns substrings matching a regular expression")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean regexpLike(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern)
    {
        return pattern.matches(source);
    }

    @Description("removes substrings matching a regular expression")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpReplace(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern)
    {
        return regexpReplace(source, pattern, Slices.EMPTY_SLICE);
    }

    @Description("replaces substrings matching a regular expression by given string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpReplace(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern, @SqlType(StandardTypes.VARCHAR) Slice replacement)
    {
        return pattern.replace(source, replacement);
    }

    @Description("string(s) extracted using the given pattern")
    @ScalarFunction
    @SqlType("array<varchar>")
    public static Block regexpExtractAll(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern)
    {
        return regexpExtractAll(source, pattern, 0);
    }

    @Description("group(s) extracted using the given pattern")
    @ScalarFunction
    @SqlType("array<varchar>")
    public static Block regexpExtractAll(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        return pattern.extractAll(source, groupIndex);
    }

    @SqlNullable
    @Description("string extracted using the given pattern")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpExtract(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern)
    {
        return regexpExtract(source, pattern, 0);
    }

    @SqlNullable
    @Description("returns regex group of extracted string with a pattern")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpExtract(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        return pattern.extract(source, groupIndex);
    }

    @ScalarFunction
    @Description("returns array of strings split by pattern")
    @SqlType("array<varchar>")
    public static Block regexpSplit(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern)
    {
        return pattern.split(source);
    }
}
