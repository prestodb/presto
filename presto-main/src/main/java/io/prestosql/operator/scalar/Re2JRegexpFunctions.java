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
package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.type.Constraint;
import io.prestosql.type.Re2JRegexp;
import io.prestosql.type.Re2JRegexpType;

public final class Re2JRegexpFunctions
{
    private Re2JRegexpFunctions()
    {
    }

    @Description("returns substrings matching a regular expression")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean regexpLike(@SqlType("varchar(x)") Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern)
    {
        return pattern.matches(source);
    }

    @Description("removes substrings matching a regular expression")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpReplace(@SqlType("varchar(x)") Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern)
    {
        return regexpReplace(source, pattern, Slices.EMPTY_SLICE);
    }

    @Description("replaces substrings matching a regular expression by given string")
    @ScalarFunction
    @LiteralParameters({"x", "y", "z"})
    // Longest possible output is when the pattern is empty, than the replacement will be placed in between
    // any two letters of source (x + 1) times. As the replacement may be wildcard and the wildcard input that takes two letters
    // can produce (x) length output it max length is (x * y / 2) however for (x < 2), (y) itself (without wildcards)
    // may be longer, so we choose max of (x * y / 2) and (y). We than add the length we've added to basic length of source (x)
    // to get the formula: x + max(x * y / 2, y) * (x + 1)
    @Constraint(variable = "z", expression = "min(2147483647, x + max(x * y / 2, y) * (x + 1))")
    @SqlType("varchar(z)")
    public static Slice regexpReplace(@SqlType("varchar(x)") Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern, @SqlType("varchar(y)") Slice replacement)
    {
        return pattern.replace(source, replacement);
    }

    @Description("string(s) extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("array<varchar(x)>")
    public static Block regexpExtractAll(@SqlType("varchar(x)") Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern)
    {
        return regexpExtractAll(source, pattern, 0);
    }

    @Description("group(s) extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("array<varchar(x)>")
    public static Block regexpExtractAll(@SqlType("varchar(x)") Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        return pattern.extractAll(source, groupIndex);
    }

    @SqlNullable
    @Description("string extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpExtract(@SqlType("varchar(x)") Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern)
    {
        return regexpExtract(source, pattern, 0);
    }

    @SqlNullable
    @Description("returns regex group of extracted string with a pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpExtract(@SqlType("varchar(x)") Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        return pattern.extract(source, groupIndex);
    }

    @ScalarFunction
    @Description("returns array of strings split by pattern")
    @LiteralParameters("x")
    @SqlType("array<varchar(x)>")
    public static Block regexpSplit(@SqlType("varchar(x)") Slice source, @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern)
    {
        return pattern.split(source);
    }
}
