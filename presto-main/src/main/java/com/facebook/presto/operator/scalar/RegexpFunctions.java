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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.RegexpType;
import com.facebook.presto.type.SqlType;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class RegexpFunctions
{
    private RegexpFunctions(){}

    @ScalarOperator(OperatorType.CAST)
    @SqlType(RegexpType.NAME)
    public static RegexpGenericPattern castToRegexp(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice pattern)
    {
        String regexLibraryName = session.getSystemProperty("regex_library", String.class);
        if (regexLibraryName == null) {
            regexLibraryName = "JONI"; //default is JONI
        }
        RegexpGenericPattern regexGeneric = new RegexpGenericPattern(regexLibraryName);

        try {
            regexGeneric.createPatternObject(pattern);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        return regexGeneric;
    }

    @Description("returns substrings matching a regular expression")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean regexpLike(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) RegexpGenericPattern regexpGenericPattern)
    {
        return regexpGenericPattern.matches(source);
    }

    @Description("removes substrings matching a regular expression")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpReplace(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) RegexpGenericPattern regexpGenericPattern)
    {
        return regexpReplace(source, regexpGenericPattern, Slices.EMPTY_SLICE);
    }

    @Description("replaces substrings matching a regular expression by given string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpReplace(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) RegexpGenericPattern regexpGenericPattern, @SqlType(StandardTypes.VARCHAR) Slice replacement)
    {
        return regexpGenericPattern.replace(source, replacement);
    }

    @Description("string(s) extracted using the given pattern")
    @ScalarFunction
    @SqlType("array<varchar>")
    public static Block regexpExtractAll(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) RegexpGenericPattern regexpGenericPattern)
    {
        return regexpExtractAll(source, regexpGenericPattern, 0);
    }

    @Description("group(s) extracted using the given pattern")
    @ScalarFunction
    @SqlType("array<varchar>")
    public static Block regexpExtractAll(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) RegexpGenericPattern regexpGenericPattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        return regexpGenericPattern.extractAll(source, groupIndex);
    }

    @Nullable
    @Description("string extracted using the given pattern")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpExtract(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) RegexpGenericPattern regexpGenericPattern)
    {
        return regexpExtract(source, regexpGenericPattern, 0);
    }

    @Nullable
    @Description("returns regex group of extracted string with a pattern")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpExtract(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) RegexpGenericPattern regexpGenericPattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        return regexpGenericPattern.extract(source, groupIndex);
    }

    @ScalarFunction
    @Description("returns array of strings split by pattern")
    @SqlType("array<varchar>")
    public static Block regexpSplit(@SqlType(StandardTypes.VARCHAR) Slice source, @SqlType(RegexpType.NAME) RegexpGenericPattern regexpGenericPattern)
    {
        return regexpGenericPattern.split(source);
    }
}
