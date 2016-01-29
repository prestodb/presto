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
import com.facebook.presto.operator.scalar.RegexpGenericPattern.RegexLibrary;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.RegexpType;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import static com.facebook.presto.SystemSessionProperties.RE2J_DFA_RETRIES;
import static com.facebook.presto.SystemSessionProperties.RE2J_DFA_STATES_LIMIT;
import static com.facebook.presto.SystemSessionProperties.REGEX_LIBRARY;
import static com.facebook.presto.operator.scalar.RegexpGenericPattern.RegexLibrary.JONI;
import static com.facebook.presto.operator.scalar.RegexpGenericPattern.RegexLibrary.RE2J;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;

public final class RegexpFunctions
{
    private RegexpFunctions()
    {
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(RegexpType.NAME)
    public static RegexpGenericPattern castToRegexp(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice pattern)
    {
        String regexLibraryName = session.getProperty(REGEX_LIBRARY, String.class);
        RegexLibrary regexLibrary;
        if (regexLibraryName.toUpperCase().equals("RE2J")) {
            regexLibrary = RE2J;
        }
        else if (regexLibraryName.toUpperCase().equals("JONI")) {
            regexLibrary = JONI;
        }
        else {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "The setting for '" + REGEX_LIBRARY + "' is not valid. It should be either RE2J or JONI.");
        }

        int dfaStatesLimit = session.getProperty(RE2J_DFA_STATES_LIMIT, Integer.class);
        if (dfaStatesLimit < 2) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "The setting for '" + RE2J_DFA_STATES_LIMIT + "' is not valid. Value must not be less than 2");
        }

        int dfaRetries = session.getProperty(RE2J_DFA_RETRIES, Integer.class);
        if (dfaRetries < 0) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "The setting for '" + RE2J_DFA_RETRIES + "' is not valid. Value must be not be less than 0");
        }

        RegexpGenericPattern regexGenericPattern = new RegexpGenericPattern(regexLibrary, dfaStatesLimit, dfaRetries);
        try {
            regexGenericPattern.createPatternObject(pattern);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        return regexGenericPattern;
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
