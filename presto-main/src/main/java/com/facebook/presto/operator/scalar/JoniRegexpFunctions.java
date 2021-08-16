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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.Constraint;
import com.facebook.presto.type.JoniRegexpType;
import io.airlift.joni.Matcher;
import io.airlift.joni.Regex;
import io.airlift.joni.Region;
import io.airlift.joni.exception.ValueException;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.joni.Option.DEFAULT;
import static io.airlift.joni.Option.DONT_CAPTURE_GROUP;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class JoniRegexpFunctions
{
    private static final Block EMPTY_BLOCK = VarcharType.VARCHAR.createBlockBuilder(null, 0).build();
    private JoniRegexpFunctions()
    {
    }

    @Description("returns whether the pattern is contained within the string")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean regexpLike(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) Regex pattern)
    {
        Matcher matcher;
        int offset;
        if (source.hasByteArray()) {
            offset = source.byteArrayOffset();
            matcher = pattern.matcher(source.byteArray(), offset, offset + source.length());
        }
        else {
            offset = 0;
            matcher = pattern.matcher(source.getBytes());
        }

        return getMatchingOffset(matcher, offset, offset + source.length()) != -1;
    }

    @Description("removes substrings matching a regular expression")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpReplace(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) Regex pattern)
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
    public static Slice regexpReplace(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) Regex pattern, @SqlType("varchar(y)") Slice replacement)
    {
        Matcher matcher = pattern.matcher(source.getBytes());
        SliceOutput sliceOutput = new DynamicSliceOutput(source.length() + replacement.length() * 5);

        int lastEnd = 0;
        int nextStart = 0; // nextStart is the same as lastEnd, unless the last match was zero-width. In such case, nextStart is lastEnd + 1.
        while (true) {
            int offset = getMatchingOffset(matcher, nextStart, source.length());
            if (offset == -1) {
                break;
            }
            if (matcher.getEnd() == matcher.getBegin()) {
                nextStart = matcher.getEnd() + 1;
            }
            else {
                nextStart = matcher.getEnd();
            }
            Slice sliceBetweenReplacements = source.slice(lastEnd, matcher.getBegin() - lastEnd);
            lastEnd = matcher.getEnd();
            sliceOutput.appendBytes(sliceBetweenReplacements);
            appendReplacement(sliceOutput, source, pattern, matcher.getEagerRegion(), replacement);
        }
        sliceOutput.appendBytes(source.slice(lastEnd, source.length() - lastEnd));

        return sliceOutput.slice();
    }

    private static void appendReplacement(SliceOutput result, Slice source, Regex pattern, Region region, Slice replacement)
    {
        // Handle the following items:
        // 1. ${name};
        // 2. $0, $1, $123 (group 123, if exists; or group 12, if exists; or group 1);
        // 3. \\, \$, \t (literal 't').
        // 4. Anything that doesn't starts with \ or $ is considered regular bytes

        int idx = 0;

        while (idx < replacement.length()) {
            byte nextByte = replacement.getByte(idx);
            if (nextByte == '$') {
                idx++;
                if (idx == replacement.length()) { // not using checkArgument because `.toStringUtf8` is expensive
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacement.toStringUtf8());
                }
                nextByte = replacement.getByte(idx);
                int backref;
                if (nextByte == '{') { // case 1 in the above comment
                    idx++;
                    int startCursor = idx;
                    while (idx < replacement.length()) {
                        nextByte = replacement.getByte(idx);
                        if (nextByte == '}') {
                            break;
                        }
                        idx++;
                    }
                    byte[] groupName = replacement.getBytes(startCursor, idx - startCursor);
                    try {
                        backref = pattern.nameToBackrefNumber(groupName, 0, groupName.length, region);
                    }
                    catch (ValueException e) {
                        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: unknown group { " + new String(groupName, StandardCharsets.UTF_8) + " }");
                    }
                    idx++;
                }
                else { // case 2 in the above comment
                    backref = nextByte - '0';
                    if (backref < 0 || backref > 9) { // not using checkArgument because `.toStringUtf8` is expensive
                        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacement.toStringUtf8());
                    }
                    if (region.numRegs <= backref) {
                        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: unknown group " + backref);
                    }
                    idx++;
                    while (idx < replacement.length()) { // Adaptive group number: find largest group num that is not greater than actual number of groups
                        int nextDigit = replacement.getByte(idx) - '0';
                        if (nextDigit < 0 || nextDigit > 9) {
                            break;
                        }
                        int newBackref = (backref * 10) + nextDigit;
                        if (region.numRegs <= newBackref) {
                            break;
                        }
                        backref = newBackref;
                        idx++;
                    }
                }
                int beg = region.beg[backref];
                int end = region.end[backref];
                if (beg != -1 && end != -1) { // the specific group doesn't exist in the current match, skip
                    result.appendBytes(source.slice(beg, end - beg));
                }
            }
            else { // case 3 and 4 in the above comment
                if (nextByte == '\\') {
                    idx++;
                    if (idx == replacement.length()) { // not using checkArgument because `.toStringUtf8` is expensive
                        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacement.toStringUtf8());
                    }
                    nextByte = replacement.getByte(idx);
                }
                result.appendByte(nextByte);
                idx++;
            }
        }
    }

    @Description("string(s) extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("array(varchar(x))")
    public static Block regexpExtractAll(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) Regex pattern)
    {
        return regexpExtractAll(source, pattern, 0);
    }

    @Description("group(s) extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("array(varchar(x))")
    public static Block regexpExtractAll(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) Regex pattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        Matcher matcher = pattern.matcher(source.getBytes());
        int nextStart = 0;
        int offset = getMatchingOffset(matcher, nextStart, source.length(), false);
        if (offset == -1) {
            return EMPTY_BLOCK;
        }

        validateGroup(groupIndex, matcher.getEagerRegion());
        ArrayList<Integer> matches = new ArrayList<>(10);
        int group = toIntExact(groupIndex);
        do {
            int beg = matcher.getBegin();
            int end = matcher.getEnd();
            if (end == beg) {
                nextStart = beg + 1;
            }
            else {
                nextStart = end;
            }
            Region region = matcher.getEagerRegion();
            matches.add(region.beg[group]);
            matches.add(region.end[group]);
            offset = getMatchingOffset(matcher, nextStart, source.length(), false);
        } while (offset != -1);

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, matches.size());
        for (int i = 0; i < matches.size(); i += 2) {
            int beg = matches.get(i);
            int end = matches.get(i + 1);
            if (beg == -1 || end == -1) {
                blockBuilder.appendNull();
            }
            else {
                VARCHAR.writeSlice(blockBuilder, source, beg, end - beg);
            }
        }

        return blockBuilder;
    }

    @SqlNullable
    @Description("string extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpExtract(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) Regex pattern)
    {
        return regexpExtract(source, pattern, 0);
    }

    @SqlNullable
    @Description("returns regex group of extracted string with a pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpExtract(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) Regex pattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        Matcher matcher = pattern.matcher(source.getBytes());
        int group = toIntExact(groupIndex);

        int offset = getMatchingOffset(matcher, 0, source.length(), false);
        if (offset == -1) {
            return null;
        }

        validateGroup(groupIndex, matcher.getEagerRegion());
        Region region = matcher.getEagerRegion();
        int beg = region.beg[group];
        int end = region.end[group];
        if (beg == -1) {
            // end == -1 must be true
            return null;
        }

        return source.slice(beg, end - beg);
    }

    @ScalarFunction
    @LiteralParameters("x")
    @Description("returns array of strings split by pattern")
    @SqlType("array(varchar(x))")
    public static Block regexpSplit(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) Regex pattern)
    {
        Matcher matcher = pattern.matcher(source.getBytes());
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 32);

        int lastEnd = 0;
        int nextStart = 0;
        int offset = getMatchingOffset(matcher, nextStart, source.length());
        if (offset == -1) {
            VARCHAR.writeSlice(blockBuilder, source);
            return blockBuilder.build();
        }

        do {
            if (matcher.getEnd() == matcher.getBegin()) {
                nextStart = matcher.getEnd() + 1;
            }
            else {
                nextStart = matcher.getEnd();
            }
            VARCHAR.writeSlice(blockBuilder, source, lastEnd, matcher.getBegin() - lastEnd);
            lastEnd = matcher.getEnd();
            offset = getMatchingOffset(matcher, nextStart, source.length());
        } while (offset != -1);

        VARCHAR.writeSlice(blockBuilder, source.slice(lastEnd, source.length() - lastEnd));
        return blockBuilder.build();
    }

    private static int getMatchingOffset(Matcher matcher, int at, int range)
    {
        return getMatchingOffset(matcher, at, range, true);
    }

    private static int getMatchingOffset(Matcher matcher, int at, int range, boolean noGroups)
    {
        try {
            return matcher.searchInterruptible(at, range, noGroups ? DONT_CAPTURE_GROUP : DEFAULT);
        }
        catch (InterruptedException interruptedException) {
            throw new PrestoException(GENERIC_USER_ERROR, "Regexp matching interrupted");
        }
    }

    private static void validateGroup(long group, Region region)
    {
        if (group < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Group cannot be negative");
        }
        if (group > region.numRegs - 1) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Pattern has %d groups. Cannot access group %d", region.numRegs - 1, group));
        }
    }
}
