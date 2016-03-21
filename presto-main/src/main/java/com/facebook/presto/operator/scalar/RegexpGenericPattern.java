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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.google.common.primitives.Ints;
import com.google.re2j.Options;
import com.google.re2j.Options.EventsListener;
import com.google.re2j.Pattern;
import io.airlift.jcodings.specific.NonStrictUTF8Encoding;
import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.joni.Region;
import io.airlift.joni.Syntax;
import io.airlift.joni.exception.ValueException;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;

import static com.facebook.presto.operator.scalar.RegexpGenericPattern.RegexLibrary.JONI;
import static com.facebook.presto.operator.scalar.RegexpGenericPattern.RegexLibrary.RE2J;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.re2j.Options.Algorithm.DFA_FALLBACK_TO_NFA;
import static java.lang.String.format;

public final class RegexpGenericPattern
{
    private static final java.util.regex.Pattern DOT_STAR_PREFIX_SUFFIX_PATTERN = java.util.regex.Pattern.compile("^(\\.\\*\\??)?(.*?)(\\.\\*\\??)?$");
    private static final int CORE_PATTERN_INDEX = 2;

    private static final Logger log = Logger.get(RegexpGenericPattern.class);

    public enum RegexLibrary
    {
        RE2J,
        JONI
    }

    public final RegexLibrary regexLibrary;
    public final int dfaStatesLimit;
    public final int dfaRetries;

    public Pattern re2jPattern;
    public Pattern re2jPatternWithoutDotStartPrefixSuffix;
    public Regex joniRegex;

    public RegexpGenericPattern(RegexLibrary regexLibrary, int dfaStatesLimit, int dfaRetries)
    {
        this.regexLibrary = regexLibrary;
        this.dfaStatesLimit = dfaStatesLimit;
        this.dfaRetries = dfaRetries;
    }

    public void createPatternObject(Slice pattern)
            throws Exception
    {
        if (regexLibrary == RE2J) {
            Options options = Options.builder()
                    .setAlgorithm(DFA_FALLBACK_TO_NFA)
                    .setMaximumNumberOfDFAStates(dfaStatesLimit)
                    .setNumberOfDFARetries(dfaRetries)
                    .setEventsListener(new RE2JEventsListener())
                    .build();

            String patternString = pattern.toStringUtf8();
            re2jPattern = Pattern.compile(patternString, options);

            // Remove .* prefix and suffix. This will give performance boost when using RE2J Pattern.find() function.
            Matcher dotStarPrefixSuffixMatcher = DOT_STAR_PREFIX_SUFFIX_PATTERN.matcher(patternString);
            checkState(dotStarPrefixSuffixMatcher.matches());
            String patternStringWithoutDotStartPrefixSuffix = dotStarPrefixSuffixMatcher.group(CORE_PATTERN_INDEX);

            if (!patternStringWithoutDotStartPrefixSuffix.equals(patternString)) {
                re2jPatternWithoutDotStartPrefixSuffix = Pattern.compile(patternStringWithoutDotStartPrefixSuffix, options);
            }
            else {
                re2jPatternWithoutDotStartPrefixSuffix = re2jPattern;
            }
        }
        else if (regexLibrary == JONI) {
            joniRegex = new Regex(pattern.getBytes(), 0, pattern.length(), Option.DEFAULT, NonStrictUTF8Encoding.INSTANCE, Syntax.Java);
        }
    }

    public boolean matches(Slice source)
    {
        if (regexLibrary == RE2J) {
            return re2jPatternWithoutDotStartPrefixSuffix.find(source);
        }
        else if (regexLibrary == JONI) {
            io.airlift.joni.Matcher m = joniRegex.matcher(source.getBytes());
            int offset = m.search(0, source.length(), Option.DEFAULT);
            return offset != -1;
        }
        return false;
    }

    public Slice replace(Slice source, Slice replacement)
    {
        if (regexLibrary == RE2J) {
            com.google.re2j.Matcher matcher = re2jPattern.matcher(source);
            try {
                return matcher.replaceAll(replacement);
            }
            catch (IndexOutOfBoundsException | IllegalArgumentException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacement.toStringUtf8());
            }
        }
        else if (regexLibrary == JONI) {
            io.airlift.joni.Matcher matcher = joniRegex.matcher(source.getBytes());
            SliceOutput sliceOutput = new DynamicSliceOutput(source.length() + replacement.length() * 5);

            int lastEnd = 0;
            int nextStart = 0; // nextStart is the same as lastEnd, unless the last match was zero-width. In such case, nextStart is lastEnd + 1.
            while (true) {
                int offset = matcher.search(nextStart, source.length(), Option.DEFAULT);
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
                appendReplacement(sliceOutput, source, matcher.getEagerRegion(), replacement);
            }
            sliceOutput.appendBytes(source.slice(lastEnd, source.length() - lastEnd));

            return sliceOutput.slice();
        }
        return null;
    }

    private void appendReplacement(SliceOutput result, Slice source, Region region, Slice replacement)
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
                        backref = joniRegex.nameToBackrefNumber(groupName, 0, groupName.length, region);
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

    public Block extractAll(Slice source, long groupIndex)
    {
        if (regexLibrary == RE2J) {
            com.google.re2j.Matcher matcher = re2jPattern.matcher(source);
            validateRe2jGroup(Ints.checkedCast(groupIndex), matcher.groupCount());

            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 32);
            while (true) {
                if (!matcher.find()) {
                    break;
                }

                Slice searchedGroup = matcher.group(Ints.checkedCast(groupIndex));
                if (searchedGroup == null) {
                    blockBuilder.appendNull();
                    continue;
                }
                VARCHAR.writeSlice(blockBuilder, searchedGroup);
            }
            return blockBuilder.build();
        }
        else if (regexLibrary == JONI) {
            io.airlift.joni.Matcher matcher = joniRegex.matcher(source.getBytes());
            validateJoniGroup(groupIndex, matcher.getEagerRegion());
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 32);
            int group = Ints.checkedCast(groupIndex);

            int nextStart = 0;
            while (true) {
                int offset = matcher.search(nextStart, source.length(), Option.DEFAULT);
                if (offset == -1) {
                    break;
                }
                if (matcher.getEnd() == matcher.getBegin()) {
                    nextStart = matcher.getEnd() + 1;
                }
                else {
                    nextStart = matcher.getEnd();
                }
                Region region = matcher.getEagerRegion();
                int beg = region.beg[group];
                int end = region.end[group];
                if (beg == -1 || end == -1) {
                    blockBuilder.appendNull();
                }
                else {
                    Slice slice = source.slice(beg, end - beg);
                    VARCHAR.writeSlice(blockBuilder, slice);
                }
            }
            return blockBuilder.build();
        }

        return null;
    }

    public Slice extract(Slice source, long groupIndex)
    {
        if (regexLibrary == RE2J) {
            com.google.re2j.Matcher matcher = re2jPattern.matcher(source);
            validateRe2jGroup(Ints.checkedCast(groupIndex), matcher.groupCount());
            int group = Ints.checkedCast(groupIndex);

            if (!matcher.find()) {
                return null;
            }

            return matcher.group(group);
        }
        else if (regexLibrary == JONI) {
            io.airlift.joni.Matcher matcher = joniRegex.matcher(source.getBytes());
            validateJoniGroup(groupIndex, matcher.getEagerRegion());
            int group = Ints.checkedCast(groupIndex);

            int offset = matcher.search(0, source.length(), Option.DEFAULT);
            if (offset == -1) {
                return null;
            }
            Region region = matcher.getEagerRegion();
            int beg = region.beg[group];
            int end = region.end[group];
            if (beg == -1) {
                // end == -1 must be true
                return null;
            }

            Slice slice = source.slice(beg, end - beg);
            return slice;
        }

        return null;
    }

    public Block split(Slice source)
    {
        if (regexLibrary == RE2J) {
            com.google.re2j.Matcher matcher = re2jPattern.matcher(source);
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 32);

            int lastEnd = 0;
            while (matcher.find()) {
                Slice slice = source.slice(lastEnd, matcher.start() - lastEnd);
                lastEnd = matcher.end();
                VARCHAR.writeSlice(blockBuilder, slice);
            }

            VARCHAR.writeSlice(blockBuilder, source.slice(lastEnd, source.length() - lastEnd));
            return blockBuilder.build();
        }
        else if (regexLibrary == JONI) {
            io.airlift.joni.Matcher matcher = joniRegex.matcher(source.getBytes());
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 32);

            int lastEnd = 0;
            int nextStart = 0;
            while (true) {
                int offset = matcher.search(nextStart, source.length(), Option.DEFAULT);
                if (offset == -1) {
                    break;
                }
                if (matcher.getEnd() == matcher.getBegin()) {
                    nextStart = matcher.getEnd() + 1;
                }
                else {
                    nextStart = matcher.getEnd();
                }
                Slice slice = source.slice(lastEnd, matcher.getBegin() - lastEnd);
                lastEnd = matcher.getEnd();
                VARCHAR.writeSlice(blockBuilder, slice);
            }
            VARCHAR.writeSlice(blockBuilder, source.slice(lastEnd, source.length() - lastEnd));

            return blockBuilder.build();
        }
        return null;
    }

    private void validateJoniGroup(long group, Region region)
    {
        if (group < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Group cannot be negative");
        }
        if (group > region.numRegs - 1) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Pattern has %d groups. Cannot access group %d", region.numRegs - 1, group));
        }
    }

    private void validateRe2jGroup(int group, int groupCount)
    {
        if (group < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Group cannot be negative");
        }
        if (group > groupCount) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Pattern has %d groups. Cannot access group %d", groupCount, group));
        }
    }

    private static class RE2JEventsListener
            implements EventsListener
    {
        @Override
        public void fallbackToNFA()
        {
            log.info("Fallback to NFA");
        }
    }
}
