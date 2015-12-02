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
import com.google.re2j.Pattern;

import io.airlift.jcodings.specific.NonStrictUTF8Encoding;
import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.joni.Region;
import io.airlift.joni.Syntax;
import io.airlift.joni.exception.ValueException;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.nio.charset.StandardCharsets;

import static com.facebook.presto.operator.scalar.RegexpGenericPattern.RegexLibrary.JONI;
import static com.facebook.presto.operator.scalar.RegexpGenericPattern.RegexLibrary.RE2J;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public final class RegexpGenericPattern
{
    enum RegexLibrary {
        RE2J,
        JONI
    }

    public Pattern re2jPattern;
    public Regex joniRegex;
    public final RegexLibrary selectedRegexLibrary;

    public RegexpGenericPattern(String regexLibraryName)
    {
        if (regexLibraryName.toUpperCase().equals("RE2J")) {
            selectedRegexLibrary = RE2J;
        }
        else if (regexLibraryName.toUpperCase().equals("JONI")) {
            selectedRegexLibrary = JONI;
        }
        else {
          throw new PrestoException(INVALID_SESSION_PROPERTY, "The setting for regex_library is not valid. It should be either RE2J or JONI.");
        }
    }

    public void createPatternObject(Slice pattern) throws Exception
    {
        if (selectedRegexLibrary == RE2J) {
            re2jPattern =  Pattern.compile(pattern.toStringUtf8());
        }
        else if (selectedRegexLibrary == JONI) {
            joniRegex = new Regex(pattern.getBytes(), 0, pattern.length(), Option.DEFAULT, NonStrictUTF8Encoding.INSTANCE, Syntax.Java);
        }
    }

    public boolean matches(Slice source)
    {
        if (selectedRegexLibrary == RE2J) {
            com.google.re2j.Matcher m = re2jPattern.matcher(source.toStringUtf8());
            return m.find();
        }
        else if (selectedRegexLibrary == JONI) {
            io.airlift.joni.Matcher m = joniRegex.matcher(source.getBytes());
            int offset = m.search(0, source.length(), Option.DEFAULT);
            return offset != -1;
        }
        return false;
    }

    //checking the replacement string contains unsupported chars/patterns:
    // 1) invalid group name/number
    // 2) backslash
    private void re2jCheckReplacementString(String replacementString, int groupCount)
    {
        int idx = 0;
        int replacementStringLength = replacementString.length();
        while (idx < replacementStringLength) {
            char nextChar = replacementString.charAt(idx);
            if (nextChar == '$') {
                idx++;
                if (idx == replacementStringLength) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacementString);
                }
                nextChar = replacementString.charAt(idx);
                if (nextChar != '{') {
                    int backref = nextChar - '0';
                    if (backref < 0 || backref > 9) {
                        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacementString);
                    }
                    if (groupCount < backref) {
                        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: unknown group " + backref);
                    }
                }
            }
            else {
                if (nextChar == '\\') {
                    idx++;
                    if (idx == replacementString.length()) {
                        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacementString);
                    }
                }
            }
            idx++;
        }
    }

    public Slice replace(Slice source, Slice replacement)
    {
        if (selectedRegexLibrary == RE2J) {
            String inputString =  source.toStringUtf8();
            String replacementString = replacement.toStringUtf8();
            com.google.re2j.Matcher matcher = re2jPattern.matcher(inputString);

            re2jCheckReplacementString(replacementString, re2jPattern.groupCount());
            String replacedString;

            try {
                replacedString = matcher.replaceAll(replacementString);
            }
            catch (Exception exp) {
               //any exception here is assumed to be due to INVALID_FUNCTION_ARGUMENT
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacementString);
            }

            SliceOutput sliceOutput = new DynamicSliceOutput(source.length() + replacement.length() * 5);
            sliceOutput.appendBytes(replacedString.getBytes(), 0, replacedString.length());
            return sliceOutput.slice();
        }
        else if (selectedRegexLibrary == JONI) {
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
        if (selectedRegexLibrary == RE2J) {
            String inputString =  source.toStringUtf8();
            com.google.re2j.Matcher matcher = re2jPattern.matcher(inputString);
            validateRe2jGroup(Ints.checkedCast(groupIndex), matcher.groupCount());

            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 32);
            while (true) {
                 if (!matcher.find()) {
                    break;
                 }

                 if (groupIndex == 0) { //does not need any specific group, return the whole match
                     Slice slice = source.slice(matcher.start(), matcher.end() - matcher.start());
                     VARCHAR.writeSlice(blockBuilder, slice);
                 }
                 else  {
                     String searchedGroupString = matcher.group(Ints.checkedCast(groupIndex));
                     if (searchedGroupString == null) {
                         blockBuilder.appendNull();
                         continue;
                     }
                     SliceOutput outputStringSlice = new DynamicSliceOutput(searchedGroupString.length());
                     outputStringSlice.writeBytes(searchedGroupString.getBytes(), 0, searchedGroupString.length());
                     VARCHAR.writeSlice(blockBuilder, outputStringSlice.slice());
                 }
            }
            return blockBuilder.build();
        }
        else if (selectedRegexLibrary == JONI) {
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
        if (selectedRegexLibrary == RE2J) {
            String inputString =  source.toStringUtf8();
            com.google.re2j.Matcher matcher = re2jPattern.matcher(inputString);
            validateRe2jGroup(Ints.checkedCast(groupIndex), matcher.groupCount());
            int group = Ints.checkedCast(groupIndex);

            if (!matcher.find()) {
                return null;
            }

            if (group == 0) { //return the whole matched string
                Slice slice = source.slice(matcher.start(), matcher.end() - matcher.start());
                return slice;
            }
            else {
                String searchedGroupString = matcher.group(group);
                if (searchedGroupString == null) {
                    return null;
                }
                SliceOutput outputStringSlice = new DynamicSliceOutput(searchedGroupString.length());
                outputStringSlice.writeBytes(searchedGroupString.getBytes(), 0, searchedGroupString.length());
                return outputStringSlice.slice();
            }
        }
        else if (selectedRegexLibrary == JONI) {
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
        if (selectedRegexLibrary == RE2J) {
            String inputString =  source.toStringUtf8();
            com.google.re2j.Matcher matcher = re2jPattern.matcher(inputString);
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
        else if (selectedRegexLibrary == JONI) {
            io.airlift.joni.Matcher matcher =  joniRegex.matcher(source.getBytes());
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
}
