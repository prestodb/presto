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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.google.re2j.Matcher;
import com.google.re2j.Options;
import com.google.re2j.Pattern;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.primitives.Ints.checkedCast;
import static com.google.re2j.Options.Algorithm.DFA_FALLBACK_TO_NFA;
import static java.lang.String.format;

public final class Re2JRegexp
{
    private static final Logger log = Logger.get(Re2JRegexp.class);

    private static final java.util.regex.Pattern DOT_STAR_PREFIX_SUFFIX_PATTERN = java.util.regex.Pattern.compile("^(\\.\\*\\??)?(.*?)(\\.\\*\\??)?$");
    private static final int CORE_PATTERN_INDEX = 2;

    public final int dfaStatesLimit;
    public final int dfaRetries;

    public final Pattern re2jPattern;
    public final Pattern re2jPatternWithoutDotStartPrefixSuffix;

    public Re2JRegexp(int dfaStatesLimit, int dfaRetries, Slice pattern)
    {
        this.dfaStatesLimit = dfaStatesLimit;
        this.dfaRetries = dfaRetries;

        Options options = Options.builder()
                .setAlgorithm(DFA_FALLBACK_TO_NFA)
                .setMaximumNumberOfDFAStates(dfaStatesLimit)
                .setNumberOfDFARetries(dfaRetries)
                .setEventsListener(new RE2JEventsListener())
                .build();

        String patternString = pattern.toStringUtf8();
        re2jPattern = Pattern.compile(patternString, options);

        // Remove .* prefix and suffix. This will give performance boost when using RE2J Pattern.find() function.
        java.util.regex.Matcher dotStarPrefixSuffixMatcher = DOT_STAR_PREFIX_SUFFIX_PATTERN.matcher(patternString);
        checkState(dotStarPrefixSuffixMatcher.matches());
        String patternStringWithoutDotStartPrefixSuffix = dotStarPrefixSuffixMatcher.group(CORE_PATTERN_INDEX);

        if (!patternStringWithoutDotStartPrefixSuffix.equals(patternString)) {
            re2jPatternWithoutDotStartPrefixSuffix = Pattern.compile(patternStringWithoutDotStartPrefixSuffix, options);
        }
        else {
            re2jPatternWithoutDotStartPrefixSuffix = re2jPattern;
        }
    }

    public boolean matches(Slice source)
    {
        return re2jPatternWithoutDotStartPrefixSuffix.find(source);
    }

    public Slice replace(Slice source, Slice replacement)
    {
        Matcher matcher = re2jPattern.matcher(source);
        try {
            return matcher.replaceAll(replacement);
        }
        catch (IndexOutOfBoundsException | IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacement.toStringUtf8());
        }
    }

    public Block extractAll(Slice source, long groupIndex)
    {
        Matcher matcher = re2jPattern.matcher(source);
        int group = checkedCast(groupIndex);
        validateGroup(group, matcher.groupCount());

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 32);
        while (true) {
            if (!matcher.find()) {
                break;
            }

            Slice searchedGroup = matcher.group(group);
            if (searchedGroup == null) {
                blockBuilder.appendNull();
                continue;
            }
            VARCHAR.writeSlice(blockBuilder, searchedGroup);
        }
        return blockBuilder.build();
    }

    public Slice extract(Slice source, long groupIndex)
    {
        Matcher matcher = re2jPattern.matcher(source);
        int group = checkedCast(groupIndex);
        validateGroup(group, matcher.groupCount());

        if (!matcher.find()) {
            return null;
        }

        return matcher.group(group);
    }

    public Block split(Slice source)
    {
        Matcher matcher = re2jPattern.matcher(source);
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

    private void validateGroup(int group, int groupCount)
    {
        if (group < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Group cannot be negative");
        }
        if (group > groupCount) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Pattern has %d groups. Cannot access group %d", groupCount, group));
        }
    }

    private static class RE2JEventsListener
            implements Options.EventsListener
    {
        @Override
        public void fallbackToNFA()
        {
            log.info("Fallback to NFA");
        }
    }
}
