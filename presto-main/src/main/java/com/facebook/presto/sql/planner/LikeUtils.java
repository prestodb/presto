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
package com.facebook.presto.sql.planner;

import com.facebook.presto.util.ThreadLocalCache;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Syntax;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.google.common.base.Charsets.UTF_8;
import static org.joni.constants.MetaChar.INEFFECTIVE_META_CHAR;

public final class LikeUtils
{
    private final static Syntax SYNTAX = new Syntax(
            Syntax.OP_DOT_ANYCHAR | Syntax.OP_ASTERISK_ZERO_INF | Syntax.OP_LINE_ANCHOR,
            0,
            0,
            Option.NONE,
            new Syntax.MetaCharTable(
                    '\\',                           /* esc */
                    INEFFECTIVE_META_CHAR,          /* anychar '.' */
                    INEFFECTIVE_META_CHAR,          /* anytime '*' */
                    INEFFECTIVE_META_CHAR,          /* zero or one time '?' */
                    INEFFECTIVE_META_CHAR,          /* one or more time '+' */
                    INEFFECTIVE_META_CHAR           /* anychar anytime */
            )
    );

    private LikeUtils()
    {
    }

    public static boolean dynamicLike(LikePatternCache callSiteCache, Slice value, Slice pattern, Slice escape)
    {
        LikeCacheKey key = new LikeCacheKey(pattern, escape);
        Regex regex = callSiteCache.get(key);
        return regexMatches(regex, value);
    }

    public static boolean regexMatches(Regex regex, Slice value)
    {
        // Joni doesn't handle invalid UTF-8, so replace invalid characters
        byte[] bytes = value.getBytes();
        if (isAscii(bytes)) {
            return regexMatches(regex, bytes);
        }
        return regexMatches(regex, value.toString(UTF_8).getBytes(UTF_8));
    }

    public static boolean regexMatches(Regex regex, byte[] bytes)
    {
        return regex.matcher(bytes).match(0, bytes.length, Option.NONE) != -1;
    }

    public static char getEscapeChar(Slice escape)
    {
        char escapeChar;
        String escapeString = escape.toString(UTF_8);
        if (escapeString.length() == 0) {
            // escaping disabled
            escapeChar = (char) -1; // invalid character
        }
        else if (escapeString.length() == 1) {
            escapeChar = escapeString.charAt(0);
        }
        else {
            throw new IllegalArgumentException("escape must be empty or a single character: " + escapeString);
        }
        return escapeChar;
    }

    public static boolean isAscii(byte[] bytes)
    {
        boolean high = false;
        for (byte b : bytes) {
            high |= (b & 0x80) != 0;
        }
        return !high;
    }

    public static Regex likeToPattern(Slice pattern, @Nullable Slice escapeSlice)
    {
        String patternString = pattern.toString(UTF_8);
        if (escapeSlice != null) {
            return likeToPattern(patternString, getEscapeChar(escapeSlice));
        }
        else {
            return likeToPattern(patternString);
        }
    }

    public static Regex likeToPattern(String patternString, char escapeChar)
    {
        return likeToPattern(patternString, escapeChar, true);
    }

    public static Regex likeToPattern(String patternString)
    {
        return likeToPattern(patternString, 'x', false);
    }

    private static Regex likeToPattern(String patternString, char escapeChar, boolean shouldEscape)
    {
        StringBuilder regex = new StringBuilder(patternString.length() * 2);

        regex.append('^');
        boolean escaped = false;
        for (char currentChar : patternString.toCharArray()) {
            if (shouldEscape && !escaped && currentChar == escapeChar) {
                escaped = true;
            }
            else {
                switch (currentChar) {
                    case '%':
                        if (escaped) {
                            regex.append("%");
                        }
                        else {
                            regex.append(".*");
                        }
                        escaped = false;
                        break;
                    case '_':
                        if (escaped) {
                            regex.append("_");
                        }
                        else {
                            regex.append('.');
                        }
                        escaped = false;
                        break;
                    default:
                        // escape special regex characters
                        switch (currentChar) {
                            case '\\':
                            case '^':
                            case '$':
                            case '.':
                            case '*':
                                regex.append('\\');
                        }

                        regex.append(currentChar);
                        escaped = false;
                }
            }
        }
        regex.append('$');

        byte[] bytes = regex.toString().getBytes(Charsets.UTF_8);
        return new Regex(bytes, 0, bytes.length, 0, UTF8Encoding.INSTANCE, SYNTAX);
    }

    public static class LikePatternCache
            extends ThreadLocalCache<LikeCacheKey, Regex>
    {
        public LikePatternCache(int maxSizePerThread)
        {
            super(maxSizePerThread);
        }

        @Nonnull
        @Override
        protected Regex load(LikeCacheKey key)
        {
            return likeToPattern(key.pattern, key.escape);
        }
    }

    public static class LikeCacheKey
    {
        private final Slice pattern;
        private final Slice escape;

        public LikeCacheKey(Slice pattern, Slice escape)
        {
            this.pattern = pattern;
            this.escape = escape;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(pattern, escape);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final LikeCacheKey other = (LikeCacheKey) obj;
            return Objects.equal(this.pattern, other.pattern) && Objects.equal(this.escape, other.escape);
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("pattern", pattern.toString(UTF_8))
                    .add("escape", escape.toString(UTF_8))
                    .toString();
        }
    }
}
