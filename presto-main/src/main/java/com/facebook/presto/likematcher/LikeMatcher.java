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
package com.facebook.presto.likematcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static java.nio.charset.StandardCharsets.UTF_8;

public class LikeMatcher
{
    private final String pattern;
    private final Optional<Character> escape;

    private final int minSize;
    private final OptionalInt maxSize;
    private final byte[] prefix;
    private final byte[] suffix;
    private final Optional<Matcher> matcher;

    private LikeMatcher(
            String pattern,
            Optional<Character> escape,
            int minSize,
            OptionalInt maxSize,
            byte[] prefix,
            byte[] suffix,
            Optional<Matcher> matcher)
    {
        this.pattern = pattern;
        this.escape = escape;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.prefix = prefix;
        this.suffix = suffix;
        this.matcher = matcher;
    }

    public String getPattern()
    {
        return pattern;
    }

    public Optional<Character> getEscape()
    {
        return escape;
    }

    public static LikeMatcher compile(String pattern)
    {
        return compile(pattern, Optional.empty(), true);
    }

    public static LikeMatcher compile(String pattern, Optional<Character> escape)
    {
        return compile(pattern, escape, true);
    }

    public static LikeMatcher compile(String pattern, Optional<Character> escape, boolean optimize)
    {
        List<Pattern> parsed = parse(pattern, escape);

        // Calculate minimum and maximum size for candidate strings
        // This is used for short-circuiting the match if the size of
        // the input is outside those bounds
        int minSize = 0;
        int maxSize = 0;
        boolean unbounded = false;
        for (Pattern expression : parsed) {
            if (expression instanceof Pattern.Literal) {
                Pattern.Literal literal = (Pattern.Literal) expression;
                int length = literal.getValue().getBytes(UTF_8).length;
                minSize += length;
                maxSize += length;
            }
            else if (expression instanceof Pattern.ZeroOrMore) {
                unbounded = true;
            }
            else if (expression instanceof Pattern.Any) {
                Pattern.Any any = (Pattern.Any) expression;
                int length = any.getLength();
                minSize += length;
                maxSize += length * 4; // at most 4 bytes for a single UTF-8 codepoint
            }
            else {
                throw new UnsupportedOperationException("Not supported: " + expression.getClass().getName());
            }
        }

        // Calculate exact match prefix and suffix
        // If the pattern starts and ends with a literal, we can perform a quick
        // exact match to short-circuit DFA evaluation
        byte[] prefix = new byte[0];
        byte[] suffix = new byte[0];

        int patternStart = 0;
        int patternEnd = parsed.size() - 1;
        if (parsed.size() > 0 && parsed.get(0) instanceof Pattern.Literal) {
            Pattern.Literal literal = (Pattern.Literal) parsed.get(0);
            prefix = literal.getValue().getBytes(UTF_8);
            patternStart++;
        }

        if (parsed.size() > 1 && parsed.get(parsed.size() - 1) instanceof Pattern.Literal) {
            Pattern.Literal literal = (Pattern.Literal) parsed.get(parsed.size() - 1);
            suffix = literal.getValue().getBytes(UTF_8);
            patternEnd--;
        }

        // If the pattern (after excluding constant prefix/suffixes) ends with an unbounded match (i.e., %)
        // we can perform a non-exact match and end as soon as the DFA reaches an accept state -- there
        // is no need to consume the remaining input
        // This section determines whether the pattern is a candidate for non-exact match.
        boolean exact = true; // whether to match to the end of the input
        if (patternStart <= patternEnd && parsed.get(patternEnd) instanceof Pattern.ZeroOrMore) {
            // guaranteed to be Any or ZeroOrMore because any Literal would've been turned into a suffix above
            exact = false;
            patternEnd--;
        }

        Optional<Matcher> matcher = Optional.empty();
        if (patternStart <= patternEnd) {
            if (optimize) {
                matcher = Optional.of(new DenseDfaMatcher(parsed, patternStart, patternEnd, exact));
            }
            else {
                matcher = Optional.of(new NfaMatcher(parsed, patternStart, patternEnd, exact));
            }
        }

        return new LikeMatcher(
                pattern,
                escape,
                minSize,
                unbounded ? OptionalInt.empty() : OptionalInt.of(maxSize),
                prefix,
                suffix,
                matcher);
    }

    public boolean match(byte[] input)
    {
        return match(input, 0, input.length);
    }

    public boolean match(byte[] input, int offset, int length)
    {
        if (length < minSize) {
            return false;
        }

        if (maxSize.isPresent() && length > maxSize.getAsInt()) {
            return false;
        }

        if (!startsWith(prefix, input, offset)) {
            return false;
        }

        if (!startsWith(suffix, input, offset + length - suffix.length)) {
            return false;
        }

        if (matcher.isPresent()) {
            return matcher.get().match(input, offset + prefix.length, length - suffix.length - prefix.length);
        }

        return true;
    }

    private boolean startsWith(byte[] pattern, byte[] input, int offset)
    {
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != input[offset + i]) {
                return false;
            }
        }

        return true;
    }

    static List<Pattern> parse(String pattern, Optional<Character> escape)
    {
        List<Pattern> result = new ArrayList<>();

        StringBuilder literal = new StringBuilder();
        int anyCount = 0;
        boolean anyUnbounded = false;
        boolean inEscape = false;
        for (int i = 0; i < pattern.length(); i++) {
            char character = pattern.charAt(i);

            if (inEscape) {
                if (character != '%' && character != '_' && character != escape.get()) {
                    throw new IllegalArgumentException("Escape character must be followed by '%', '_' or the escape character itself");
                }

                literal.append(character);
                inEscape = false;
            }
            else if (escape.isPresent() && character == escape.get()) {
                inEscape = true;

                if (anyCount != 0) {
                    result.add(new Pattern.Any(anyCount));
                    anyCount = 0;
                }

                if (anyUnbounded) {
                    result.add(new Pattern.ZeroOrMore());
                    anyUnbounded = false;
                }
            }
            else if (character == '%' || character == '_') {
                if (literal.length() != 0) {
                    result.add(new Pattern.Literal(literal.toString()));
                    literal.setLength(0);
                }

                if (character == '%') {
                    anyUnbounded = true;
                }
                else {
                    anyCount++;
                }
            }
            else {
                if (anyCount != 0) {
                    result.add(new Pattern.Any(anyCount));
                    anyCount = 0;
                }

                if (anyUnbounded) {
                    result.add(new Pattern.ZeroOrMore());
                    anyUnbounded = false;
                }

                literal.append(character);
            }
        }

        if (inEscape) {
            throw new IllegalArgumentException("Escape character must be followed by '%', '_' or the escape character itself");
        }

        if (literal.length() != 0) {
            result.add(new Pattern.Literal(literal.toString()));
        }
        else {
            if (anyCount != 0) {
                result.add(new Pattern.Any(anyCount));
            }

            if (anyUnbounded) {
                result.add(new Pattern.ZeroOrMore());
            }
        }

        return result;
    }
}
