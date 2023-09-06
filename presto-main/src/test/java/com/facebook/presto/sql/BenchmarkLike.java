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

//package com.facebook.presto.sql;
//
//import com.facebook.presto.likematcher.LikeMatcher;
//import io.airlift.jcodings.specific.NonStrictUTF8Encoding;
//import io.airlift.joni.Matcher;
//import io.airlift.joni.Option;
//import io.airlift.joni.Regex;
//import io.airlift.joni.Syntax;
//import io.airlift.slice.Slice;
//import io.airlift.slice.Slices;
//import io.trino.likematcher.LikeMatcher;
//import io.trino.type.JoniRegexp;
//import org.openjdk.jmh.annotations.Benchmark;
//import org.openjdk.jmh.annotations.BenchmarkMode;
//import org.openjdk.jmh.annotations.Fork;
//import org.openjdk.jmh.annotations.Measurement;
//import org.openjdk.jmh.annotations.OutputTimeUnit;
//import org.openjdk.jmh.annotations.Param;
//import org.openjdk.jmh.annotations.Setup;
//import org.openjdk.jmh.annotations.State;
//import org.openjdk.jmh.annotations.Warmup;
//import org.openjdk.jmh.results.format.ResultFormatType;
//import org.openjdk.jmh.runner.Runner;
//import org.openjdk.jmh.runner.RunnerException;
//import org.openjdk.jmh.runner.options.Options;
//import org.openjdk.jmh.runner.options.OptionsBuilder;
//import org.openjdk.jmh.runner.options.VerboseMode;
//
//import java.util.Optional;
//
//import static io.airlift.joni.constants.MetaChar.INEFFECTIVE_META_CHAR;
//import static io.airlift.joni.constants.SyntaxProperties.OP_ASTERISK_ZERO_INF;
//import static io.airlift.joni.constants.SyntaxProperties.OP_DOT_ANYCHAR;
//import static io.airlift.joni.constants.SyntaxProperties.OP_LINE_ANCHOR;
//import static java.nio.charset.StandardCharsets.UTF_8;
//import static java.util.concurrent.TimeUnit.MILLISECONDS;
//import static java.util.concurrent.TimeUnit.NANOSECONDS;
//import static org.openjdk.jmh.annotations.Mode.AverageTime;
//import static org.openjdk.jmh.annotations.Scope.Thread;
//
//@State(Thread)
//@OutputTimeUnit(NANOSECONDS)
//@BenchmarkMode(AverageTime)
//@Fork(3)
//@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
//@Measurement(iterations = 30, time = 500, timeUnit = MILLISECONDS)
//public class BenchmarkLike
//{
//    private static final Syntax SYNTAX = new Syntax(
//            OP_DOT_ANYCHAR | OP_ASTERISK_ZERO_INF | OP_LINE_ANCHOR,
//            0,
//            0,
//            Option.NONE,
//            new Syntax.MetaCharTable(
//                    '\\',                           /* esc */
//                    INEFFECTIVE_META_CHAR,          /* anychar '.' */
//                    INEFFECTIVE_META_CHAR,          /* anytime '*' */
//                    INEFFECTIVE_META_CHAR,          /* zero or one time '?' */
//                    INEFFECTIVE_META_CHAR,          /* one or more time '+' */
//                    INEFFECTIVE_META_CHAR));        /* anychar anytime */
//
//    @State(Thread)
//    public static class Data
//    {
//        @Param({
//                "%",
//                "_%",
//                "%_",
//                "abc%",
//                "%abc",
//                "_____",
//                "abc%def%ghi",
//                "%abc%def%",
//        })
//        private String pattern;
//
//        private Slice data;
//        private byte[] bytes;
//        private Regex regex;
//        private LikeMatcher matcher;
//
//        @Setup
//        public void setup()
//        {
//            data = Slices.utf8Slice(
//                    switch (pattern) {
//                        case "%" -> "qeroighqeorhgqerhb2eriuyerqiubgierubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhet";
//                        case "_%", "%_" -> "qeroighqeorhgqerhb2eriuyerqiubgierubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhet";
//                        case "abc%" -> "abcqeroighqeorhgqerhb2eriuyerqiubgierubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhet";
//                        case "%abc" -> "qeroighqeorhgqerhb2eriuyerqiubgierubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhetabc";
//                        case "_____" -> "abcde";
//                        case "abc%def%ghi" -> "abc qeroighqeorhgqerhb2eriuyerqiubgier def ubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhet ghi";
//                        case "%abc%def%" -> "fdnbqerbfklerqbgqjerbgkr abc qeroighqeorhgqerhb2eriuyerqiubgier def ubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhet";
//                        default -> throw new IllegalArgumentException("Unknown pattern: " + pattern);
//                    });
//
//            matcher = LikeMatcher.compile(pattern, Optional.empty());
//            joniPattern = compileJoni(Slices.utf8Slice(pattern).toStringUtf8(), '0', false);
//
//            bytes = data.getBytes();
//        }
//    }
//
//    @Benchmark
//    public boolean benchmarkJoni(Data data)
//    {
//        return likeVarchar(data.data, data.joniPattern);
//    }
//
//    @Benchmark
//    public boolean benchmarkCurrent(Data data)
//    {
//        return data.matcher.match(data.bytes, 0, data.bytes.length);
//    }
//
//    public static boolean likeVarchar(Slice value, JoniRegexp pattern)
//    {
//        Matcher matcher;
//        int offset;
//        if (value.hasByteArray()) {
//            offset = value.byteArrayOffset();
//            matcher = pattern.regex().matcher(value.byteArray(), offset, offset + value.length());
//        }
//        else {
//            offset = 0;
//            matcher = pattern.matcher(value.getBytes());
//        }
//        return matcher.match(offset, offset + value.length(), Option.NONE) != -1;
//    }
//
//    private static JoniRegexp compileJoni(String patternString, char escapeChar, boolean shouldEscape)
//    {
//        byte[] bytes = likeToRegex(patternString, escapeChar, shouldEscape).getBytes(UTF_8);
//        Regex joniRegex = new Regex(bytes, 0, bytes.length, Option.MULTILINE, NonStrictUTF8Encoding.INSTANCE, SYNTAX);
//        return new JoniRegexp(Slices.wrappedBuffer(bytes), joniRegex);
//    }
//
//    private static String likeToRegex(String patternString, char escapeChar, boolean shouldEscape)
//    {
//        StringBuilder regex = new StringBuilder(patternString.length() * 2);
//
//        regex.append('^');
//        boolean escaped = false;
//        for (char currentChar : patternString.toCharArray()) {
//            checkEscape(!escaped || currentChar == '%' || currentChar == '_' || currentChar == escapeChar);
//            if (shouldEscape && !escaped && (currentChar == escapeChar)) {
//                escaped = true;
//            }
//            else {
//                switch (currentChar) {
//                    case '%' -> {
//                        regex.append(escaped ? "%" : ".*");
//                        escaped = false;
//                    }
//                    case '_' -> {
//                        regex.append(escaped ? "_" : ".");
//                        escaped = false;
//                    }
//                    default -> {
//                        // escape special regex characters
//                        switch (currentChar) {
//                            case '\\', '^', '$', '.', '*' -> regex.append('\\');
//                        }
//                        regex.append(currentChar);
//                        escaped = false;
//                    }
//                }
//            }
//        }
//        checkEscape(!escaped);
//        regex.append('$');
//        return regex.toString();
//    }
//
//    private static void checkEscape(boolean condition)
//    {
//        checkCondition(condition, INVALID_FUNCTION_ARGUMENT, "Escape character must be followed by '%%', '_' or the escape character itself");
//    }
//
//    public static void main(String[] args)
//            throws RunnerException
//    {
//        Options options = new OptionsBuilder()
//                .verbosity(VerboseMode.NORMAL)
//                .include(".*" + BenchmarkLike.class.getSimpleName() + ".*")
//                .resultFormat(ResultFormatType.JSON)
//                .build();
//
//        new Runner(options).run();
//    }
//}
