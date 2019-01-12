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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.operator.scalar.StringFunctions.leftTrim;
import static io.prestosql.operator.scalar.StringFunctions.length;
import static io.prestosql.operator.scalar.StringFunctions.lower;
import static io.prestosql.operator.scalar.StringFunctions.reverse;
import static io.prestosql.operator.scalar.StringFunctions.rightTrim;
import static io.prestosql.operator.scalar.StringFunctions.substr;
import static io.prestosql.operator.scalar.StringFunctions.trim;
import static io.prestosql.operator.scalar.StringFunctions.upper;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.SURROGATE;
import static java.lang.Character.getType;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 4, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = MILLISECONDS)
public class StringFunctionsBenchmark
{
    @Benchmark
    public long benchmarkLength(BenchmarkData data)
    {
        return length(data.getSlice());
    }

    @Benchmark
    public Slice benchmarkSubstringStart(BenchmarkData data)
    {
        Slice slice = data.getSlice();
        int length = data.getLength();
        return substr(slice, (length / 2) - 1);
    }

    @Benchmark
    public Slice benchmarkSubstringStartLength(BenchmarkData data)
    {
        Slice slice = data.getSlice();
        int length = data.getLength();
        return substr(slice, (length / 2) - 1, length / 2);
    }

    @Benchmark
    public Slice benchmarkSubstringStartFromEnd(BenchmarkData data)
    {
        Slice slice = data.getSlice();
        int length = data.getLength();
        return substr(slice, -((length / 2) + 1));
    }

    @Benchmark
    public Slice benchmarkSubstringStartLengthFromEnd(BenchmarkData data)
    {
        Slice slice = data.getSlice();
        int length = data.getLength();
        return substr(slice, -((length / 2) + 1), length / 2);
    }

    @Benchmark
    public Slice benchmarkReverse(BenchmarkData data)
    {
        return reverse(data.getSlice());
    }

    @Benchmark
    public Slice benchmarkLeftTrim(WhitespaceData data)
    {
        return leftTrim(data.getLeftWhitespace());
    }

    @Benchmark
    public Slice benchmarkRightTrim(WhitespaceData data)
    {
        return rightTrim(data.getRightWhitespace());
    }

    @Benchmark
    public Slice benchmarkTrim(WhitespaceData data)
    {
        return trim(data.getBothWhitespace());
    }

    @Benchmark
    public Slice benchmarkUpper(BenchmarkData data)
    {
        return upper(data.getSlice());
    }

    @Benchmark
    public Slice benchmarkLower(BenchmarkData data)
    {
        return lower(data.getSlice());
    }

    @State(Thread)
    public static class BenchmarkData
    {
        private static final int[] ASCII_CODE_POINTS;
        private static final int[] ALL_CODE_POINTS;

        static {
            ASCII_CODE_POINTS = IntStream.range(0, 0x7F)
                    .toArray();
            ALL_CODE_POINTS = IntStream.range(0, MAX_CODE_POINT)
                    .filter(codePoint -> getType(codePoint) != SURROGATE)
                    .toArray();
        }

        @Param({"2", "5", "10", "100", "1000", "10000"})
        private int length;

        @Param({"true", "false"})
        private boolean ascii;

        private Slice slice;
        private int[] codePoints;

        @Setup
        public void setup()
        {
            int[] codePointSet = ascii ? ASCII_CODE_POINTS : ALL_CODE_POINTS;
            ThreadLocalRandom random = ThreadLocalRandom.current();

            codePoints = new int[length];
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(length * 4);
            for (int i = 0; i < codePoints.length; i++) {
                int codePoint = codePointSet[random.nextInt(codePointSet.length)];
                codePoints[i] = codePoint;
                sliceOutput.appendBytes(new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8));
            }
            slice = sliceOutput.slice();
        }

        public Slice getSlice()
        {
            return slice;
        }

        public int getLength()
        {
            return length;
        }
    }

    @State(Thread)
    public static class WhitespaceData
    {
        private static final int[] ASCII_WHITESPACE;
        private static final int[] ALL_WHITESPACE;

        static {
            ASCII_WHITESPACE = IntStream.range(0, 0x7F)
                    .filter(Character::isWhitespace)
                    .toArray();
            ALL_WHITESPACE = IntStream.range(0, MAX_CODE_POINT)
                    .filter(Character::isWhitespace)
                    .toArray();
        }

        @Param({"2", "5", "10", "100", "1000", "10000"})
        private int length;

        @Param({"true", "false"})
        private boolean ascii;

        private Slice leftWhitespace;
        private Slice rightWhitespace;
        private Slice bothWhitespace;

        @Setup
        public void setup()
        {
            Slice whitespace = createRandomUtf8Slice(ascii ? ASCII_WHITESPACE : ALL_WHITESPACE, length + 1);
            leftWhitespace = Slices.copyOf(whitespace);
            leftWhitespace.setByte(leftWhitespace.length() - 1, 'X');
            rightWhitespace = Slices.copyOf(whitespace);
            rightWhitespace.setByte(0, 'X');
            bothWhitespace = Slices.copyOf(whitespace);
            bothWhitespace.setByte(length / 2, 'X');
        }

        private static Slice createRandomUtf8Slice(int[] codePointSet, int length)
        {
            int[] codePoints = new int[length];
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < codePoints.length; i++) {
                int codePoint = codePointSet[random.nextInt(codePointSet.length)];
                codePoints[i] = codePoint;
            }
            return utf8Slice(new String(codePoints, 0, codePoints.length));
        }

        public int getLength()
        {
            return length;
        }

        public Slice getLeftWhitespace()
        {
            return leftWhitespace;
        }

        public Slice getRightWhitespace()
        {
            return rightWhitespace;
        }

        public Slice getBothWhitespace()
        {
            return bothWhitespace;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + StringFunctionsBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
