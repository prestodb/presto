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

import io.airlift.joni.Regex;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
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

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.scalar.RegexpFunctions.regexpLike;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class RegexpFunctionsBenchmark
{
    @Benchmark
    public boolean benchmarkLike(DotStarAroundData data)
    {
        return regexpLike(data.getSource(), data.getPattern());
    }

    @State(Thread)
    public static class DotStarAroundData
    {
        @Param({ ".*x.*", ".*(x|y).*", "longdotstar", "phone", "literal" })
        private String patternString;

        @Param({ "1024", "32768" })
        private int sourceLength;

        private Regex pattern;
        private Slice source;

        @Setup
        public void setup()
        {
            SliceOutput sliceOutput = new DynamicSliceOutput(sourceLength);
            switch (patternString) {
                case ".*x.*":
                    pattern = RegexpFunctions.castToRegexp(Slices.utf8Slice(".*x.*"));
                    IntStream.generate(() -> 97).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
                case ".*(x|y).*":
                    pattern = RegexpFunctions.castToRegexp(Slices.utf8Slice(".*(x|y).*"));
                    IntStream.generate(() -> 97).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
                case "longdotstar":
                    pattern = RegexpFunctions.castToRegexp(Slices.utf8Slice(".*coolfunctionname.*"));
                    ThreadLocalRandom.current().ints(97, 123).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
                case "phone":
                    pattern = RegexpFunctions.castToRegexp(Slices.utf8Slice("\\d{3}/\\d{3}/\\d{4}"));
                    // 47: '/', 48-57: '0'-'9'
                    ThreadLocalRandom.current().ints(47, 58).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
                case "literal":
                    pattern = RegexpFunctions.castToRegexp(Slices.utf8Slice("literal"));
                    // 97-122: 'a'-'z'
                    ThreadLocalRandom.current().ints(97, 123).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
            }
            source = sliceOutput.slice();
            checkState(source.length() == sourceLength, "source.length=%s, sourceLength=%s", source.length(), sourceLength);
        }

        public Slice getSource()
        {
            return source;
        }

        public Regex getPattern()
        {
            return pattern;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + RegexpFunctionsBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
