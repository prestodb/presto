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
package com.facebook.presto.orc;

import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@State(Scope.Thread)
@OutputTimeUnit(NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBytesValues
{
    // NOT IN ("apple", "grape", "orange")
    // "abc", "apple", "banana", "grape", "orange", "peach"
    private final byte[][] testWords = new byte[][] {"abc".getBytes(), "apple".getBytes(), "banana".getBytes(), "grape".getBytes(), "orange".getBytes(), "peach".getBytes()};

    @Benchmark
    public int lookupInExclusive(BenchmarkBytesValues.BenchmarkData data)
    {
        int hits = 0;
        for (int i = 0; i < testWords.length; i++) {
            if (data.exclusiveFilter.testBytes(testWords[i], 0, testWords[i].length)) {
                hits++;
            }
        }

        return hits;
    }

    @Benchmark
    public int lookupInMultiRange(BenchmarkBytesValues.BenchmarkData data)
    {
        int hits = 0;
        for (int i = 0; i < testWords.length; i++) {
            if (data.multiRangeFilter.testBytes(testWords[i], 0, testWords[i].length)) {
                hits++;
            }
        }

        return hits;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private TupleDomainFilter exclusiveFilter;
        private TupleDomainFilter multiRangeFilter;

        @Setup
        public void setup()
                throws Exception
        {
            exclusiveFilter = TupleDomainFilter.BytesValuesExclusive.of(new byte[][] {"apple".getBytes(), "grape".getBytes(), "orange".getBytes()}, false);
            multiRangeFilter = TupleDomainFilter.MultiRange.of(
                    ImmutableList.of(
                            TupleDomainFilter.BytesRange.of(null, true, "apple".getBytes(), true, false),
                            TupleDomainFilter.BytesRange.of("apple".getBytes(), true, "grape".getBytes(), true, false),
                            TupleDomainFilter.BytesRange.of("grape".getBytes(), true, "orange".getBytes(), true, false),
                            TupleDomainFilter.BytesRange.of("orange".getBytes(), true, null, true, false)),
                    false,
                    false);
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkBytesValues.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
