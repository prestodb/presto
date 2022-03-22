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

import com.facebook.presto.orc.TupleDomainFilter.BigintValuesUsingBitmask;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Random;

import static com.facebook.presto.orc.TupleDomainFilter.BigintValuesUsingBitmask.BigintValuesUsingHashTable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBigintValues
{
    @Benchmark
    public int lookupInHashTable(BenchmarkData data)
    {
        int hits = 0;
        for (int i = 0; i < 10_000; i++) {
            if (data.hashtableFilter.testLong(i)) {
                hits++;
            }
        }

        return hits;
    }

    @Benchmark
    public int lookupInBitmask(BenchmarkData data)
    {
        int hits = 0;
        for (int i = 0; i < 10_000; i++) {
            if (data.bitmaskFilter.testLong(i)) {
                hits++;
            }
        }

        return hits;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private long[] values;
        private TupleDomainFilter hashtableFilter;
        private TupleDomainFilter bitmaskFilter;

        @Param({"5000", "1000", "100", "10"})
        private int numValues;

        @Setup
        public void setup()
                throws Exception
        {
            values = new long[numValues];
            Random random = new Random(0);
            for (int i = 0; i < numValues; i++) {
                values[i] = random.nextInt(10_000);
            }

            hashtableFilter = BigintValuesUsingHashTable.of(0, 10_000, values, false);
            bitmaskFilter = BigintValuesUsingBitmask.of(0, 10_000, values, false);
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkBigintValues.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
