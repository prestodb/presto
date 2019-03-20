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
package com.facebook.presto.operator;

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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkForLoop
{
    private static final int RUNS = 5000;

    @Benchmark
    public void forLoop(BenchmarkData data, Blackhole blackhole)
    {
        long sum = 0;
        for (int run = 0; run < RUNS; run++) {
            long[] values = data.getLongValues();
            for (long value : values) {
                sum += value;
            }
        }
        blackhole.consume(sum);
    }

    @Benchmark
    public void forLoopWithIndex(BenchmarkData data, Blackhole blackhole)
    {
        long sum = 0;
        for (int run = 0; run < RUNS; run++) {
            long[] values = data.getLongValues();
            for (int i = 0; i < values.length; i++) {
                sum += values[i];
            }
        }
        blackhole.consume(sum);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private long[] longValues;

        //@Param({"BIGINT"})
        private String type = "BIGINT";

        @Setup
        public void setup()
        {
            createBigintValues();
        }

        public long[] getLongValues()
        {
            return longValues;
        }

        private void createBigintValues()
        {
            longValues = new long[1024 * 1024];
            for (int i = 0; i < longValues.length; i++) {
                longValues[i] = i;
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        Blackhole blackhole = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
        new BenchmarkForLoop().forLoop(data, blackhole);
        new BenchmarkForLoop().forLoopWithIndex(data, blackhole);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .jvmArgs("-Xmx10g")
                .include(".*" + BenchmarkForLoop.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
