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

import com.facebook.airlift.units.Duration;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDateTimeFunctions
{
    @Param({"ns", "us", "ms", "s", "m", "h", "d"})
    private String unit = "ns";

    private Random random = new Random();

    @Setup
    public void setup()
    {
        random = new Random(42); // Fixed seed for reproducibility
    }

    @Benchmark
    public void testBaseline(Blackhole bh)
    {
        int v1 = random.nextInt(10000);
        int v2 = random.nextInt(10000);
        Slice value = Slices.utf8Slice(v1 + "." + v2 + " " + unit);
        bh.consume(value.toStringUtf8());
    }

    @Benchmark
    public void testUseBigDecimal(Blackhole bh)
    {
        int v1 = random.nextInt(10000);
        int v2 = random.nextInt(10000);
        Slice value = Slices.utf8Slice(v1 + "." + v2 + " " + unit);
        bh.consume(DateTimeFunctions.parseDuration(value));
    }

    @Benchmark
    public void testUseDouble(Blackhole bh)
    {
        int v1 = random.nextInt(10000);
        int v2 = random.nextInt(10000);
        Slice value = Slices.utf8Slice(v1 + "." + v2 + " " + unit);
        bh.consume(Duration.valueOf(value.toStringUtf8()).toMillis());
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(BenchmarkDateTimeFunctions.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
