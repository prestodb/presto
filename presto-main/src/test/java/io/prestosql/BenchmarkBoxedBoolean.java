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
package io.prestosql;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Level.Iteration;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 3)
@Warmup(iterations = 10, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
public class BenchmarkBoxedBoolean
{
    private static final int ARRAY_SIZE = 100;

    @Benchmark
    public void primitive(BenchmarkData data, Blackhole blackhole)
    {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            if (data.primitives[i]) {
                blackhole.consume(0xDEADBEAF);
            }
            else {
                blackhole.consume(0xBEAFDEAD);
            }
        }
    }

    @Benchmark
    public void unboxing(BenchmarkData data, Blackhole blackhole)
    {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            if (data.boxed[i]) {
                blackhole.consume(0xDEADBEAF);
            }
            else {
                blackhole.consume(0xBEAFDEAD);
            }
        }
    }

    @Benchmark
    public void identity(BenchmarkData data, Blackhole blackhole)
    {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            if (TRUE == data.constants[i]) {
                blackhole.consume(0xDEADBEAF);
            }
            else {
                blackhole.consume(0xBEAFDEAD);
            }
        }
    }

    @Benchmark
    public void booleanEquals(BenchmarkData data, Blackhole blackhole)
    {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            if (TRUE.equals(data.constants[i])) {
                blackhole.consume(0xDEADBEAF);
            }
            else {
                blackhole.consume(0xBEAFDEAD);
            }
        }
    }

    @Benchmark
    public void object(BenchmarkData data, Blackhole blackhole)
    {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            if (TRUE.equals(data.objects[i])) {
                blackhole.consume(0xDEADBEAF);
            }
            else {
                blackhole.consume(0xBEAFDEAD);
            }
        }
    }

    @Benchmark
    public void booleanEqualsNotNull(BenchmarkData data, Blackhole blackhole)
    {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            if (TRUE.equals(data.boxed[i])) {
                blackhole.consume(0xDEADBEAF);
            }
            else {
                blackhole.consume(0xBEAFDEAD);
            }
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        public boolean[] primitives = new boolean[ARRAY_SIZE];
        public Boolean[] boxed = new Boolean[ARRAY_SIZE];
        public Boolean[] constants = new Boolean[ARRAY_SIZE];
        public Boolean[] objects = new Boolean[ARRAY_SIZE];

        @Setup(Iteration)
        public void setup()
        {
            for (int i = 0; i < ARRAY_SIZE; i++) {
                boolean value = ThreadLocalRandom.current().nextBoolean();
                boolean isNull = ThreadLocalRandom.current().nextBoolean();

                primitives[i] = value;
                boxed[i] = value;

                constants[i] = isNull ? null : value;
                objects[i] = isNull ? null : new Boolean(value);
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkBoxedBoolean.class.getSimpleName() + ".*")
                .addProfiler("perfasm")
                .build();
        new Runner(options).run();
    }
}
