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
package io.prestosql.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.getInteger;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCPUCounters
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private static final int ITERATIONS = 1000;

    @Benchmark
    @OperationsPerInvocation(ITERATIONS)
    public void nanoTime(Blackhole blackhole)
    {
        for (int i = 0; i < ITERATIONS; i++) {
            blackhole.consume(System.nanoTime());
        }
    }

    @Benchmark
    @OperationsPerInvocation(ITERATIONS)
    public void cpuTime(Blackhole blackhole)
    {
        for (int i = 0; i < ITERATIONS; i++) {
            blackhole.consume(currentThreadCpuTime());
        }
    }

    @Benchmark
    @OperationsPerInvocation(ITERATIONS)
    public void userTime(Blackhole blackhole)
    {
        for (int i = 0; i < ITERATIONS; i++) {
            blackhole.consume(currentThreadUserTime());
        }
    }

    private static long currentThreadCpuTime()
    {
        return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }

    private static long currentThreadUserTime()
    {
        return THREAD_MX_BEAN.getCurrentThreadUserTime();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .threads(getInteger("threads", 1))
                .include(".*" + BenchmarkCPUCounters.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
