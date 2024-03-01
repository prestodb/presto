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
package com.facebook.presto.benchmark;

import com.facebook.presto.common.Page;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.List;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkDecimalAggregation
{
    @State(Thread)
    public static class AggregationContext
    {
        @Param({"orderstatus, avg(totalprice)",
                "orderstatus, min(totalprice)",
                "orderstatus, sum(totalprice), avg(totalprice), min(totalprice), max(totalprice)"})
        private String project;

        @Param({"double", "decimal(14,2)", "decimal(30,10)"})
        private String type;

        private MemoryLocalQueryRunner queryRunner;

        public final MemoryLocalQueryRunner getQueryRunner()
        {
            return queryRunner;
        }

        @Setup
        public void setUp()
        {
            queryRunner = new MemoryLocalQueryRunner();
            queryRunner.execute(format(
                    "CREATE TABLE memory.default.orders AS SELECT orderstatus, cast(totalprice as %s) totalprice FROM tpch.sf1.orders",
                    type));
        }

        @TearDown
        public void tearDown()
        {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Benchmark
    public List<Page> benchmarkBuildHash(AggregationContext context)
    {
        return context.getQueryRunner()
                .execute(String.format("SELECT %s FROM orders GROUP BY orderstatus", context.project));
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkDecimalAggregation.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
