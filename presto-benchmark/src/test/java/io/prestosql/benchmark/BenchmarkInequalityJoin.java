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

import com.google.common.collect.ImmutableMap;
import io.prestosql.SystemSessionProperties;
import io.prestosql.spi.Page;
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

/**
 * This benchmark a case when there is almost like a cross join query
 * but with very selective inequality join condition. In other words
 * for each probe position there are lots of matching build positions
 * which are filtered out by filtering function.
 */
@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkInequalityJoin
{
    @State(Thread)
    public static class Context
    {
        private MemoryLocalQueryRunner queryRunner;

        @Param({"true", "false"})
        private String fastInequalityJoins;

        // number of buckets. The smaller number of buckets, the longer position links chain
        @Param({"100", "1000", "10000", "60000"})
        private int buckets;

        // How many positions out of 1000 will be actually joined
        // 10 means 1 - 10/1000 = 99/100 positions will be filtered out
        @Param("10")
        private int filterOutCoefficient;

        public MemoryLocalQueryRunner getQueryRunner()
        {
            return queryRunner;
        }

        @Setup
        public void setUp()
        {
            queryRunner = new MemoryLocalQueryRunner(ImmutableMap.of(SystemSessionProperties.FAST_INEQUALITY_JOINS, fastInequalityJoins));

            // t1.val1 is in range [0, 1000)
            // t1.bucket is in [0, 1000)
            queryRunner.execute(format(
                    "CREATE TABLE memory.default.t1 AS SELECT " +
                            "orderkey %% %d bucket, " +
                            "(orderkey * 13) %% 1000 val1 " +
                            "FROM tpch.tiny.lineitem",
                    buckets));
            // t2.val2 is in range [0, 10)
            // t2.bucket is in [0, 1000)
            queryRunner.execute(format(
                    "CREATE TABLE memory.default.t2 AS SELECT " +
                            "orderkey %% %d bucket, " +
                            "(orderkey * 379) %% %d val2 " +
                            "FROM tpch.tiny.lineitem",
                    buckets,
                    filterOutCoefficient));
        }

        @TearDown
        public void tearDown()
        {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Benchmark
    public List<Page> benchmarkJoin(Context context)
    {
        return context.getQueryRunner()
                .execute("SELECT count(*) FROM t1 JOIN t2 on (t1.bucket = t2.bucket) WHERE t1.val1 < t2.val2");
    }

    @Benchmark
    public List<Page> benchmarkJoinWithArithmeticInPredicate(Context context)
    {
        return context.getQueryRunner()
                .execute("SELECT count(*) FROM t1 JOIN t2 on (t1.bucket = t2.bucket) AND t1.val1 < t2.val2 + 10");
    }

    @Benchmark
    public List<Page> benchmarkJoinWithFunctionPredicate(Context context)
    {
        return context.getQueryRunner()
                .execute("SELECT count(*) FROM t1 JOIN t2 on (t1.bucket = t2.bucket) AND t1.val1 < sin(t2.val2)");
    }

    @Benchmark
    public List<Page> benchmarkRangePredicateJoin(Context context)
    {
        return context.getQueryRunner()
                .execute("SELECT count(*) FROM t1 JOIN t2 on (t1.bucket = t2.bucket) AND t1.val1 + 1 < t2.val2 AND t2.val2 < t1.val1 + 5 ");
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkInequalityJoin.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
