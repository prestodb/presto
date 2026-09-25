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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
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

import static com.facebook.presto.SystemSessionProperties.PARALLELIZE_CHAINED_AGGREGATION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Measures end-to-end execution latency of chained aggregations where the outer
 * grouping keys are a strict subset of the inner grouping keys. The rule inserts
 * a local round-robin exchange above the inner aggregation so the outer PARTIAL
 * fans out across all local drivers instead of inheriting the inner aggregation's
 * parallelism.
 * <p>
 * The {@code parallelizeChainedAggregationEnabled} param runs the same queries
 * with the rule on and off so the speedup can be read directly from the report.
 */
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkParallelizeChainedAggregation
{
    @Benchmark
    public MaterializedResult benchmarkSingleAggregation(BenchmarkInfo info)
    {
        // Inner groups by (linestatus, returnflag, shipmode); outer groups by (linestatus, returnflag).
        // {linestatus, returnflag} is a strict subset of the inner keys, so the rule fires.
        return info.getQueryRunner().execute(
                "SELECT sum(s) FROM (" +
                        "  SELECT sum(extendedprice) AS s, linestatus, returnflag, shipmode " +
                        "  FROM lineitem " +
                        "  GROUP BY linestatus, returnflag, shipmode" +
                        ") GROUP BY linestatus, returnflag");
    }

    @Benchmark
    public MaterializedResult benchmarkMultipleAggregations(BenchmarkInfo info)
    {
        return info.getQueryRunner().execute(
                "SELECT sum(s), max(mx), min(mn) FROM (" +
                        "  SELECT sum(extendedprice) AS s, max(extendedprice) AS mx, min(extendedprice) AS mn, " +
                        "         linestatus, returnflag, shipmode " +
                        "  FROM lineitem " +
                        "  GROUP BY linestatus, returnflag, shipmode" +
                        ") GROUP BY linestatus, returnflag");
    }

    @Benchmark
    public MaterializedResult benchmarkHighCardinalityInner(BenchmarkInfo info)
    {
        // Inner produces many rows (groups by orderkey + a column), outer collapses
        // back to coarse grain — the case where avoiding materialization helps most.
        return info.getQueryRunner().execute(
                "SELECT count(c) FROM (" +
                        "  SELECT count(*) AS c, orderkey, linestatus " +
                        "  FROM lineitem " +
                        "  GROUP BY orderkey, linestatus" +
                        ") GROUP BY linestatus");
    }

    @State(Thread)
    public static class BenchmarkInfo
    {
        @Param({"true", "false"})
        private String parallelizeChainedAggregationEnabled;

        private LocalQueryRunner queryRunner;

        @Setup
        public void setup()
        {
            Session session = testSessionBuilder()
                    .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, parallelizeChainedAggregationEnabled)
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build();
            queryRunner = new LocalQueryRunner(session);
            queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        }

        public QueryRunner getQueryRunner()
        {
            return queryRunner;
        }

        @TearDown
        public void tearDown()
        {
            queryRunner.close();
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkParallelizeChainedAggregation.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
