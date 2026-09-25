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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchPlugin;
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
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Distributed end-to-end JMH benchmark for ParallelizeChainedAggregation.
 * <p>
 * Runs the same chained-aggregation queries against a multi-worker
 * {@link DistributedQueryRunner} with the optimization on and off so the
 * speedup from inserting a local round-robin above the inner aggregation can
 * be read directly from the report. The distributed runner exercises real
 * REMOTE_STREAMING exchanges between stages and provides multiple local
 * drivers, which is where the extra fan-out the rule introduces matters most.
 */
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class BenchmarkParallelizeChainedAggregationDistributed
{
    @Benchmark
    public MaterializedResult benchmarkSingleAggregation(BenchmarkInfo info)
    {
        // Inner groups by (linestatus, returnflag, shipmode); outer groups by (linestatus, returnflag).
        return info.queryRunner.execute(
                "SELECT sum(s) FROM (" +
                        "  SELECT sum(extendedprice) AS s, linestatus, returnflag, shipmode " +
                        "  FROM lineitem " +
                        "  GROUP BY linestatus, returnflag, shipmode" +
                        ") GROUP BY linestatus, returnflag");
    }

    @Benchmark
    public MaterializedResult benchmarkMultipleAggregations(BenchmarkInfo info)
    {
        return info.queryRunner.execute(
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
        // Inner aggregation produces many rows (per orderkey), outer collapses to coarse grain.
        // High inner-key cardinality means the inner FINAL is already parallel — the local
        // round-robin should add little here, providing a noise floor for the other cases.
        return info.queryRunner.execute(
                "SELECT count(c) FROM (" +
                        "  SELECT count(*) AS c, orderkey, linestatus " +
                        "  FROM lineitem " +
                        "  GROUP BY orderkey, linestatus" +
                        ") GROUP BY linestatus");
    }

    @Benchmark
    public MaterializedResult benchmarkPercentileOutliers(BenchmarkInfo info)
    {
        // Realistic outlier-detection shape: per-key sums in the inner aggregation,
        // approx_percentile + sum in the outer to surface tail behavior per partition.
        // approx_percentile is CPU-heavy on the outer PARTIAL, so this is where
        // parallelizing the outer PARTIAL across more local drivers should pay off most.
        return info.queryRunner.execute(
                "SELECT approx_percentile(s, 0.99) AS p99, " +
                        "       approx_percentile(s, 0.5)  AS p50, " +
                        "       sum(s) AS total, " +
                        "       linestatus " +
                        "FROM (" +
                        "  SELECT sum(extendedprice) AS s, orderkey, linestatus " +
                        "  FROM lineitem " +
                        "  GROUP BY orderkey, linestatus" +
                        ") GROUP BY linestatus");
    }

    @Benchmark
    public MaterializedResult benchmarkGlobalOuterAggregation(BenchmarkInfo info)
    {
        // Global outer aggregation (no GROUP BY). Outer PARTIAL has no key to partition by,
        // so it otherwise runs on whatever drivers the inner FINAL has — local round-robin
        // can fan it out across all local drivers, which matters most for CPU-heavy outer
        // aggregations like approx_percentile.
        return info.queryRunner.execute(
                "SELECT approx_percentile(s, 0.5) FROM (" +
                        "  SELECT sum(extendedprice) AS s, linestatus, returnflag, shipmode " +
                        "  FROM lineitem " +
                        "  GROUP BY linestatus, returnflag, shipmode" +
                        ")");
    }

    @State(Thread)
    public static class BenchmarkInfo
    {
        @Param({"true", "false"})
        private String parallelizeChainedAggregationEnabled;

        private DistributedQueryRunner queryRunner;

        @Setup
        public void setup()
                throws Exception
        {
            Session session = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema(TINY_SCHEMA_NAME)
                    .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, parallelizeChainedAggregationEnabled)
                    .build();
            queryRunner = DistributedQueryRunner.builder(session)
                    .setNodeCount(4)
                    .build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");
            }
            catch (Exception e) {
                queryRunner.close();
                throw e;
            }
        }

        @TearDown
        public void tearDown()
        {
            queryRunner.close();
            queryRunner = null;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkParallelizeChainedAggregationDistributed.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
