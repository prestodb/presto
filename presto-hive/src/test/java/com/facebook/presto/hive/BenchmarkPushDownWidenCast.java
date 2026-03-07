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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_DOWN_WIDEN_CAST_ENABLED;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.ORDERS;

/**
 * Microbenchmark comparing query latency with and without the {@code PushDownWidenCast} optimizer.
 *
 * <p>Uses a {@link DistributedQueryRunner} backed by Hive in PARQUET format, mirroring the
 * production path taken by the Velox native worker. The benchmark runs a fixed number of warm-up
 * and measured iterations for each query/session combination and prints a comparison table to
 * stdout.
 *
 * <p><b>Note:</b> The {@code PushDownWidenCast} optimizer only fires when both
 * {@code push_down_widen_cast_enabled=true} and {@code native_execution_enabled=true} are set.
 * The Java Hive connector does not perform type widening during scan, so the optimization is
 * gated on native execution. Running this benchmark without a Velox native worker will show
 * ~0% improvement (optimizer does not fire on the Java path).
 *
 * <p>Run:
 * <pre>
 *   mvn test -pl presto-hive -Dtest=BenchmarkPushDownWidenCast -Dcheckstyle.skip=true
 * </pre>
 */
@Test(singleThreaded = true)
public class BenchmarkPushDownWidenCast
{
    private static final int WARMUP = 3;
    private static final int MEASURED = 5;

    private DistributedQueryRunner queryRunner;
    private Session sessionOn;
    private Session sessionOff;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = (DistributedQueryRunner) HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, LINE_ITEM),
                ImmutableMap.of(),
                Optional.empty());

        // Create PARQUET benchmark tables from the loaded TPCH tiny data
        queryRunner.execute("DROP TABLE IF EXISTS bench_orders_parquet");
        queryRunner.execute(
                "CREATE TABLE bench_orders_parquet WITH (format = 'PARQUET') AS " +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderpriority, clerk, " +
                "       shippriority, comment " +
                "FROM orders");

        queryRunner.execute("DROP TABLE IF EXISTS bench_orders_parquet_ext");
        queryRunner.execute(
                "CREATE TABLE bench_orders_parquet_ext WITH (format = 'PARQUET') AS " +
                "SELECT orderkey, custkey, " +
                "       shippriority, " +
                "       cast(shippriority AS tinyint)  AS tiny_shippriority, " +
                "       cast(shippriority AS smallint) AS small_shippriority " +
                "FROM orders");

        queryRunner.execute("DROP TABLE IF EXISTS bench_lineitem_parquet");
        queryRunner.execute(
                "CREATE TABLE bench_lineitem_parquet WITH (format = 'PARQUET') AS " +
                "SELECT orderkey, linenumber, quantity, extendedprice, discount, tax " +
                "FROM lineitem");

        sessionOff = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(PUSH_DOWN_WIDEN_CAST_ENABLED, "false")
                .build();

        sessionOn = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(PUSH_DOWN_WIDEN_CAST_ENABLED, "true")
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.execute("DROP TABLE IF EXISTS bench_orders_parquet");
            queryRunner.execute("DROP TABLE IF EXISTS bench_orders_parquet_ext");
            queryRunner.execute("DROP TABLE IF EXISTS bench_lineitem_parquet");
            queryRunner.close();
        }
    }

    // -----------------------------------------------------------------------
    // Benchmark queries
    // -----------------------------------------------------------------------

    private static final String Q_SIMPLE_PROJECTION =
            "SELECT CAST(shippriority AS BIGINT) FROM bench_orders_parquet";

    private static final String Q_AGGREGATION =
            "SELECT CAST(shippriority AS BIGINT), count(*), sum(CAST(shippriority AS BIGINT)) " +
            "FROM bench_orders_parquet GROUP BY 1";

    private static final String Q_MULTI_COLUMN =
            "SELECT " +
            "    CAST(shippriority AS BIGINT)       AS pri_big, " +
            "    CAST(tiny_shippriority AS INTEGER) AS tiny_to_int, " +
            "    CAST(small_shippriority AS BIGINT) AS small_to_big " +
            "FROM bench_orders_parquet_ext";

    private static final String Q_JOIN =
            "SELECT o.orderkey, CAST(o.shippriority AS BIGINT), l.linenumber " +
            "FROM bench_orders_parquet o JOIN bench_lineitem_parquet l ON o.orderkey = l.orderkey";

    // -----------------------------------------------------------------------
    // Test entry point
    // -----------------------------------------------------------------------

    @Test
    public void runBenchmark()
    {
        System.out.println();
        System.out.println("=".repeat(100));
        System.out.println("BenchmarkPushDownWidenCast  (warmup=" + WARMUP + "  measured=" + MEASURED + ")");
        System.out.println("NOTE: Full scan-level benefit requires Velox native execution.");
        System.out.println("      Java-worker savings: elimination of the ProjectNode CAST operator only.");
        System.out.println("=".repeat(100));

        runCase("simple_projection", Q_SIMPLE_PROJECTION);
        runCase("aggregation", Q_AGGREGATION);
        runCase("multi_column_cast", Q_MULTI_COLUMN);
        runCase("join_with_cast", Q_JOIN);

        System.out.println("=".repeat(100));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void runCase(String name, String sql)
    {
        System.out.printf("%n  %-25s%n", name);

        long offMs = timeQuery(sessionOff, sql);
        long onMs = timeQuery(sessionOn, sql);

        double pct = offMs > 0 ? 100.0 * (offMs - onMs) / offMs : 0;
        System.out.printf("    opt=OFF  %5d ms  |  opt=ON  %5d ms  |  improvement %+.1f%%%n",
                offMs, onMs, pct);
    }

    /** Runs the query {@code WARMUP+MEASURED} times; returns the median of the measured runs in ms. */
    private long timeQuery(Session session, String sql)
    {
        // warm-up
        for (int i = 0; i < WARMUP; i++) {
            MaterializedResult ignored = queryRunner.execute(session, sql);
        }

        List<Long> durations = new ArrayList<>(MEASURED);
        for (int i = 0; i < MEASURED; i++) {
            long t0 = System.currentTimeMillis();
            MaterializedResult ignored = queryRunner.execute(session, sql);
            durations.add(System.currentTimeMillis() - t0);
        }

        return median(durations);
    }

    private static long median(List<Long> values)
    {
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compareTo);
        return sorted.get(sorted.size() / 2);
    }
}
