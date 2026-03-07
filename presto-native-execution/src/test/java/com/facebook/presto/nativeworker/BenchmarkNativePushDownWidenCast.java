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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static com.facebook.presto.SystemSessionProperties.PUSH_DOWN_WIDEN_CAST_ENABLED;

/**
 * Microbenchmark comparing query latency with and without the {@code PushDownWidenCast} optimizer
 * on the Velox native execution path.
 *
 * <p>Requires a built Velox worker binary. Pass the path via JVM argument:
 * <pre>
 *   -DPRESTO_SERVER=&lt;path/to/presto_server&gt;
 *   -DDATA_DIR=&lt;path/to/data_dir&gt;
 * </pre>
 *
 * <p>Run:
 * <pre>
 *   mvn test -pl presto-native-execution \
 *       -Dtest=BenchmarkNativePushDownWidenCast \
 *       -DPRESTO_SERVER=/path/to/presto_server \
 *       -DDATA_DIR=/tmp/velox_bench_data \
 *       -Dcheckstyle.skip=true
 * </pre>
 *
 * <p>With native execution the optimizer pushes widening casts (e.g., INTEGER→BIGINT) into the
 * {@code TableScanNode} so the Velox Parquet reader applies the coercion inline during column
 * reading, eliminating the {@code ProjectNode} CAST operator entirely.
 */
@Test(singleThreaded = true)
public class BenchmarkNativePushDownWidenCast
{
    private static final int WARMUP = 3;
    private static final int MEASURED = 5;

    private DistributedQueryRunner queryRunner;
    private Session sessionOff;
    private Session sessionOn;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        // HiveQueryRunner asserts DateTimeZone.getDefault() == America/Bahia_Banderas.
        // Set it here so callers don't need -Duser.timezone on the command line.
        TimeZone.setDefault(TimeZone.getTimeZone("America/Bahia_Banderas"));

        // Native runner: sets native_execution_enabled=true by default at the server level,
        // which is inherited as the session default. The PushDownWidenCast optimizer therefore
        // fires whenever push_down_widen_cast_enabled=true is set in the session.
        queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .build();

        createBenchmarkTablesIfAbsent();

        sessionOff = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(PUSH_DOWN_WIDEN_CAST_ENABLED, "false")
                .build();

        // native_execution_enabled is already true by default in this runner.
        sessionOn = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(PUSH_DOWN_WIDEN_CAST_ENABLED, "true")
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
    }

    // -----------------------------------------------------------------------
    // Table creation
    // -----------------------------------------------------------------------

    private void createBenchmarkTablesIfAbsent()
    {
        // Use IF NOT EXISTS so repeated runs reuse persisted data in DATA_DIR.
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "bench_orders_parquet")) {
            queryRunner.execute(
                    "CREATE TABLE bench_orders_parquet WITH (format = 'PARQUET') AS " +
                    "SELECT orderkey, custkey, orderstatus, totalprice, orderpriority, clerk, " +
                    "       shippriority, comment " +
                    "FROM tpch.tiny.orders");
        }

        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "bench_orders_parquet_narrow")) {
            queryRunner.execute(
                    "CREATE TABLE bench_orders_parquet_narrow WITH (format = 'PARQUET') AS " +
                    "SELECT orderkey, custkey, " +
                    "       shippriority, " +
                    "       cast(shippriority AS tinyint)  AS tiny_shippriority, " +
                    "       cast(shippriority AS smallint) AS small_shippriority " +
                    "FROM tpch.tiny.orders");
        }

        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "bench_lineitem_parquet")) {
            queryRunner.execute(
                    "CREATE TABLE bench_lineitem_parquet WITH (format = 'PARQUET') AS " +
                    "SELECT orderkey, linenumber, quantity, extendedprice, discount, tax " +
                    "FROM tpch.tiny.lineitem");
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
            "    CAST(shippriority AS BIGINT)              AS pri_big, " +
            "    CAST(tiny_shippriority AS INTEGER)        AS tiny_to_int, " +
            "    CAST(small_shippriority AS BIGINT)        AS small_to_big " +
            "FROM bench_orders_parquet_narrow";

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
        System.out.println("BenchmarkNativePushDownWidenCast  (warmup=" + WARMUP + "  measured=" + MEASURED + ")");
        System.out.println("Velox native execution — optimizer pushes cast into Parquet reader.");
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
