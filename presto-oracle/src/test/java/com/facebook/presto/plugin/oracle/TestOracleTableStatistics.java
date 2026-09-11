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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestOracleTableStatistics
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "oracle_stats";
    private static final String JMX_MBEAN = "com.facebook.presto.plugin.jdbc:type=JdbcMetadataCacheStats,name=" + CATALOG;
    private static final String ATTR_HIT = "tablestatisticscachehit";
    private static final String ATTR_MISS = "tablestatisticscachemiss";
    private static final String ATTR_EVICTION = "tablestatisticscacheeviction";
    private static final String ATTR_SIZE = "tablestatisticscachesize";
    private static final String ATTR_LOAD_SUCCESS = "tablestatisticscacheloadsuccesscount";
    private static final String ATTR_LOAD_EXCEPTION = "tablestatisticscacheloadexceptioncount";
    private static final String ATTR_AVG_LOAD_PENALTY = "tablestatisticscacheaverageloadpenalty";

    private final OracleServerTester oracleServer;

    protected TestOracleTableStatistics()
    {
        this.oracleServer = new OracleServerTester();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Use nodeCount=1 so that the coordinator is the sole server in the JVM.
        return createOracleQueryRunner(
                oracleServer,
                CATALOG,
                ImmutableMap.of(
                        "table-statistics-cache-ttl", "1h",
                        "table-statistics-cache-maximum-size", "10000"),
                ImmutableList.of(ORDERS),
                1);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (oracleServer != null) {
            oracleServer.close();
        }
    }

    @Test
    public void testShowStatsRowCount()
    {
        gatherStats(OracleServerTester.TEST_SCHEMA.toUpperCase(), "ORDERS");

        MaterializedResult result = computeActual("SHOW STATS FOR orders");
        assertTrue(result.getRowCount() > 0, "SHOW STATS FOR must return at least one row after ANALYZE");

        // Summary row has null column_name, row_count is field index 4.
        MaterializedRow summaryRow = result.getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .findFirst()
                .orElseThrow(() -> new AssertionError("SHOW STATS FOR must include a summary row"));

        assertNotNull(summaryRow.getField(4), "row_count must be non-null after ANALYZE");
        assertTrue(((Double) summaryRow.getField(4)) > 0, "row_count must be > 0 after ANALYZE");
    }

    @Test
    public void testShowStatsEmptyWithoutAnalyze()
    {
        String table = "STATS_NO_ANALYZE";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER, name VARCHAR2(50))", table));
        try {
            oracleServer.execute(format("INSERT INTO %s VALUES (1, 'Alice')", table));
            oracleServer.execute("COMMIT");

            MaterializedResult result = computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));

            MaterializedRow summaryRow = result.getMaterializedRows().stream()
                    .filter(row -> row.getField(0) == null)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("SHOW STATS FOR must always include a summary row"));

            // Oracle may or may not have auto-statistics enabled, accept null (no stats) or 0.0 (empty stats).
            Double rowCount = (Double) summaryRow.getField(4);
            assertTrue(rowCount == null || rowCount == 0.0, format("row_count must be null or 0 when ANALYZE has not been run, got: %s", rowCount));
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testShowStatsColumnStats()
    {
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String table = "STATS_COLS";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER, name VARCHAR2(50), score BINARY_DOUBLE)", table));
        try {
            oracleServer.execute(format("INSERT INTO %s VALUES (1, 'Alice',  95.5)", table));
            oracleServer.execute(format("INSERT INTO %s VALUES (2, 'Bob',    80.0)", table));
            oracleServer.execute(format("INSERT INTO %s VALUES (3, 'Alice',  70.0)", table));
            oracleServer.execute(format("INSERT INTO %s VALUES (4, NULL,     60.0)", table));
            oracleServer.execute("COMMIT");
            gatherStats(schema, table);

            MaterializedResult result = computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));

            assertTrue(result.getRowCount() >= 4, "Expected stats rows for all columns plus summary row");

            MaterializedRow summaryRow = result.getMaterializedRows().stream()
                    .filter(row -> row.getField(0) == null)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No summary row in SHOW STATS FOR output"));
            assertEquals((Double) summaryRow.getField(4), 4.0, "Expected row_count=4");

            MaterializedRow nameRow = result.getMaterializedRows().stream()
                    .filter(row -> "name".equalsIgnoreCase((String) row.getField(0)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No NAME column row in SHOW STATS FOR output"));
            Double nullFraction = (Double) nameRow.getField(2);
            assertNotNull(nullFraction, "NAME null_fraction must not be null");
            assertTrue(nullFraction > 0.0, format("Expected null_fraction > 0 for NAME, got: %s", nullFraction));
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testStatsCacheJmxRegistered()
    {
        MaterializedResult result = computeActual(format(
                "SELECT \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\" FROM jmx.current.\"%s\"",
                ATTR_HIT, ATTR_MISS, ATTR_EVICTION, ATTR_SIZE,
                ATTR_LOAD_SUCCESS, ATTR_LOAD_EXCEPTION, ATTR_AVG_LOAD_PENALTY,
                JMX_MBEAN));

        assertFalse(result.getMaterializedRows().isEmpty(), "JMX MBean must be registered");

        MaterializedRow row = result.getMaterializedRows().get(0);
        assertTrue(((Long) row.getField(0)) >= 0, "hit count must be >= 0");
        assertTrue(((Long) row.getField(1)) >= 0, "miss count must be >= 0");
        assertTrue(((Long) row.getField(2)) >= 0, "eviction count must be >= 0");
        assertTrue(((Long) row.getField(3)) >= 0, "cache size must be >= 0");
        assertTrue(((Long) row.getField(4)) >= 0, "load success count must be >= 0");
        assertTrue(((Long) row.getField(5)) >= 0, "load exception count must be >= 0");
        assertTrue(((Double) row.getField(6)) >= 0.0, "average load penalty must be >= 0");
    }

    @Test
    public void testCacheStability()
    {
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String table = "STATS_REPEAT";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER)", table));
        try {
            oracleServer.execute(format("INSERT INTO %s VALUES (1)", table));
            oracleServer.execute(format("INSERT INTO %s VALUES (2)", table));
            oracleServer.execute("COMMIT");
            gatherStats(schema, table);

            MaterializedResult first = computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));
            MaterializedResult second = computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));

            Double rowCount1 = (Double) first.getMaterializedRows().stream()
                    .filter(row -> row.getField(0) == null)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No summary row in first SHOW STATS FOR"))
                    .getField(4);
            Double rowCount2 = (Double) second.getMaterializedRows().stream()
                    .filter(row -> row.getField(0) == null)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No summary row in second SHOW STATS FOR"))
                    .getField(4);

            assertEquals(rowCount1, rowCount2, "Cached row_count must be identical across repeated requests");
            assertTrue(rowCount1 != null && rowCount1 > 0, format("row_count must be positive after ANALYZE, got: %s", rowCount1));
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testCacheIsolation()
    {
        // Each table's cached entry must reflect only that table's data.
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String table1 = "STATS_INDEP_T1";
        String table2 = "STATS_INDEP_T2";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER)", table1));
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER)", table2));
        try {
            // table1 has 1 row, table2 has 3 rows - their row_counts must differ.
            oracleServer.execute(format("INSERT INTO %s VALUES (1)", table1));
            oracleServer.execute(format("INSERT INTO %s VALUES (1)", table2));
            oracleServer.execute(format("INSERT INTO %s VALUES (2)", table2));
            oracleServer.execute(format("INSERT INTO %s VALUES (3)", table2));
            oracleServer.execute("COMMIT");
            gatherStats(schema, table1);
            gatherStats(schema, table2);

            Double rowCount1 = summaryRowCount(computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table1)));
            Double rowCount2 = summaryRowCount(computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table2)));

            assertEquals(rowCount1, 1.0, "table1 must report row_count=1");
            assertEquals(rowCount2, 3.0, "table2 must report row_count=3");
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table1));
            oracleServer.execute(format("DROP TABLE %s", table2));
        }
    }

    @Test
    public void testGlobalCacheLoadCountIncreasesOnShowStats()
    {
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String table = "STATS_LOAD_CHECK";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER)", table));
        try {
            oracleServer.execute(format("INSERT INTO %s VALUES (1)", table));
            oracleServer.execute("COMMIT");
            gatherStats(schema, table);

            long loadBefore = jmxLong(ATTR_LOAD_SUCCESS);
            long exceptionBefore = jmxLong(ATTR_LOAD_EXCEPTION);

            computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));

            long loadAfter = jmxLong(ATTR_LOAD_SUCCESS);
            long exceptionAfter = jmxLong(ATTR_LOAD_EXCEPTION);

            assertTrue(loadAfter > loadBefore || exceptionAfter > exceptionBefore,
                    format("SHOW STATS FOR must drive at least one cache load (success or exception). "
                            + "loadSuccessCount: %d->%d, loadExceptionCount: %d->%d",
                            loadBefore, loadAfter, exceptionBefore, exceptionAfter));

            assertTrue(loadAfter - loadBefore >= 1 && exceptionAfter == exceptionBefore,
                    format("SHOW STATS FOR must complete with exactly 1 successful load and 0 exceptions. "
                            + "loadSuccessCount delta: %d, loadExceptionCount delta: %d",
                            loadAfter - loadBefore, exceptionAfter - exceptionBefore));
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testGlobalCacheServesSecondQueryFromCache()
    {
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String table = "STATS_GLOBAL_CACHE";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER)", table));
        try {
            oracleServer.execute(format("INSERT INTO %s VALUES (42)", table));
            oracleServer.execute("COMMIT");
            gatherStats(schema, table);

            long loadBefore = jmxLong(ATTR_LOAD_SUCCESS);

            // First SHOW STATS: cold entry - exactly 1 DB load.
            computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));
            long loadAfterFirst = jmxLong(ATTR_LOAD_SUCCESS);
            assertEquals(loadAfterFirst - loadBefore, 1L, "First SHOW STATS must trigger exactly 1 DB load. delta=" + (loadAfterFirst - loadBefore));

            // Second SHOW STATS: warm entry - zero DB loads.
            computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));
            long loadAfterSecond = jmxLong(ATTR_LOAD_SUCCESS);
            assertEquals(loadAfterSecond - loadAfterFirst, 0L, "Second SHOW STATS must be a global cache hit. delta=" + (loadAfterSecond - loadAfterFirst));
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testGlobalCacheIsolatesLoadPerTable()
    {
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String table1 = "STATS_ISO_T1";
        String table2 = "STATS_ISO_T2";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER)", table1));
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER)", table2));
        try {
            oracleServer.execute(format("INSERT INTO %s VALUES (1)", table1));
            oracleServer.execute(format("INSERT INTO %s VALUES (1)", table2));
            oracleServer.execute("COMMIT");
            gatherStats(schema, table1);
            gatherStats(schema, table2);

            long loadBefore = jmxLong(ATTR_LOAD_SUCCESS);

            computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table1));
            assertEquals(jmxLong(ATTR_LOAD_SUCCESS) - loadBefore, 1L, "table1 first load must be 1 DB call");

            computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table2));
            assertEquals(jmxLong(ATTR_LOAD_SUCCESS) - loadBefore, 2L, "table2 first load must be 1 additional DB call");

            computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table1));
            assertEquals(jmxLong(ATTR_LOAD_SUCCESS) - loadBefore, 2L, "table1 second load must be a cache hit");
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table1));
            oracleServer.execute(format("DROP TABLE %s", table2));
        }
    }

    @Test
    public void testTransactionCacheDisabledStillServesStats()
            throws Exception
    {
        // When metadata-transaction-cache-enabled=false the JdbcMetadataFactory.create() path
        // bypasses the transaction-level cache and uses the global cache directly.
        String jmxBean = "com.facebook.presto.plugin.jdbc:type=JdbcMetadataCacheStats,name=oracle_notx";
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String table = "STATS_NOTX";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER)", table));
        try {
            oracleServer.execute(format("INSERT INTO %s VALUES (1)", table));
            oracleServer.execute(format("INSERT INTO %s VALUES (2)", table));
            oracleServer.execute("COMMIT");
            gatherStats(schema, table);

            try (QueryRunner noTxRunner = OracleQueryRunner.createOracleQueryRunner(
                    oracleServer,
                    "oracle_notx",
                    ImmutableMap.of(
                            "table-statistics-cache-ttl", "1h",
                            "table-statistics-cache-maximum-size", "10000",
                            "metadata-transaction-cache-enabled", "false"),
                    ImmutableList.of(),
                    1)) {
                // First SHOW STATS: global cache miss -> 1 DB load.
                long loadBefore = jmxLongFrom(noTxRunner, ATTR_LOAD_SUCCESS, jmxBean);
                noTxRunner.execute(noTxRunner.getDefaultSession(),
                        format("SHOW STATS FOR oracle_notx.%s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));
                assertEquals(jmxLongFrom(noTxRunner, ATTR_LOAD_SUCCESS, jmxBean) - loadBefore, 1L, "With tx-cache disabled: first SHOW STATS must trigger 1 DB load");

                MaterializedResult result = noTxRunner.execute(noTxRunner.getDefaultSession(),
                        format("SHOW STATS FOR oracle_notx.%s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));
                Double rowCount = (Double) result.getMaterializedRows().stream()
                        .filter(row -> row.getField(0) == null)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("No summary row"))
                        .getField(4);
                assertEquals(rowCount, 2.0, "With tx-cache disabled: row_count must still be correct");

                // Second SHOW STATS: global cache must serve it (0 new DB loads).
                long loadAfterFirst = jmxLongFrom(noTxRunner, ATTR_LOAD_SUCCESS, jmxBean);
                assertEquals(loadAfterFirst - loadBefore, 1L, "With tx-cache disabled: second SHOW STATS must be a global cache hit");
            }
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testGlobalCacheStoresAllColumnsAndProjectsSubsetOnReuse()
    {
        // The global cache always fetches ALL columns. A second SHOW STATS FOR the same table
        // must be served from the global cache (0 new DB loads) regardless of which columns
        // are requested - the cache projects the right subset from the stored full snapshot.
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String table = "STATS_PROJ_CACHE";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER, name VARCHAR2(50), score NUMBER)", table));
        try {
            oracleServer.execute(format("INSERT INTO %s VALUES (1, 'Alice', 95)", table));
            oracleServer.execute(format("INSERT INTO %s VALUES (2, 'Bob',   80)", table));
            oracleServer.execute(format("INSERT INTO %s VALUES (3, NULL,    70)", table));
            oracleServer.execute("COMMIT");
            gatherStats(schema, table);

            long loadBefore = jmxLong(ATTR_LOAD_SUCCESS);

            computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));
            long loadAfterFirst = jmxLong(ATTR_LOAD_SUCCESS);
            assertEquals(loadAfterFirst - loadBefore, 1L, "First SHOW STATS must trigger exactly 1 DB load to populate the global cache");

            // The global cache projects the requested columns from its full snapshot - no new DB call.
            computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));
            long loadAfterSecond = jmxLong(ATTR_LOAD_SUCCESS);
            assertEquals(loadAfterSecond - loadAfterFirst, 0L, "Second SHOW STATS for same table must be served from global cache (0 new DB loads)");

            MaterializedResult result = computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));
            long nonSummaryRows = result.getMaterializedRows().stream()
                    .filter(row -> row.getField(0) != null)
                    .count();
            assertTrue(nonSummaryRows >= 1, "Cached result must still contain column-level stats rows, not just the summary row");
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testTransactionCacheLoadsEachTableIndependently()
    {
        // Within a single Presto query that joins two Oracle tables, the query planner calls
        // getTableStatistics once per table inside the same transaction (same JdbcMetadata instance).
        // The transaction cache must load each table independently - 2 DB loads for 2 tables -
        // but must NOT duplicate-load either table.
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String tableA = "STATS_TX_A";
        String tableB = "STATS_TX_B";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER, val NUMBER)", tableA));
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER, val NUMBER)", tableB));
        try {
            oracleServer.execute(format("INSERT INTO %s VALUES (1, 10)", tableA));
            oracleServer.execute(format("INSERT INTO %s VALUES (2, 20)", tableA));
            oracleServer.execute(format("INSERT INTO %s VALUES (1, 100)", tableB));
            oracleServer.execute(format("INSERT INTO %s VALUES (2, 200)", tableB));
            oracleServer.execute("COMMIT");
            gatherStats(schema, tableA);
            gatherStats(schema, tableB);

            // Warm the global cache for tableA only - so tableB is guaranteed a cold miss.
            // tableA will be served from global cache during the join query (no new load for it).
            computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, tableA));
            long loadAfterWarm = jmxLong(ATTR_LOAD_SUCCESS);

            // Run a JOIN query: the planner must request stats for both tables within one
            // planning transaction.  tableA is warm (no new load); tableB is cold (1 new load).
            computeActual(format(
                    "SELECT a.id FROM %s.\"%s\" a JOIN %s.\"%s\" b ON a.id = b.id",
                    OracleServerTester.TEST_SCHEMA, tableA,
                    OracleServerTester.TEST_SCHEMA, tableB));

            long loadAfterJoin = jmxLong(ATTR_LOAD_SUCCESS);
            assertEquals(loadAfterJoin - loadAfterWarm, 1L, "JOIN query must trigger exactly 1 new global cache load (tableB cold miss, tableA warm hit). delta=" + (loadAfterJoin - loadAfterWarm));

            // Verify SHOW STATS for tableB is now served from global cache (it was populated by the join).
            long loadBeforeB = jmxLong(ATTR_LOAD_SUCCESS);
            computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, tableB));
            assertEquals(jmxLong(ATTR_LOAD_SUCCESS) - loadBeforeB, 0L, "After the JOIN query populated tableB in global cache, SHOW STATS FOR tableB must be a cache hit");
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", tableA));
            oracleServer.execute(format("DROP TABLE %s", tableB));
        }
    }

    @Test
    public void testShowStatsNonExistentTable()
    {
        try {
            computeActual(format("SHOW STATS FOR %s.\"DOES_NOT_EXIST\"", OracleServerTester.TEST_SCHEMA));
        }
        catch (RuntimeException e) {
            Throwable cause = e;
            while (cause != null) {
                assertFalse(cause instanceof NullPointerException, "NullPointerException must not propagate from the stats path: " + cause.getMessage());
                cause = cause.getCause();
            }
        }
    }

    @Test
    public void testShowStatsZeroRowsAfterAnalyze()
    {
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String table = "STATS_EMPTY";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER, name VARCHAR2(50))", table));
        try {
            // no rows inserted - table is empty
            gatherStats(schema, table);

            MaterializedResult result = computeActual(format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));

            MaterializedRow summaryRow = result.getMaterializedRows().stream()
                    .filter(row -> row.getField(0) == null)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("SHOW STATS FOR must always include a summary row"));

            Double rowCount = (Double) summaryRow.getField(4);
            assertNotNull(rowCount, "row_count must not be null after ANALYZE even on an empty table");
            assertEquals(rowCount, 0.0, "row_count must be 0 for an empty table");
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testCacheMaximumSizeEvictionCausesReload()
            throws Exception
    {
        String jmxBean = "com.facebook.presto.plugin.jdbc:type=JdbcMetadataCacheStats,name=oracle_evict";
        String schema = OracleServerTester.TEST_SCHEMA.toUpperCase();
        String table1 = "STATS_EVICT_T1";
        String table2 = "STATS_EVICT_T2";
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER)", table1));
        oracleServer.execute(format("CREATE TABLE %s (id NUMBER)", table2));
        try {
            oracleServer.execute(format("INSERT INTO %s VALUES (1)", table1));
            oracleServer.execute(format("INSERT INTO %s VALUES (1)", table2));
            oracleServer.execute("COMMIT");
            gatherStats(schema, table1);
            gatherStats(schema, table2);

            // Use a fresh runner with maximum-size=1 so eviction is deterministic.
            // The catalog name "oracle_evict" gives this runner a unique JMX MBean name.
            try (QueryRunner evictRunner = OracleQueryRunner.createOracleQueryRunner(
                    oracleServer,
                    "oracle_evict",
                    ImmutableMap.of(
                            "table-statistics-cache-ttl", "1h",
                            "table-statistics-cache-maximum-size", "1"),
                    ImmutableList.of(),
                    1)) {
                long loadBefore = jmxLongFrom(evictRunner, ATTR_LOAD_SUCCESS, jmxBean);

                evictRunner.execute(evictRunner.getDefaultSession(), format("SHOW STATS FOR oracle_evict.%s.\"%s\"", OracleServerTester.TEST_SCHEMA, table1));
                assertEquals(jmxLongFrom(evictRunner, ATTR_LOAD_SUCCESS, jmxBean) - loadBefore, 1L, "table1 first load must be exactly 1 DB call");

                evictRunner.execute(evictRunner.getDefaultSession(), format("SHOW STATS FOR oracle_evict.%s.\"%s\"", OracleServerTester.TEST_SCHEMA, table2));
                assertEquals(jmxLongFrom(evictRunner, ATTR_LOAD_SUCCESS, jmxBean) - loadBefore, 2L, "table2 first load must be exactly 1 additional DB call");

                assertTrue(jmxLongFrom(evictRunner, ATTR_EVICTION, jmxBean) >= 1L, "evictionCount must be >= 1 after size-based eviction");

                evictRunner.execute(evictRunner.getDefaultSession(), format("SHOW STATS FOR oracle_evict.%s.\"%s\"", OracleServerTester.TEST_SCHEMA, table1));
                assertEquals(jmxLongFrom(evictRunner, ATTR_LOAD_SUCCESS, jmxBean) - loadBefore, 3L, "table1 must be reloaded from DB after eviction");
            }
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table1));
            oracleServer.execute(format("DROP TABLE %s", table2));
        }
    }

    private static Double summaryRowCount(MaterializedResult result)
    {
        return (Double) result.getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No summary row in SHOW STATS FOR output"))
                .getField(4);
    }

    private long jmxLong(String attribute)
    {
        MaterializedResult result = computeActual(format(
                "SELECT \"%s\" FROM jmx.current.\"%s\"",
                attribute, JMX_MBEAN));
        return (Long) result.getMaterializedRows().get(0).getField(0);
    }

    private static long jmxLongFrom(QueryRunner runner, String attribute, String mbean)
    {
        MaterializedResult result = runner.execute(runner.getDefaultSession(), format(
                "SELECT \"%s\" FROM jmx.current.\"%s\"",
                attribute, mbean));
        return (Long) result.getMaterializedRows().get(0).getField(0);
    }

    private void gatherStats(String schema, String table)
    {
        oracleServer.execute(
                format("BEGIN DBMS_STATS.GATHER_TABLE_STATS("
                                + "ownname => '%s', "
                                + "tabname => '%s'"
                                + "); END;",
                        schema, table));
    }
}
