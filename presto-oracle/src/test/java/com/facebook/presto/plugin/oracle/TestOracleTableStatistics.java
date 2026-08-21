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

@Test(singleThreaded = true)
public class TestOracleTableStatistics
        extends AbstractTestQueryFramework
{
    private static final String JMX_MBEAN = "com.facebook.presto.plugin.jdbc:type=JdbcMetadataCacheStats,name=oracle";
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
        return createOracleQueryRunner(
                oracleServer,
                ImmutableMap.of(
                        "table-statistics-cache-ttl", "1h",
                        "table-statistics-cache-maximum-size", "10000"),
                ImmutableList.of(ORDERS));
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

            MaterializedResult result = computeActual(
                    format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));

            MaterializedRow summaryRow = result.getMaterializedRows().stream()
                    .filter(row -> row.getField(0) == null)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("SHOW STATS FOR must always include a summary row"));

            // Oracle may or may not have auto-statistics enabled, accept null (no stats) or 0.0 (empty stats).
            Double rowCount = (Double) summaryRow.getField(4);
            assertTrue(rowCount == null || rowCount == 0.0,
                    format("row_count must be null or 0 when ANALYZE has not been run, got: %s", rowCount));
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

            MaterializedResult result = computeActual(
                    format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));

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

            MaterializedResult first = computeActual(
                    format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));
            MaterializedResult second = computeActual(
                    format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table));

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
            assertTrue(rowCount1 != null && rowCount1 > 0,
                    format("row_count must be positive after ANALYZE, got: %s", rowCount1));
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

            Double rowCount1 = summaryRowCount(computeActual(
                    format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table1)));
            Double rowCount2 = summaryRowCount(computeActual(
                    format("SHOW STATS FOR %s.\"%s\"", OracleServerTester.TEST_SCHEMA, table2)));

            assertEquals(rowCount1, 1.0, "table1 must report row_count=1");
            assertEquals(rowCount2, 3.0, "table2 must report row_count=3");
        }
        finally {
            oracleServer.execute(format("DROP TABLE %s", table1));
            oracleServer.execute(format("DROP TABLE %s", table2));
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
                assertFalse(cause instanceof NullPointerException,
                        "NullPointerException must not propagate from the stats path: " + cause.getMessage());
                cause = cause.getCause();
            }
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
