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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.TestingSession;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.testing.Assertions;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.ADD_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SET_SESSION;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SET_USER;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestConnectorQueries
        extends AbstractTestQueryFramework
{
    protected AbstractTestConnectorQueries(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    protected boolean supportsViews()
    {
        return true;
    }

    @Test
    public void testShowPartitions()
    {
        assertQueryFails("SHOW PARTITIONS FROM orders", "line 1:1: Table does not have partition columns: \\S+\\.orders");
        assertQueryFails("SHOW PARTITIONS FROM orders WHERE orderkey < 10", "line 1:1: Table does not have partition columns: \\S+\\.orders");
        assertQueryFails("SHOW PARTITIONS FROM orders WHERE invalid_column < 10", "line 1:35: Column 'invalid_column' cannot be resolved");
        assertQueryFails("SHOW PARTITIONS FROM orders LIMIT 2", "line 1:1: Table does not have partition columns: \\S+\\.orders");
    }

    @Test
    public void testSingleDistinctOptimizer()
    {
        assertQuery("SELECT custkey, orderstatus, COUNT(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");
        assertQuery("SELECT custkey, orderstatus, COUNT(DISTINCT orderkey), SUM(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");
        assertQuery("" +
                "SELECT custkey, COUNT(DISTINCT orderstatus) FROM (" +
                "   SELECT orders.custkey AS custkey, orders.orderstatus AS orderstatus " +
                "   FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey " +
                "   GROUP BY orders.custkey, orders.orderstatus" +
                ") " +
                "GROUP BY custkey");
        assertQuery("SELECT custkey, COUNT(DISTINCT orderkey), COUNT(DISTINCT orderstatus) FROM orders GROUP BY custkey");

        assertQuery("SELECT SUM(DISTINCT x) FROM (SELECT custkey, COUNT(DISTINCT orderstatus) x FROM orders GROUP BY custkey) t");
    }

    @Test
    public void testExtractDistinctAggregationOptimizer()
    {
        assertQuery("SELECT max(orderstatus), COUNT(orderkey), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT custkey, orderstatus, avg(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");

        assertQuery("SELECT s, MAX(custkey), SUM(a) FROM (" +
                "    SELECT custkey, avg(shippriority) as a, SUM(DISTINCT orderkey) as s FROM orders GROUP BY custkey, orderstatus" +
                ") " +
                "GROUP BY s");

        assertQuery("SELECT max(orderstatus), COUNT(distinct orderkey), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT max(orderstatus), COUNT(distinct shippriority), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT COUNT(tan(shippriority)), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT count(DISTINCT a), max(b) FROM (VALUES (row(1, 2), 3)) t(a, b)", "VALUES (1, 3)");

        // Test overlap between GroupBy columns and aggregation columns
        assertQuery("SELECT shippriority, MAX(orderstatus), SUM(DISTINCT shippriority) FROM orders GROUP BY shippriority");

        assertQuery("SELECT shippriority, COUNT(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY shippriority");

        assertQuery("SELECT shippriority, COUNT(shippriority), SUM(DISTINCT shippriority) FROM orders GROUP BY shippriority");

        assertQuery("SELECT clerk, shippriority, MAX(orderstatus), SUM(DISTINCT shippriority) FROM orders GROUP BY clerk, shippriority");

        assertQuery("SELECT clerk, shippriority, COUNT(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY clerk, shippriority");

        assertQuery("SELECT clerk, shippriority, COUNT(shippriority), SUM(DISTINCT shippriority) FROM orders GROUP BY clerk, shippriority");
    }

    @Test
    public void testDistinctWindow()
    {
        MaterializedResult actual = computeActual(
                "SELECT RANK() OVER (PARTITION BY orderdate ORDER BY COUNT(DISTINCT clerk)) rnk " +
                        "FROM orders " +
                        "GROUP BY orderdate, custkey " +
                        "ORDER BY rnk " +
                        "LIMIT 1");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT).row(1L).build();
        assertEquals(actual, expected);
    }

    @Test
    public void testDistinctWhere()
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) FROM orders WHERE LENGTH(clerk) > 5");
    }

    @Test
    public void testMultipleDifferentDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT orderstatus), SUM(DISTINCT custkey) FROM orders");
    }

    @Test
    public void testMultipleDistinct()
    {
        assertQuery(
                "SELECT COUNT(DISTINCT custkey), SUM(DISTINCT custkey) FROM orders",
                "SELECT COUNT(*), SUM(custkey) FROM (SELECT DISTINCT custkey FROM orders) t");
    }

    @Test
    public void testComplexDistinct()
    {
        assertQuery(
                "SELECT COUNT(DISTINCT custkey), " +
                        "SUM(DISTINCT custkey), " +
                        "SUM(DISTINCT custkey + 1.0E0), " +
                        "AVG(DISTINCT custkey), " +
                        "VARIANCE(DISTINCT custkey) FROM orders",
                "SELECT COUNT(*), " +
                        "SUM(custkey), " +
                        "SUM(custkey + 1.0), " +
                        "AVG(custkey), " +
                        "VARIANCE(custkey) FROM (SELECT DISTINCT custkey FROM orders) t");
    }

    @Test
    public void testSetSession()
    {
        MaterializedResult result = computeActual("SET SESSION test_string = 'bar'");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("test_string", "bar"));

        result = computeActual(format("SET SESSION %s.connector_long = 999", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_long", "999"));

        result = computeActual(format("SET SESSION %s.connector_string = 'baz'", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_string", "baz"));

        result = computeActual(format("SET SESSION %s.connector_string = 'ban' || 'ana'", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_string", "banana"));

        result = computeActual(format("SET SESSION %s.connector_long = 444", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_long", "444"));

        result = computeActual(format("SET SESSION %s.connector_long = 111 + 111", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_long", "222"));

        result = computeActual(format("SET SESSION %s.connector_boolean = 111 < 3", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_boolean", "false"));

        result = computeActual(format("SET SESSION %s.connector_double = 11.1", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_double", "11.1"));
    }

    @Test
    public void testResetSession()
    {
        MaterializedResult result = computeActual(getSession(), "RESET SESSION test_string");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getResetSessionProperties(), ImmutableSet.of("test_string"));

        result = computeActual(getSession(), format("RESET SESSION %s.connector_string", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getResetSessionProperties(), ImmutableSet.of(TESTING_CATALOG + ".connector_string"));
    }

    @Test
    public void testCreateTable()
    {
        assertUpdate("CREATE TABLE test_create (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create"));
        assertTableColumnNames("test_create", "a", "b", "c");

        assertUpdate("DROP TABLE test_create");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create"));

        assertUpdate("CREATE TABLE test_create_table_if_not_exists (a bigint, b varchar, c double)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exists"));
        assertTableColumnNames("test_create_table_if_not_exists", "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_create_table_if_not_exists (d bigint, e varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exists"));
        assertTableColumnNames("test_create_table_if_not_exists", "a", "b", "c");

        assertUpdate("DROP TABLE test_create_table_if_not_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exists"));

        // Test CREATE TABLE LIKE
        assertUpdate("CREATE TABLE test_create_original (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_original"));
        assertTableColumnNames("test_create_original", "a", "b", "c");

        assertUpdate("CREATE TABLE test_create_like (LIKE test_create_original, d boolean, e varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_like"));
        assertTableColumnNames("test_create_like", "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE test_create_original");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_original"));

        assertUpdate("DROP TABLE test_create_like");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_like"));
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertUpdate("CREATE TABLE test_create_table_as_if_not_exists (a bigint, b double)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "a", "b");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_create_table_as_if_not_exists AS SELECT orderkey, discount FROM lineitem", 0);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "a", "b");

        assertUpdate("DROP TABLE test_create_table_as_if_not_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));

        assertCreateTableAsSelect(
                "test_select",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "test_group",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

        assertCreateTableAsSelect(
                "test_join",
                "SELECT count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "test_limit",
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");

        assertCreateTableAsSelect(
                "test_unicode",
                "SELECT '\u2603' unicode",
                "SELECT 1");

        assertCreateTableAsSelect(
                "test_with_data",
                "SELECT * FROM orders WITH DATA",
                "SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "test_with_no_data",
                "SELECT * FROM orders WITH NO DATA",
                "SELECT * FROM orders LIMIT 0",
                "SELECT 0");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(
                "test_union_all",
                "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 0 UNION ALL " +
                        "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 1",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "test_union_all",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "test_union_all",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE analyze_test AS SELECT orderstatus FROM orders");
        assertQuery("SELECT * from analyze_test", "SELECT orderstatus FROM orders");
        assertUpdate("DROP TABLE analyze_test");
    }

    @Test
    public void testExplainAnalyze()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
        assertExplainAnalyze("" +
                "EXPLAIN ANALYZE SELECT *, o2.custkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 5 = 0)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 256 = 0) o1\n" +
                "JOIN (SELECT * FROM orders WHERE custkey % 256 = 0) o2\n" +
                "  ON (o1.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0)) = (o2.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0))\n" +
                "WHERE o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 4 = 0)\n" +
                "ORDER BY o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 7 = 0)");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk UNION ALL SELECT sum(orderkey), clerk FROM orders GROUP BY clerk");

        assertExplainAnalyze("EXPLAIN ANALYZE SHOW COLUMNS FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN SELECT count(*) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN ANALYZE SELECT count(*) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW FUNCTIONS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW TABLES");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SCHEMAS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW CATALOGS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SESSION");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "EXPLAIN ANALYZE only supported for statements that are queries")
    public void testExplainAnalyzeDDL()
    {
        computeActual("EXPLAIN ANALYZE DROP TABLE orders");
    }

    private void assertExplainAnalyze(@Language("SQL") String query)
    {
        String value = (String) computeActual(query).getOnlyValue();

        assertTrue(value.matches("(?s:.*)CPU:.*, Input:.*, Output(?s:.*)"), format("Expected output to contain \"CPU:.*, Input:.*, Output\", but it is %s", value));

        // TODO: check that rendered plan is as expected, once stats are collected in a consistent way
        // assertTrue(value.contains("Cost: "), format("Expected output to contain \"Cost: \", but it is %s", value));
    }

    protected void assertCreateTableAsSelect(String table, @Language("SQL") String query, @Language("SQL") String rowCountQuery)
    {
        assertCreateTableAsSelect(getSession(), table, query, query, rowCountQuery);
    }

    protected void assertCreateTableAsSelect(String table, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
    {
        assertCreateTableAsSelect(getSession(), table, query, expectedQuery, rowCountQuery);
    }

    protected void assertCreateTableAsSelect(Session session, String table, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
    {
        assertUpdate(session, "CREATE TABLE " + table + " AS " + query, rowCountQuery);
        assertQuery(session, "SELECT * FROM " + table, expectedQuery);
        assertUpdate(session, "DROP TABLE " + table);

        assertFalse(getQueryRunner().tableExists(session, table));
    }

    @Test
    public void testRenameTable()
    {
        assertUpdate("CREATE TABLE test_rename AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE test_rename RENAME TO test_rename_new");
        MaterializedResult materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        // provide new table name in uppercase
        assertUpdate("ALTER TABLE test_rename_new RENAME TO TEST_RENAME");
        materializedRows = computeActual("SELECT x FROM test_rename");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("DROP TABLE test_rename");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_new"));
    }

    @Test
    public void testRenameColumn()
    {
        assertUpdate("CREATE TABLE test_rename_column AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN x TO y");
        MaterializedResult materializedRows = computeActual("SELECT y FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN y TO Z");
        materializedRows = computeActual("SELECT z FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("DROP TABLE test_rename_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_column"));
    }

    @Test
    public void testDropColumn()
    {
        assertUpdate("CREATE TABLE test_drop_column AS SELECT 123 x, 111 a", 1);

        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN x");
        assertQueryFails("SELECT x FROM test_drop_column", ".* Column 'x' cannot be resolved");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN a", ".* Cannot drop the only column in a table");
    }

    @Test
    public void testAddColumn()
    {
        assertUpdate("CREATE TABLE test_add_column AS SELECT 123 x", 1);
        assertUpdate("CREATE TABLE test_add_column_a AS SELECT 234 x, 111 a", 1);
        assertUpdate("CREATE TABLE test_add_column_ab AS SELECT 345 x, 222 a, 33.3E0 b", 1);

        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN x bigint", ".* Column 'x' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN X bigint", ".* Column 'X' already exists");

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN a bigint");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_a", 1);
        MaterializedResult materializedRows = computeActual("SELECT x, a FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123);
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(1), null);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN b double");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_ab", 1);
        materializedRows = computeActual("SELECT x, a, b FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123);
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(1), null);
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(2), null);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(2), null);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(0), 345);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(1), 222L);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(2), 33.3);

        assertUpdate("DROP TABLE test_add_column");
        assertUpdate("DROP TABLE test_add_column_a");
        assertUpdate("DROP TABLE test_add_column_ab");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_a"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_ab"));
    }

    @Test
    public void testInsert()
    {
        @Language("SQL") String query = "SELECT orderdate, orderkey, totalprice FROM orders";

        assertUpdate("CREATE TABLE test_insert AS " + query + " WITH NO DATA", 0);
        assertQuery("SELECT count(*) FROM test_insert", "SELECT 0");

        assertUpdate("INSERT INTO test_insert " + query, "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM test_insert", query);

        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (-1)", 1);
        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (null)", 1);
        assertUpdate("INSERT INTO test_insert (orderdate) VALUES (DATE '2001-01-01')", 1);
        assertUpdate("INSERT INTO test_insert (orderkey, orderdate) VALUES (-2, DATE '2001-01-02')", 1);
        assertUpdate("INSERT INTO test_insert (orderdate, orderkey) VALUES (DATE '2001-01-03', -3)", 1);
        assertUpdate("INSERT INTO test_insert (totalprice) VALUES (1234)", 1);

        assertQuery("SELECT * FROM test_insert", query
                + " UNION ALL SELECT null, -1, null"
                + " UNION ALL SELECT null, null, null"
                + " UNION ALL SELECT DATE '2001-01-01', null, null"
                + " UNION ALL SELECT DATE '2001-01-02', -2, null"
                + " UNION ALL SELECT DATE '2001-01-03', -3, null"
                + " UNION ALL SELECT null, null, 1234");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        assertUpdate(
                "INSERT INTO test_insert (orderkey, orderdate, totalprice) " +
                        "SELECT orderkey, orderdate, totalprice FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, orderdate, totalprice FROM orders",
                "SELECT 2 * count(*) FROM orders");

        assertUpdate("DROP TABLE test_insert");

        assertUpdate("CREATE TABLE test_insert (a ARRAY<DOUBLE>, b ARRAY<BIGINT>)");

        assertUpdate("INSERT INTO test_insert (a) VALUES (ARRAY[null])", 1);
        assertUpdate("INSERT INTO test_insert (a) VALUES (ARRAY[1234])", 1);
        assertQuery("SELECT a[1] FROM test_insert", "VALUES (null), (1234)");

        assertQueryFails("INSERT INTO test_insert (b) VALUES (ARRAY[1.23E1])", "Insert query has mismatched column types: .*");

        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testDelete()
    {
        // delete half the table, then delete the rest

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertUpdate("DELETE FROM test_delete WHERE orderkey % 2 = 0", "SELECT count(*) FROM orders WHERE orderkey % 2 = 0");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE orderkey % 2 <> 0");

        assertUpdate("DELETE FROM test_delete", "SELECT count(*) FROM orders WHERE orderkey % 2 <> 0");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders LIMIT 0");

        assertUpdate("DROP TABLE test_delete");

        // delete successive parts of the table

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertUpdate("DELETE FROM test_delete WHERE custkey <= 100", "SELECT count(*) FROM orders WHERE custkey <= 100");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE custkey > 100");

        assertUpdate("DELETE FROM test_delete WHERE custkey <= 300", "SELECT count(*) FROM orders WHERE custkey > 100 AND custkey <= 300");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE custkey > 300");

        assertUpdate("DELETE FROM test_delete WHERE custkey <= 500", "SELECT count(*) FROM orders WHERE custkey > 300 AND custkey <= 500");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE custkey > 500");

        assertUpdate("DROP TABLE test_delete");

        // delete using a constant property

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertUpdate("DELETE FROM test_delete WHERE orderstatus = 'O'", "SELECT count(*) FROM orders WHERE orderstatus = 'O'");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE orderstatus <> 'O'");

        assertUpdate("DROP TABLE test_delete");

        // delete without matching any rows

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertUpdate("DELETE FROM test_delete WHERE rand() < 0", 0);
        assertUpdate("DELETE FROM test_delete WHERE orderkey < 0", 0);
        assertUpdate("DROP TABLE test_delete");

        // delete with a predicate that optimizes to false

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertUpdate("DELETE FROM test_delete WHERE orderkey > 5 AND orderkey < 4", 0);
        assertUpdate("DROP TABLE test_delete");

        // delete using a subquery

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM lineitem", "SELECT count(*) FROM lineitem");

        assertUpdate(
                "DELETE FROM test_delete WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')",
                "SELECT count(*) FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')");
        assertQuery(
                "SELECT * FROM test_delete",
                "SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus <> 'F')");

        assertUpdate("DROP TABLE test_delete");

        // delete with multiple SemiJoin

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM lineitem", "SELECT count(*) FROM lineitem");

        assertUpdate(
                "DELETE FROM test_delete\n" +
                        "WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')\n" +
                        "  AND orderkey IN (SELECT orderkey FROM orders WHERE custkey % 5 = 0)\n",
                "SELECT count(*) FROM lineitem\n" +
                        "WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')\n" +
                        "  AND orderkey IN (SELECT orderkey FROM orders WHERE custkey % 5 = 0)");
        assertQuery(
                "SELECT * FROM test_delete",
                "SELECT * FROM lineitem\n" +
                        "WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus <> 'F')\n" +
                        "  OR orderkey IN (SELECT orderkey FROM orders WHERE custkey % 5 <> 0)");

        assertUpdate("DROP TABLE test_delete");

        // delete with SemiJoin null handling

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertUpdate(
                "DELETE FROM test_delete\n" +
                        "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NULL\n",
                "SELECT count(*) FROM orders\n" +
                        "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NULL\n");
        assertQuery(
                "SELECT * FROM test_delete",
                "SELECT * FROM orders\n" +
                        "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NOT NULL\n");

        assertUpdate("DROP TABLE test_delete");

        // delete using a scalar and EXISTS subquery
        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertUpdate("DELETE FROM test_delete WHERE orderkey = (SELECT orderkey FROM orders ORDER BY orderkey LIMIT 1)", 1);
        assertUpdate("DELETE FROM test_delete WHERE orderkey = (SELECT orderkey FROM orders WHERE false)", 0);
        assertUpdate("DELETE FROM test_delete WHERE EXISTS(SELECT 1 WHERE false)", 0);
        assertUpdate("DELETE FROM test_delete WHERE EXISTS(SELECT 1)", "SELECT count(*) - 1 FROM orders");
        assertUpdate("DROP TABLE test_delete");

        // test EXPLAIN ANALYZE with CTAS
        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE analyze_test AS SELECT CAST(orderstatus AS VARCHAR(15)) orderstatus FROM orders");
        assertQuery("SELECT * from analyze_test", "SELECT orderstatus FROM orders");
        // check that INSERT works also
        assertExplainAnalyze("EXPLAIN ANALYZE INSERT INTO analyze_test SELECT clerk FROM orders");
        assertQuery("SELECT * from analyze_test", "SELECT orderstatus FROM orders UNION ALL SELECT clerk FROM orders");
        // check DELETE works with EXPLAIN ANALYZE
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM analyze_test WHERE TRUE");
        assertQuery("SELECT COUNT(*) from analyze_test", "SELECT 0");
        assertUpdate("DROP TABLE analyze_test");
    }

    @Test
    public void testDropTableIfExists()
    {
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
        assertUpdate("DROP TABLE IF EXISTS test_drop_if_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
    }

    @Test
    public void testView()
    {
        skipTestUnless(supportsViews());

        @Language("SQL") String query = "SELECT orderkey, orderstatus, totalprice / 2 half FROM orders";

        assertUpdate("CREATE VIEW test_view AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW test_view AS " + query);

        assertQuery("SELECT * FROM test_view", query);

        assertQuery(
                "SELECT * FROM test_view a JOIN test_view b on a.orderkey = b.orderkey",
                format("SELECT * FROM (%s) a JOIN (%s) b ON a.orderkey = b.orderkey", query, query));

        assertQuery("WITH orders AS (SELECT * FROM orders LIMIT 0) SELECT * FROM test_view", query);

        String name = format("%s.%s.test_view", getSession().getCatalog().get(), getSession().getSchema().get());
        assertQuery("SELECT * FROM " + name, query);

        assertUpdate("DROP VIEW test_view");
    }

    @Test
    public void testGroupingSets()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupByArray()
    {
        assertQuery("SELECT col[1], count FROM (SELECT ARRAY[custkey] col, COUNT(*) count FROM ORDERS GROUP BY 1 ORDER BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey ORDER BY custkey");
    }

    @Test
    public void testGroupByMap()
    {
        assertQuery("SELECT col[1], count FROM (SELECT MAP(ARRAY[1], ARRAY[custkey]) col, COUNT(*) count FROM ORDERS GROUP BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testGroupByComplexMap()
    {
        assertQuery("SELECT MAP_KEYS(x)[1] FROM (VALUES MAP(ARRAY['a'], ARRAY[ARRAY[1]]), MAP(ARRAY['b'], ARRAY[ARRAY[2]])) t(x) GROUP BY x", "SELECT * FROM (VALUES 'a', 'b')");
    }

    @Test
    public void testGroupByRow()
    {
        assertQuery("SELECT col.col1, count FROM (SELECT cast(row(custkey, custkey) as row(col0 bigint, col1 bigint)) col, COUNT(*) count FROM ORDERS GROUP BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testViewCaseSensitivity()
    {
        skipTestUnless(supportsViews());

        computeActual("CREATE VIEW test_view_uppercase AS SELECT X FROM (SELECT 123 X)");
        computeActual("CREATE VIEW test_view_mixedcase AS SELECT XyZ FROM (SELECT 456 XyZ)");
        assertQuery("SELECT * FROM test_view_uppercase", "SELECT X FROM (SELECT 123 X)");
        assertQuery("SELECT * FROM test_view_mixedcase", "SELECT XyZ FROM (SELECT 456 XyZ)");
    }

    @Test
    public void testCompatibleTypeChangeForView()
    {
        skipTestUnless(supportsViews());

        assertUpdate("CREATE TABLE test_table_1 AS SELECT 'abcdefg' a", 1);
        assertUpdate("CREATE VIEW test_view_1 AS SELECT a FROM test_table_1");

        assertQuery("SELECT * FROM test_view_1", "VALUES 'abcdefg'");

        // replace table with a version that's implicitly coercible to the previous one
        assertUpdate("DROP TABLE test_table_1");
        assertUpdate("CREATE TABLE test_table_1 AS SELECT 'abc' a", 1);

        assertQuery("SELECT * FROM test_view_1", "VALUES 'abc'");

        assertUpdate("DROP VIEW test_view_1");
        assertUpdate("DROP TABLE test_table_1");
    }

    @Test
    public void testCompatibleTypeChangeForView2()
    {
        skipTestUnless(supportsViews());

        assertUpdate("CREATE TABLE test_table_2 AS SELECT BIGINT '1' v", 1);
        assertUpdate("CREATE VIEW test_view_2 AS SELECT * FROM test_table_2");

        assertQuery("SELECT * FROM test_view_2", "VALUES 1");

        // replace table with a version that's implicitly coercible to the previous one
        assertUpdate("DROP TABLE test_table_2");
        assertUpdate("CREATE TABLE test_table_2 AS SELECT INTEGER '1' v", 1);

        assertQuery("SELECT * FROM test_view_2 WHERE v = 1", "VALUES 1");

        assertUpdate("DROP VIEW test_view_2");
        assertUpdate("DROP TABLE test_table_2");
    }

    @Test
    public void testGroupingInWindowFunction()
    {
        assertQuery(
                "SELECT orderkey, custkey, sum(totalprice), grouping(orderkey)+grouping(custkey) as g, " +
                        "       rank() OVER (PARTITION BY grouping(orderkey)+grouping(custkey), " +
                        "       CASE WHEN grouping(orderkey) = 0 THEN custkey END ORDER BY orderkey ASC) as r " +
                        "FROM orders " +
                        "GROUP BY ROLLUP (orderkey, custkey) " +
                        "ORDER BY orderkey, custkey " +
                        "LIMIT 10",
                "VALUES (1, 370, 172799.49, 0, 1), " +
                        "       (1, NULL, 172799.49, 1, 1), " +
                        "       (2, 781, 38426.09, 0, 1), " +
                        "       (2, NULL, 38426.09, 1, 2), " +
                        "       (3, 1234, 205654.30, 0, 1), " +
                        "       (3, NULL, 205654.30, 1, 3), " +
                        "       (4, 1369, 56000.91, 0, 1), " +
                        "       (4, NULL, 56000.91, 1, 4), " +
                        "       (5, 445, 105367.67, 0, 1), " +
                        "       (5, NULL, 105367.67, 1, 5)");
    }

    @Test
    public void testIntersect()
    {
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT select regionkey FROM nation WHERE nationkey > 21");
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT DISTINCT SELECT regionkey FROM nation WHERE nationkey > 21",
                "VALUES 1, 3");
        assertQuery(
                "WITH wnation AS (SELECT nationkey, regionkey FROM nation) " +
                        "SELECT regionkey FROM wnation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM wnation WHERE nationkey > 21", "VALUES 1, 3");
        assertQuery(
                "SELECT num FROM (SELECT 1 as num FROM nation WHERE nationkey=10 " +
                        "INTERSECT SELECT 1 FROM nation WHERE nationkey=20) T");
        assertQuery(
                "SELECT nationkey, nationkey / 2 FROM (SELECT nationkey FROM nation WHERE nationkey < 10 " +
                        "INTERSECT SELECT nationkey FROM nation WHERE nationkey > 4) T WHERE nationkey % 2 = 0");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION SELECT 4");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "UNION SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "INTERSECT SELECT 1");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION ALL SELECT 3");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION ALL SELECT 3");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) " +
                        "INTERSECT SELECT * FROM (VALUES 1.0, 2)",
                "VALUES 1.0, 2.0");
        assertQuery("SELECT NULL, NULL INTERSECT SELECT NULL, NULL FROM nation");

        MaterializedResult emptyResult = computeActual("SELECT 100 INTERSECT (SELECT regionkey FROM nation WHERE nationkey <10)");
        assertEquals(emptyResult.getMaterializedRows().size(), 0);
    }

    @Test
    public void testIntersectWithAggregation()
    {
        assertQuery("SELECT COUNT(*) FROM nation INTERSECT SELECT COUNT(regionkey) FROM nation HAVING SUM(regionkey) IS NOT NULL");
        assertQuery("SELECT SUM(nationkey), COUNT(name) FROM (SELECT nationkey,name FROM nation INTERSECT SELECT regionkey, name FROM nation) n");
        assertQuery("SELECT COUNT(*) * 2 FROM nation INTERSECT (SELECT SUM(nationkey) FROM nation GROUP BY regionkey ORDER BY 1 LIMIT 2)");
        assertQuery("SELECT COUNT(a) FROM (SELECT nationkey AS a FROM (SELECT nationkey FROM nation INTERSECT SELECT regionkey FROM nation) n1 INTERSECT SELECT regionkey FROM nation) n2");
        assertQuery("SELECT COUNT(*), SUM(2), regionkey FROM (SELECT nationkey, regionkey FROM nation INTERSECT SELECT regionkey, regionkey FROM nation) n GROUP BY regionkey");
        assertQuery("SELECT COUNT(*) FROM (SELECT nationkey FROM nation INTERSECT SELECT 2) n1 INTERSECT SELECT regionkey FROM nation");
    }

    @Test
    public void testIntersectAllFails()
    {
        assertQueryFails("SELECT * FROM (VALUES 1, 2, 3, 4) INTERSECT ALL SELECT * FROM (VALUES 3, 4)", "line 1:35: INTERSECT ALL not yet implemented");
    }

    @Test
    public void testExcept()
    {
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT select regionkey FROM nation WHERE nationkey > 21");
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT DISTINCT SELECT regionkey FROM nation WHERE nationkey > 21",
                "VALUES 0, 4");
        assertQuery(
                "WITH wnation AS (SELECT nationkey, regionkey FROM nation) " +
                        "SELECT regionkey FROM wnation WHERE nationkey < 7 " +
                        "EXCEPT SELECT regionkey FROM wnation WHERE nationkey > 21",
                "VALUES 0, 4");
        assertQuery(
                "SELECT num FROM (SELECT 1 as num FROM nation WHERE nationkey=10 " +
                        "EXCEPT SELECT 2 FROM nation WHERE nationkey=20) T");
        assertQuery(
                "SELECT nationkey, nationkey / 2 FROM (SELECT nationkey FROM nation WHERE nationkey < 10 " +
                        "EXCEPT SELECT nationkey FROM nation WHERE nationkey > 4) T WHERE nationkey % 2 = 0");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION SELECT 3");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "UNION SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "EXCEPT SELECT 1");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION ALL SELECT 4");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) " +
                        "EXCEPT SELECT * FROM (VALUES 3.0, 2)");
        assertQuery("SELECT NULL, NULL EXCEPT SELECT NULL, NULL FROM nation");

        assertQuery(
                "(SELECT * FROM (VALUES 1) EXCEPT SELECT * FROM (VALUES 0))" +
                        "EXCEPT (SELECT * FROM (VALUES 1) EXCEPT SELECT * FROM (VALUES 1))");

        MaterializedResult emptyResult = computeActual("SELECT 0 EXCEPT (SELECT regionkey FROM nation WHERE nationkey <10)");
        assertEquals(emptyResult.getMaterializedRows().size(), 0);
    }

    @Test
    public void testExceptWithAggregation()
    {
        assertQuery("SELECT COUNT(*) FROM nation EXCEPT SELECT COUNT(regionkey) FROM nation where regionkey < 3 HAVING SUM(regionkey) IS NOT NULL");
        assertQuery("SELECT SUM(nationkey), COUNT(name) FROM (SELECT nationkey, name FROM nation where nationkey < 6 EXCEPT SELECT regionkey, name FROM nation) n");
        assertQuery("(SELECT SUM(nationkey) FROM nation GROUP BY regionkey ORDER BY 1 LIMIT 2) EXCEPT SELECT COUNT(*) * 2 FROM nation");
        assertQuery("SELECT COUNT(a) FROM (SELECT nationkey AS a FROM (SELECT nationkey FROM nation EXCEPT SELECT regionkey FROM nation) n1 EXCEPT SELECT regionkey FROM nation) n2");
        assertQuery("SELECT COUNT(*), SUM(2), regionkey FROM (SELECT nationkey, regionkey FROM nation EXCEPT SELECT regionkey, regionkey FROM nation) n GROUP BY regionkey HAVING regionkey < 3");
        assertQuery("SELECT COUNT(*) FROM (SELECT nationkey FROM nation EXCEPT SELECT 10) n1 EXCEPT SELECT regionkey FROM nation");
    }

    @Test
    public void testJoinWithLessThanInJoinClause()
    {
        assertQuery("SELECT n.nationkey, r.regionkey FROM region r JOIN nation n ON n.regionkey = r.regionkey AND n.name < r.name");
        assertQuery("SELECT l.suppkey, n.nationkey, l.partkey, n.regionkey FROM nation n JOIN lineitem l ON l.suppkey = n.nationkey AND l.partkey < n.regionkey");
        // test with single null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, CAST(-1 AS BIGINT)), (0, NULL), (0, CAST(0 AS BIGINT))) t(a, b) WHERE n.regionkey - 100 < t.b AND n.nationkey = t.a",
                "VALUES -1, 0");
        // test with single (first) null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL), (0, CAST(-1 AS BIGINT)), (0, CAST(0 AS BIGINT))) t(a, b) WHERE n.regionkey - 100 < t.b AND n.nationkey = t.a",
                "VALUES -1, 0");
        // test with multiple null values in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL), (0, NULL), (0, CAST(-1 AS BIGINT)), (0, NULL)) t(a, b) WHERE n.regionkey - 100 < t.b AND n.nationkey = t.a",
                "VALUES -1");
        // test with only null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL)) t(a, b) WHERE n.regionkey - 100 < t.b AND n.nationkey = t.a", "SELECT 1 WHERE FALSE");
        // test with function predicate in ON clause
        assertQuery("SELECT n.nationkey, r.regionkey FROM nation n JOIN region r ON n.regionkey = r.regionkey AND length(n.name) < length(substr(r.name, 5))");

        assertQuery("SELECT * FROM " +
                        "(VALUES (1,1),(2,1)) t1(a,b), " +
                        "(VALUES (1,1),(1,2),(2,1)) t2(x,y) " +
                        "WHERE a=x and b<=y",
                "VALUES (1,1,1,1), (1,1,1,2), (2,1,2,1)");

        assertQuery("SELECT * FROM " +
                        "(VALUES (1,1),(2,1)) t1(a,b), " +
                        "(VALUES (1,1),(1,2),(2,1)) t2(x,y) " +
                        "WHERE a=x and b<y",
                "VALUES (1,1,1,2)");
    }

    @Test
    public void testJoinWithGreaterThanInJoinClause()
    {
        assertQuery("SELECT n.nationkey, r.regionkey FROM region r JOIN nation n ON n.regionkey = r.regionkey AND n.name > r.name AND r.regionkey = 0");
        assertQuery("SELECT l.suppkey, n.nationkey, l.partkey, n.regionkey FROM nation n JOIN lineitem l ON l.suppkey = n.nationkey AND l.partkey > n.regionkey");
        // test with single null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, CAST(-1 AS BIGINT)), (0, NULL), (0, CAST(0 AS BIGINT))) t(a, b) WHERE n.regionkey + 100 > t.b AND n.nationkey = t.a",
                "VALUES -1, 0");
        // test with single (first) null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL), (0, CAST(-1 AS BIGINT)), (0, CAST(0 AS BIGINT))) t(a, b) WHERE n.regionkey + 100 > t.b AND n.nationkey = t.a",
                "VALUES -1, 0");
        // test with multiple null values in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL), (0, NULL), (0, CAST(-1 AS BIGINT)), (0, NULL)) t(a, b) WHERE n.regionkey + 100 > t.b AND n.nationkey = t.a",
                "VALUES -1");
        // test with only null value in build side
        assertQuery("SELECT b FROM nation n, (VALUES (0, NULL)) t(a, b) WHERE n.regionkey + 100 > t.b AND n.nationkey = t.a", "SELECT 1 WHERE FALSE");
        /// test with function predicate in ON clause
        assertQuery("SELECT n.nationkey, r.regionkey FROM nation n JOIN region r ON n.regionkey = r.regionkey AND length(n.name) > length(substr(r.name, 5))");

        assertQuery("SELECT * FROM " +
                        "(VALUES (1,1),(2,1)) t1(a,b), " +
                        "(VALUES (1,1),(1,2),(2,1)) t2(x,y) " +
                        "WHERE a=x and b>=y",
                "VALUES (1,1,1,1), (2,1,2,1)");

        assertQuery("SELECT * FROM " +
                        "(VALUES (1,1),(2,1)) t1(a,b), " +
                        "(VALUES (1,1),(1,2),(2,1)) t2(x,y) " +
                        "WHERE a=x and b>y",
                "SELECT 1 WHERE FALSE");
    }

    @Test
    public void testJoinWithRangePredicatesinJoinClause()
    {
        assertQuery("SELECT COUNT(*) " +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem " +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders " +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0 " +
                "AND orders.custkey % 8 < 7 AND lineitem.suppkey % 10 < orders.custkey % 7 AND lineitem.suppkey % 7 > orders.custkey % 7");

        assertQuery("SELECT COUNT(*) " +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem " +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders " +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0 " +
                "AND orders.custkey % 8 < lineitem.linenumber % 2 AND lineitem.suppkey % 10 < orders.custkey % 7 AND lineitem.suppkey % 7 > orders.custkey % 7");
    }

    @Test
    public void testJoinWithMultipleLessThanPredicatesDifferentOrders()
    {
        // test that fast inequality join is not sensitive to order of search conjuncts.
        assertQuery("SELECT count(*) FROM lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 < n.regionkey AND l.partkey % 3 + 1 < n.regionkey AND l.partkey % 3 + 2 < n.regionkey");
        assertQuery("SELECT count(*) FROM lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 + 2 < n.regionkey AND l.partkey % 3 + 1 < n.regionkey AND l.partkey % 3 < n.regionkey");
        assertQuery("SELECT count(*) FROM lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 > n.regionkey AND l.partkey % 3 + 1 > n.regionkey AND l.partkey % 3 + 2 > n.regionkey");
        assertQuery("SELECT count(*) FROM lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 + 2 > n.regionkey AND l.partkey % 3 + 1 > n.regionkey AND l.partkey % 3 > n.regionkey");
    }

    @Test
    public void testJoinWithLessThanOnDatesInJoinClause()
    {
        assertQuery(
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN lineitem l ON l.orderkey = o.orderkey AND l.shipdate < o.orderdate + INTERVAL '10' DAY",
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN lineitem l ON l.orderkey = o.orderkey AND l.shipdate < DATEADD('DAY', 10, o.orderdate)");
        assertQuery(
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM lineitem l JOIN orders o ON l.orderkey = o.orderkey AND l.shipdate < DATE_ADD('DAY', 10, o.orderdate)",
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN lineitem l ON l.orderkey = o.orderkey AND l.shipdate < DATEADD('DAY', 10, o.orderdate)");
        assertQuery(
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN lineitem l ON o.orderkey=l.orderkey AND o.orderdate + INTERVAL '2' DAY <= l.shipdate AND l.shipdate < o.orderdate + INTERVAL '7' DAY",
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN lineitem l ON o.orderkey=l.orderkey AND DATEADD('DAY', 2, o.orderdate) <= l.shipdate AND l.shipdate < DATEADD('DAY', 7, o.orderdate)");
    }

    @Test
    public void testJoinWithNonDeterministicLessThan()
    {
        MaterializedRow actualRow = getOnlyElement(computeActual(
                "SELECT count(*) FROM " +
                        "customer c1 JOIN customer c2 ON c1.nationkey=c2.nationkey " +
                        "WHERE c1.custkey - RANDOM(CAST(c1.custkey AS BIGINT)) < c2.custkey").getMaterializedRows());
        assertEquals(actualRow.getFieldCount(), 1);
        long actualCount = (Long) actualRow.getField(0); // this should be around ~69000

        MaterializedRow expectedAtLeastRow = getOnlyElement(computeActual(
                "SELECT count(*) FROM " +
                        "customer c1 JOIN customer c2 ON c1.nationkey=c2.nationkey " +
                        "WHERE c1.custkey < c2.custkey").getMaterializedRows());
        assertEquals(expectedAtLeastRow.getFieldCount(), 1);
        long expectedAtLeastCount = (Long) expectedAtLeastRow.getField(0); // this is exactly 45022

        // Technically non-deterministic unit test but has hopefully a next to impossible chance of a false positive
        assertTrue(actualCount > expectedAtLeastCount);
    }

    @Test
    public void testSimpleJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("" +
                "SELECT COUNT(*) FROM " +
                "(SELECT orderkey FROM lineitem WHERE orderkey < 1000) a " +
                "JOIN " +
                "(SELECT orderkey FROM orders WHERE orderkey < 2000) b " +
                "ON NOT (a.orderkey <= b.orderkey)");
    }

    @Test
    public void testJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = 2");
    }

    @Test
    public void testJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testJoinDoubleClauseWithLeftOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testJoinDoubleClauseWithRightOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testViewMetadata()
    {
        skipTestUnless(supportsViews());

        @Language("SQL") String query = "SELECT BIGINT '123' x, 'foo' y";
        assertUpdate("CREATE VIEW meta_test_view AS " + query);

        // test INFORMATION_SCHEMA.TABLES
        MaterializedResult actual = computeActual(format(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("customer", "BASE TABLE")
                .row("lineitem", "BASE TABLE")
                .row("meta_test_view", "VIEW")
                .row("nation", "BASE TABLE")
                .row("orders", "BASE TABLE")
                .row("part", "BASE TABLE")
                .row("partsupp", "BASE TABLE")
                .row("region", "BASE TABLE")
                .row("supplier", "BASE TABLE")
                .build();

        assertContains(actual, expected);

        // test SHOW TABLES
        actual = computeActual("SHOW TABLES");

        MaterializedResult.Builder builder = resultBuilder(getSession(), actual.getTypes());
        for (MaterializedRow row : expected.getMaterializedRows()) {
            builder.row(row.getField(0));
        }
        expected = builder.build();

        assertContains(actual, expected);

        // test INFORMATION_SCHEMA.VIEWS
        actual = computeActual(format(
                "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        expected = resultBuilder(getSession(), actual.getTypes())
                .row("meta_test_view", formatSqlText(query))
                .build();

        assertContains(actual, expected);

        // test SHOW COLUMNS
        actual = computeActual("SHOW COLUMNS FROM meta_test_view");

        expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("x", "bigint", "", "")
                .row("y", "varchar(3)", "", "")
                .build();

        assertEquals(actual, expected);

        // test SHOW CREATE VIEW
        String expectedSql = formatSqlText(format(
                "CREATE VIEW %s.%s.%s AS %s",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "meta_test_view",
                query)).trim();

        actual = computeActual("SHOW CREATE VIEW meta_test_view");

        assertEquals(getOnlyElement(actual.getOnlyColumnAsSet()), expectedSql);

        assertUpdate("DROP VIEW meta_test_view");
    }

    @Test
    public void testQueryLoggingCount()
    {
        QueryManager queryManager = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getQueryManager();
        executeExclusively(() -> {
            assertUntilTimeout(
                    () -> assertEquals(
                            queryManager.getAllQueryInfo()
                                    .stream()
                                    .filter(info -> !info.isFinalQueryInfo())
                                    .collect(toList()),
                            ImmutableList.of()),
                    new Duration(1, MINUTES));

            // We cannot simply get the number of completed queries as soon as all the queries are completed, because this counter may not be up-to-date at that point.
            // The completed queries counter is updated in a final query info listener, which is called eventually.
            // Therefore, here we wait until the value of this counter gets stable.

            long beforeCompletedQueriesCount = waitUntilStable(() -> queryManager.getStats().getCompletedQueries().getTotalCount(), new Duration(5, SECONDS));
            long beforeSubmittedQueriesCount = queryManager.getStats().getSubmittedQueries().getTotalCount();
            assertUpdate("CREATE TABLE test_query_logging_count AS SELECT 1 foo_1, 2 foo_2_4", 1);
            assertQuery("SELECT foo_1, foo_2_4 FROM test_query_logging_count", "SELECT 1, 2");
            assertUpdate("DROP TABLE test_query_logging_count");
            assertQueryFails("SELECT * FROM test_query_logging_count", ".*Table .* does not exist");

            // TODO: Figure out a better way of synchronization
            assertUntilTimeout(
                    () -> assertEquals(queryManager.getStats().getCompletedQueries().getTotalCount() - beforeCompletedQueriesCount, 4),
                    new Duration(1, MINUTES));
            assertEquals(queryManager.getStats().getSubmittedQueries().getTotalCount() - beforeSubmittedQueriesCount, 4);
        });
    }

    private <T> T waitUntilStable(Supplier<T> computation, Duration timeout)
    {
        T lastValue = computation.get();
        long start = System.nanoTime();
        while (!currentThread().isInterrupted() && nanosSince(start).compareTo(timeout) < 0) {
            sleepUninterruptibly(100, MILLISECONDS);
            T currentValue = computation.get();
            if (currentValue.equals(lastValue)) {
                return currentValue;
            }
            lastValue = currentValue;
        }
        throw new UncheckedTimeoutException();
    }

    private static void assertUntilTimeout(Runnable assertion, Duration timeout)
    {
        long start = System.nanoTime();
        while (!currentThread().isInterrupted()) {
            try {
                assertion.run();
                return;
            }
            catch (AssertionError e) {
                if (nanosSince(start).compareTo(timeout) > 0) {
                    throw e;
                }
            }
            sleepUninterruptibly(50, MILLISECONDS);
        }
    }

    @Test
    public void testLargeQuerySuccess()
    {
        assertQuery("SELECT " + Joiner.on(" AND ").join(nCopies(500, "1 = 1")), "SELECT true");
    }

    @Test
    public void testShowSchemasFromOther()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM tpch");
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(INFORMATION_SCHEMA, "tiny", "sf1")));
    }

    @Test
    public void testTableSampleSystemBoundaryValues()
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (0)");
        MaterializedResult all = computeActual("SELECT orderkey FROM orders");

        assertContains(all, fullSample);
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }

    @Test
    public void testSymbolAliasing()
    {
        assertUpdate("CREATE TABLE test_symbol_aliasing AS SELECT 1 foo_1, 2 foo_2_4", 1);
        assertQuery("SELECT foo_1, foo_2_4 FROM test_symbol_aliasing", "SELECT 1, 2");
        assertUpdate("DROP TABLE test_symbol_aliasing");
    }

    @Test
    public void testNonQueryAccessControl()
    {
        skipTestUnless(supportsViews());

        assertAccessDenied("SET SESSION " + QUERY_MAX_MEMORY + " = '10MB'",
                "Cannot set system session property " + QUERY_MAX_MEMORY,
                privilege(QUERY_MAX_MEMORY, SET_SESSION));

        assertAccessDenied("CREATE TABLE foo (pk bigint)", "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
        assertAccessDenied("DROP TABLE orders", "Cannot drop table .*.orders.*", privilege("orders", DROP_TABLE));
        assertAccessDenied("ALTER TABLE orders RENAME TO foo", "Cannot rename table .*.orders.* to .*.foo.*", privilege("orders", RENAME_TABLE));
        assertAccessDenied("ALTER TABLE orders ADD COLUMN foo bigint", "Cannot add a column to table .*.orders.*", privilege("orders", ADD_COLUMN));
        assertAccessDenied("ALTER TABLE orders DROP COLUMN foo", "Cannot drop a column from table .*.orders.*", privilege("orders", DROP_COLUMN));
        assertAccessDenied("ALTER TABLE orders RENAME COLUMN orderkey TO foo", "Cannot rename a column in table .*.orders.*", privilege("orders", RENAME_COLUMN));
        assertAccessDenied("CREATE VIEW foo as SELECT * FROM orders", "Cannot create view .*.foo.*", privilege("foo", CREATE_VIEW));
        // todo add DROP VIEW test... not all connectors have view support

        try {
            assertAccessDenied("SELECT 1", "Principal .* cannot become user " + getSession().getUser() + ".*", privilege(getSession().getUser(), SET_USER));
        }
        catch (AssertionError e) {
            // There is no clean exception message for authorization failure.  We simply get a 403
            Assertions.assertContains(e.getMessage(), "statusCode=403");
        }
    }

    @Test
    public void testViewAccessControl()
    {
        skipTestUnless(supportsViews());

        Session viewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(new Identity("test_view_access_owner", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        // verify creation of view over a table requires special view creation privileges for the table
        assertAccessDenied(
                viewOwnerSession,
                "CREATE VIEW test_view_access AS SELECT * FROM orders",
                "Cannot select from table .*.orders.*",
                privilege("orders", CREATE_VIEW_WITH_SELECT_TABLE));

        // create the view
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW test_view_access AS SELECT * FROM orders",
                privilege("bogus", "bogus privilege to disable security", SELECT_TABLE));

        // verify selecting from a view over a table requires the view owner to have special view creation privileges for the table
        assertAccessDenied(
                "SELECT * FROM test_view_access",
                "Cannot select from table .*.orders.*",
                privilege(viewOwnerSession.getUser(), "orders", CREATE_VIEW_WITH_SELECT_TABLE));

        // verify selecting from a view over a table does not require the session user to have SELECT privileges on the underlying table
        assertAccessAllowed(
                "SELECT * FROM test_view_access",
                privilege(getSession().getUser(), "orders", CREATE_VIEW_WITH_SELECT_TABLE));
        assertAccessAllowed(
                "SELECT * FROM test_view_access",
                privilege(getSession().getUser(), "orders", SELECT_TABLE));

        Session nestedViewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(new Identity("test_nested_view_access_owner", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        // verify creation of view over a view requires special view creation privileges for the view
        assertAccessDenied(
                nestedViewOwnerSession,
                "CREATE VIEW test_nested_view_access AS SELECT * FROM test_view_access",
                "Cannot select from view .*.test_view_access.*",
                privilege("test_view_access", CREATE_VIEW_WITH_SELECT_VIEW));

        // create the nested view
        assertAccessAllowed(
                nestedViewOwnerSession,
                "CREATE VIEW test_nested_view_access AS SELECT * FROM test_view_access",
                privilege("bogus", "bogus privilege to disable security", SELECT_TABLE));

        // verify selecting from a view over a view requires the view owner of the outer view to have special view creation privileges for the inner view
        assertAccessDenied(
                "SELECT * FROM test_nested_view_access",
                "Cannot select from view .*.test_view_access.*",
                privilege(nestedViewOwnerSession.getUser(), "test_view_access", CREATE_VIEW_WITH_SELECT_VIEW));

        // verify selecting from a view over a view does not require the session user to have SELECT privileges for the inner view
        assertAccessAllowed(
                "SELECT * FROM test_nested_view_access",
                privilege(getSession().getUser(), "test_view_access", CREATE_VIEW_WITH_SELECT_VIEW));
        assertAccessAllowed(
                "SELECT * FROM test_nested_view_access",
                privilege(getSession().getUser(), "test_view_access", SELECT_VIEW));

        assertAccessAllowed(nestedViewOwnerSession, "DROP VIEW test_nested_view_access");
        assertAccessAllowed(viewOwnerSession, "DROP VIEW test_view_access");
    }

    @Test
    public void testJoinWithStatefulFilterFunction()
    {
        // Stateful function is placed in LEFT JOIN's ON clause and involves left & right symbols to prevent any kind of push down/pull down.
        Session session = Session.builder(getSession())
                // With broadcast join, lineitem would be source-distributed and not executed concurrently.
                .setSystemProperty(SystemSessionProperties.DISTRIBUTED_JOIN, "true")
                .build();
        long joinOutputRowCount = 60175;
        assertQuery(
                session,
                format(
                        "SELECT count(*) FROM lineitem l LEFT OUTER JOIN orders o ON l.orderkey = o.orderkey AND stateful_sleeping_sum(%s, 100, l.linenumber, o.shippriority) > 0",
                        10 * 1. / joinOutputRowCount),
                format("VALUES %s", joinOutputRowCount));
    }

    @Test
    public void testWrittenStats()
    {
        String sql = "CREATE TABLE test_written_stats AS SELECT * FROM nation";
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), sql);
        QueryInfo queryInfo = distributedQueryRunner.getQueryInfo(resultResultWithQueryId.getQueryId());

        assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
        assertEquals(queryInfo.getQueryStats().getWrittenPositions(), 25L);
        assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);

        sql = "INSERT INTO test_written_stats SELECT * FROM nation LIMIT 10";
        resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), sql);
        queryInfo = distributedQueryRunner.getQueryInfo(resultResultWithQueryId.getQueryId());

        assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
        assertEquals(queryInfo.getQueryStats().getWrittenPositions(), 10L);
        assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);

        assertUpdate("DROP TABLE test_written_stats");
    }

    @Test
    public void testComplexCast()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_DISTINCT_AGGREGATIONS, "true")
                .build();
        // This is optimized using CAST(null AS interval day to second) which may be problematic to deserialize on worker
        assertQuery(session, "WITH t(a, b) AS (VALUES (1, INTERVAL '1' SECOND)) " +
                        "SELECT count(DISTINCT a), CAST(max(b) AS VARCHAR) FROM t",
                "VALUES (1, '0 00:00:01.000')");
    }
}
