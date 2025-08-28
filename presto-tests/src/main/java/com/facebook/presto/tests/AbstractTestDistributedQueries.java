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

import com.facebook.airlift.testing.Assertions;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.planner.planPrinter.JsonRenderer;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.TestingSession;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.airlift.units.Duration.nanosSince;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_PAYLOAD_JOINS;
import static com.facebook.presto.SystemSessionProperties.PULL_EXPRESSION_FROM_LAMBDA_ENABLED;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.SystemSessionProperties.REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN;
import static com.facebook.presto.SystemSessionProperties.SHARDED_JOINS_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_OPTIMIZER_INFO_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.UuidType.UUID;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.sql.tree.CreateView.Security.INVOKER;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.ADD_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_COLUMNS;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SET_SESSION;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SET_USER;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public abstract class AbstractTestDistributedQueries
        extends AbstractTestQueries
{
    protected boolean supportsViews()
    {
        return true;
    }

    protected boolean supportsNotNullColumns()
    {
        return true;
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
    public void testSubfieldAccessControl()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("check_access_control_with_subfields", "true")
                .build();
        assertUpdate(
                session,
                "CREATE TABLE test_subfield AS SELECT CAST(ROW(1, 2, ARRAY[ROW(1, 2)]) AS ROW(f1 int, f2 int, f3 ARRAY<ROW(ff1 int, ff2 int)>)) x",
                1);
        assertAccessAllowed(session, "SELECT x.f1 from test_subfield");
        assertAccessAllowed(session, "SELECT x.f1 from test_subfield", privilege("x.f2", SELECT_COLUMN));
        assertAccessDenied(
                session,
                "SELECT x.f1 from test_subfield",
                ".*Cannot select from columns \\[x.f1\\].*",
                privilege("x.f1", SELECT_COLUMN));

        assertAccessDenied(session,
                "SELECT transform(x.f3, col -> col.ff1) from test_subfield",
                ".*Cannot select from columns \\[x.f3.ff1\\].*", privilege("x.f3.ff1", SELECT_COLUMN));
        assertAccessAllowed(session,
                "SELECT transform(x.f3, col -> col.ff1) from test_subfield",
                privilege("x.f3.ff2", SELECT_COLUMN));

        assertAccessAllowed(session,
                "SELECT cardinality(x.f3) from test_subfield",
                privilege("x.f3", SELECT_COLUMN));

        assertAccessAllowed(session,
                "SELECT x.f3 IS NULL from test_subfield",
                privilege("x.f3", SELECT_COLUMN));

        assertAccessAllowed(session,
                "SELECT x.f3 IS NOT NULL from test_subfield",
                privilege("x.f3", SELECT_COLUMN));

        assertAccessAllowed(session,
                "SELECT x.f3 IS NULL from test_subfield",
                privilege("x", SELECT_COLUMN));
        assertAccessAllowed(session,
                "SELECT x.f3 IS NOT NULL from test_subfield",
                privilege("x", SELECT_COLUMN));

        assertAccessAllowed(session,
                "SELECT x IS NULL from test_subfield",
                privilege("x", SELECT_COLUMN));
        assertAccessAllowed(session,
                "SELECT x IS NOT NULL from test_subfield",
                privilege("x", SELECT_COLUMN));

        assertUpdate("DROP TABLE test_subfield");
    }

    @Test
    public void testCreateTable()
    {
        assertUpdate("CREATE TABLE test_create (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create"));
        assertTableColumnNames("test_create", "a", "b", "c");

        assertUpdate("DROP TABLE test_create");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create"));

        assertQueryFails("CREATE TABLE test_create (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
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
    public void testNonAutoCommitTransactionWithRollback()
    {
        assertUpdate("create table multi_statements_transaction_rollback(a int, b varchar)");
        assertUpdate("insert into multi_statements_transaction_rollback values(1, '1001'), (2, '1002')", 2);

        Session session = assertStartTransaction(getSession(), "start transaction");

        assertQuery(session, "select * from multi_statements_transaction_rollback", "values(1, '1001'), (2, '1002')");
        assertUpdate(session, "insert into multi_statements_transaction_rollback values(3, '1003'), (4, '1004')", 2);

        // `Rollback` executes successfully, and the client gets a flag `clearTransactionId = true`
        session = assertEndTransaction(session, "rollback");

        assertQuery(session, "select * from multi_statements_transaction_rollback", "values(1, '1001'), (2, '1002')");
        assertUpdate(session, "drop table if exists multi_statements_transaction_rollback");
    }

    @Test
    public void testNonAutoCommitTransactionWithCommit()
    {
        assertUpdate("create table multi_statements_transaction_commit(a int, b varchar)");
        assertUpdate("insert into multi_statements_transaction_commit values(1, '1001'), (2, '1002')", 2);
        Session session = assertStartTransaction(getSession(), "start transaction");

        assertQuery(session, "select * from multi_statements_transaction_commit", "values(1, '1001'), (2, '1002')");
        assertUpdate(session, "insert into multi_statements_transaction_commit values(3, '1003'), (4, '1004')", 2);

        // `Commit` executes successfully, and the client gets a flag `clearTransactionId = true`
        session = assertEndTransaction(session, "commit");

        assertQuery(session, "select * from multi_statements_transaction_commit",
                "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
        assertUpdate(session, "drop table if exists multi_statements_transaction_commit");
    }

    @Test
    public void testNonAutoCommitTransactionWithFailAndRollback()
    {
        assertUpdate("create table test_non_autocommit_table(a int, b varchar)");
        Session session = Session.builder(getSession())
                .setIdentity(new Identity("admin",
                        Optional.empty(),
                        ImmutableMap.of(getSession().getCatalog().get(), new SelectedRole(ROLE, Optional.of("admin"))),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .build();

        session = assertStartTransaction(session, "start transaction");

        // simulate failure of SQL statement execution
        assertQueryFails(session, "SELECT fail('forced failure')", "forced failure");

        // cannot execute any SQLs except `rollback` in current session
        assertQueryFails(session, "select count(*) from test_non_autocommit_table", "Current transaction is aborted, commands ignored until end of transaction block");
        assertQueryFails(session, "show tables", "Current transaction is aborted, commands ignored until end of transaction block");
        assertQueryFails(session, "insert into test_non_autocommit_table values(1, '1001')", "Current transaction is aborted, commands ignored until end of transaction block");
        assertQueryFails(session, "create table test_table(a int, b varchar)", "Current transaction is aborted, commands ignored until end of transaction block");

        // execute `rollback` successfully
        session = assertEndTransaction(session, "rollback");

        assertQuery(session, "select count(*) from test_non_autocommit_table", "values(0)");
        assertUpdate(session, "drop table if exists test_non_autocommit_table");
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_ctas AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames("test_ctas", "name", "regionkey");
        assertUpdate("DROP TABLE test_ctas");

        // Some connectors support CREATE TABLE AS but not the ordinary CREATE TABLE. Let's test CTAS IF NOT EXISTS with a table that is guaranteed to exist.
        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT orderkey, discount FROM lineitem", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

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
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "test_union_all",
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
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

    @Test
    public void testExplainAnalyzeVerbose()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) FROM orders WHERE orderkey < 0");
    }

    private static void assertJsonNodesHaveStats(JsonRenderer.JsonRenderedNode node)
    {
        assertTrue(node.getStats().isPresent());
        node.getChildren().forEach(AbstractTestDistributedQueries::assertJsonNodesHaveStats);
    }

    @Test
    public void testExplainAnalyzeFormatJson()
    {
        JsonRenderer renderer = new JsonRenderer(getQueryRunner().getMetadata().getFunctionAndTypeManager());
        Map<PlanFragmentId, JsonRenderer.JsonPlan> fragments = renderer.deserialize((String) computeActual("EXPLAIN ANALYZE (format JSON) SELECT * FROM orders").getOnlyValue());
        fragments.values().forEach(planFragment -> assertJsonNodesHaveStats(planFragment.getPlan()));
        fragments = renderer.deserialize((String) computeActual("EXPLAIN ANALYZE (format JSON) SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) FROM orders").getOnlyValue());
        fragments.values().forEach(planFragment -> assertJsonNodesHaveStats(planFragment.getPlan()));
        fragments = renderer.deserialize((String) computeActual("EXPLAIN ANALYZE (format JSON) SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) FROM orders WHERE orderkey < 0").getOnlyValue());
        fragments.values().forEach(planFragment -> assertJsonNodesHaveStats(planFragment.getPlan()));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "EXPLAIN ANALYZE doesn't support statement type: DropTable")
    public void testExplainAnalyzeDDL()
    {
        computeActual("EXPLAIN ANALYZE DROP TABLE orders");
    }

    @Test(expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = "Regexp matching interrupted|The query optimizer exceeded the timeout of .*",
            timeOut = 30_000)
    public void testRunawayRegexAnalyzerTimeout()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.QUERY_ANALYZER_TIMEOUT, "5s")
                .build();

        computeActual(session, "select REGEXP_EXTRACT('runaway_regex-is-evaluated-infinitely - xxx\"}', '.*runaway_(.*?)+-+xxx.*')");
    }

    // The sample query has 2^30 leaf nodes in query plan, expect to timeout during plan statement phase.
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "The query planner exceeded the timeout of .*", timeOut = 30_000)
    public void testQueryAnalysisTimeout()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.QUERY_ANALYZER_TIMEOUT, "1s")
                .build();

        String subQuery = "t%d AS (SELECT A.id, B.name FROM t%d A Join t%d B On A.id = B.id)";
        String sql = " WITH t1 AS (SELECT * FROM ( VALUES (1, 'a'), (2, 'b'), (3, 'c') ) AS t (id, name))";
        int maxSubQueryDepth = 30;
        for (int i = 1; i < maxSubQueryDepth; ++i) {
            sql += ',';
            sql += String.format(subQuery, i + 1, i, i);
        }
        sql += String.format(" SELECT * FROM t%d", 30);

        computeActual(session, sql);
    }

    // The sample query has 2^30 leaf nodes in query plan, expect to timeout during plan statement phase. This is for explain queries.
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "The query planner exceeded the timeout of .*", timeOut = 30_000)
    public void testSemanticAnalysisTimeout()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.QUERY_ANALYZER_TIMEOUT, "1s")
                .build();

        String subQuery = "t%d AS (SELECT A.id, B.name FROM t%d A Join t%d B On A.id = B.id)";
        String sql = "EXPLAIN WITH t1 AS (SELECT * FROM ( VALUES (1, 'a'), (2, 'b'), (3, 'c') ) AS t (id, name))";
        int maxSubQueryDepth = 30;
        for (int i = 1; i < maxSubQueryDepth; ++i) {
            sql += ',';
            sql += String.format(subQuery, i + 1, i, i);
        }
        sql += String.format(" SELECT * FROM t%d", 30);

        computeActual(session, sql);
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        skipTestUnless(supportsNotNullColumns());

        String catalog = getSession().getCatalog().get();
        String createTableStatement = "CREATE TABLE " + catalog + ".tpch.test_not_null_with_insert (\n" +
                "   \"column_a\" date,\n" +
                "   \"column_b\" date NOT NULL\n" +
                ")";
        assertUpdate("CREATE TABLE test_not_null_with_insert (column_a DATE, column_b DATE NOT NULL)");
        assertQuery(
                "SHOW CREATE TABLE test_not_null_with_insert",
                "VALUES '" + createTableStatement + "'");

        assertQueryFails("INSERT INTO test_not_null_with_insert (column_a) VALUES (date '2012-12-31')", "NULL value not allowed for NOT NULL column: column_b");
        assertQueryFails("INSERT INTO test_not_null_with_insert (column_a, column_b) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_b");
        assertQueryFails("INSERT INTO test_not_null_with_insert VALUES (date '2011-11-30', date '2011-10-01'), (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_b");

        assertUpdate("ALTER TABLE test_not_null_with_insert ADD COLUMN column_c BIGINT NOT NULL");
        assertQuery(
                "SHOW CREATE TABLE test_not_null_with_insert",
                "VALUES 'CREATE TABLE " + catalog + ".tpch.test_not_null_with_insert (\n" +
                        "   \"column_a\" date,\n" +
                        "   \"column_b\" date NOT NULL,\n" +
                        "   \"column_c\" bigint NOT NULL\n" +
                        ")'");

        assertQueryFails("INSERT INTO test_not_null_with_insert (column_b) VALUES (date '2012-12-31')", "NULL value not allowed for NOT NULL column: column_c");
        assertQueryFails("INSERT INTO test_not_null_with_insert (column_b, column_c) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_c");
        assertQueryFails("INSERT INTO test_not_null_with_insert (column_b, column_c) VALUES (date '2011-11-30', 123), (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_c");

        assertUpdate("INSERT INTO test_not_null_with_insert (column_b, column_c) VALUES (date '2012-12-31', 1)", 1);
        assertUpdate("INSERT INTO test_not_null_with_insert (column_a, column_b, column_c) VALUES (date '2013-01-01', date '2013-01-02', 2)", 1);
        assertQuery(
                "SELECT * FROM test_not_null_with_insert",
                "VALUES ( NULL, CAST ('2012-12-31' AS DATE), 1 ), ( CAST ('2013-01-01' AS DATE), CAST ('2013-01-02' AS DATE), 2 );");

        assertUpdate("DROP TABLE test_not_null_with_insert");
    }

    @Test
    public void testRenameTable()
    {
        assertUpdate("CREATE TABLE test_rename AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE test_rename RENAME TO test_rename_new");
        MaterializedResult materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("ALTER TABLE IF EXISTS test_rename_new RENAME TO test_rename");
        materializedRows = computeActual("SELECT x FROM test_rename");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("ALTER TABLE IF EXISTS test_rename RENAME TO test_rename_new");
        materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        // provide new table name in uppercase
        assertUpdate("ALTER TABLE test_rename_new RENAME TO TEST_RENAME");
        materializedRows = computeActual("SELECT x FROM test_rename");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("DROP TABLE test_rename");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_new"));

        assertUpdate("ALTER TABLE IF EXISTS test_rename RENAME TO test_rename_new");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_new"));
    }

    @Test
    public void testRenameColumn()
    {
        assertUpdate("CREATE TABLE test_rename_column AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN x TO before_y");
        MaterializedResult materializedRows = computeActual("SELECT before_y FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN IF EXISTS before_y TO y");
        materializedRows = computeActual("SELECT y FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN IF EXISTS columnNotExists TO y");

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN y TO Z");
        materializedRows = computeActual("SELECT z FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN IF EXISTS z TO a");
        materializedRows = computeActual("SELECT a FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("DROP TABLE test_rename_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_column"));
        assertUpdate("ALTER TABLE IF EXISTS test_rename_column RENAME COLUMN columnNotExists TO y");
        assertUpdate("ALTER TABLE IF EXISTS test_rename_column RENAME COLUMN IF EXISTS columnNotExists TO y");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_column"));
    }

    @Test
    public void testDropColumn()
    {
        assertUpdate("CREATE TABLE test_drop_column AS SELECT 123 x, 456 y, 111 a", 1);

        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN x");
        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN IF EXISTS y");
        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN IF EXISTS notExistColumn");
        assertQueryFails("SELECT x FROM test_drop_column", ".* Column 'x' cannot be resolved");
        assertQueryFails("SELECT y FROM test_drop_column", ".* Column 'y' cannot be resolved");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN a", ".* Cannot drop the only column in a table");

        assertUpdate("DROP TABLE test_drop_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_column"));
        assertUpdate("ALTER TABLE IF EXISTS test_drop_column DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS test_drop_column DROP COLUMN IF EXISTS notExistColumn");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_column"));
    }

    @Test
    public void testAddColumn()
    {
        assertUpdate("CREATE TABLE test_add_column AS SELECT 123 x", 1);
        assertUpdate("CREATE TABLE test_add_column_a AS SELECT 234 x, 111 a", 1);
        assertUpdate("CREATE TABLE test_add_column_ab AS SELECT 345 x, 222 a, 33.3E0 b", 1);
        assertUpdate("CREATE TABLE test_add_column_abc AS SELECT 456 x, 333 a, 66.6E0 b, 'fourth' c", 1);

        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN x bigint", ".* Column 'x' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN X bigint", ".* Column 'X' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN a bigint");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_a", 1);
        MaterializedResult materializedRows = computeActual("SELECT x, a FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123);
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN b double");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_ab", 1);
        materializedRows = computeActual("SELECT x, a, b FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123);
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertNull(materializedRows.getMaterializedRows().get(0).getField(2));
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);
        assertNull(materializedRows.getMaterializedRows().get(1).getField(2));
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(0), 345);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(1), 222L);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(2), 33.3);

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("ALTER TABLE test_add_column ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_abc", 1);
        materializedRows = computeActual("SELECT x, a, b, c FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123);
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertNull(materializedRows.getMaterializedRows().get(0).getField(2));
        assertNull(materializedRows.getMaterializedRows().get(0).getField(3));
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);
        assertNull(materializedRows.getMaterializedRows().get(1).getField(2));
        assertNull(materializedRows.getMaterializedRows().get(1).getField(3));
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(0), 345);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(1), 222L);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(2), 33.3);
        assertNull(materializedRows.getMaterializedRows().get(2).getField(3));
        assertEquals(materializedRows.getMaterializedRows().get(3).getField(0), 456);
        assertEquals(materializedRows.getMaterializedRows().get(3).getField(1), 333L);
        assertEquals(materializedRows.getMaterializedRows().get(3).getField(2), 66.6);
        assertEquals(materializedRows.getMaterializedRows().get(3).getField(3), "fourth");

        assertUpdate("DROP TABLE test_add_column");
        assertUpdate("DROP TABLE test_add_column_a");
        assertUpdate("DROP TABLE test_add_column_ab");
        assertUpdate("DROP TABLE test_add_column_abc");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_a"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_ab"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_abc"));

        assertUpdate("ALTER TABLE IF EXISTS test_add_column ADD COLUMN x bigint");
        assertUpdate("ALTER TABLE IF EXISTS test_add_column ADD COLUMN IF NOT EXISTS x bigint");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column"));
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

        assertQueryFails("INSERT INTO test_insert (b) VALUES (ARRAY[1.23E1])", "line 1:37: Mismatch at column 1.*");

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

        // Test DELETE access control
        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertAccessAllowed("DELETE FROM test_delete where orderkey < 12", privilege("orderkey", DELETE_TABLE));
        assertAccessAllowed("DELETE FROM test_delete where orderkey < 12", privilege("orderdate", SELECT_COLUMN));
        assertAccessAllowed("DELETE FROM test_delete", privilege("orders", SELECT_COLUMN));
    }

    @Test
    public void testUpdate()
    {
        assertUpdate("CREATE TABLE test_update AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertUpdate("UPDATE test_update SET orderstatus = 'O_UPDATED' WHERE orderstatus = 'O'", "SELECT count(*) FROM orders WHERE orderstatus = 'O'");
        assertQuery("SELECT * FROM test_update", "SELECT * FROM orders WHERE orderstatus <> 'O'");

        assertUpdate("DROP TABLE test_update");
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
    public void testViewWithReservedKeywords()
    {
        skipTestUnless(supportsViews());

        assertUpdate("CREATE TABLE \"group\" AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        @Language("SQL") String query = "SELECT orderkey, orderstatus, totalprice / 2 half FROM orders";
        @Language("SQL") String groupQuery = "SELECT orderkey, orderstatus, totalprice / 2 half FROM \"group\"";

        assertUpdate("CREATE OR REPLACE VIEW test_view_with_reserved_keywords AS " + groupQuery);

        assertQuery("SELECT * FROM test_view_with_reserved_keywords", query);

        String expectedSql = formatSqlText(format(
                "CREATE VIEW %s.%s.%s SECURITY %s AS %s",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_view_with_reserved_keywords",
                "DEFINER",
                groupQuery)).trim();

        MaterializedResult actual = computeActual("SHOW CREATE VIEW test_view_with_reserved_keywords");

        assertEquals(getOnlyElement(actual.getOnlyColumnAsSet()), expectedSql);

        assertUpdate("DROP VIEW test_view_with_reserved_keywords");
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
        String user = getSession().getUser();
        actual = computeActual(format(
                "SELECT table_name, view_owner, view_definition FROM information_schema.views WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        expected = resultBuilder(getSession(), actual.getTypes())
                .row("meta_test_view", user, formatSqlText(query))
                .build();

        assertContains(actual, expected);

        // test SHOW COLUMNS
        actual = computeActual("SHOW COLUMNS FROM meta_test_view");

        expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("x", "bigint", "", "", 19L, null, null)
                .row("y", "varchar(3)", "", "", null, null, 3L)
                .build();

        assertEquals(actual, expected);

        // test SHOW CREATE VIEW
        String expectedSql = formatSqlText(format(
                "CREATE VIEW %s.%s.%s SECURITY %s AS %s",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "meta_test_view",
                "DEFINER",
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
                            queryManager.getQueries().stream()
                                    .map(BasicQueryInfo::getQueryId)
                                    .map(queryManager::getFullQueryInfo)
                                    .filter(info -> !info.isFinalQueryInfo())
                                    .collect(toList()),
                            ImmutableList.of()),
                    new Duration(1, MINUTES));

            // We cannot simply get the number of completed queries as soon as all the queries are completed, because this counter may not be up-to-date at that point.
            // The completed queries counter is updated in a final query info listener, which is called eventually.
            // Therefore, here we wait until the value of this counter gets stable.

            DispatchManager dispatchManager = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDispatchManager();
            long beforeCompletedQueriesCount = waitUntilStable(() -> dispatchManager.getStats().getCompletedQueries().getTotalCount(), new Duration(5, SECONDS));
            long beforeSubmittedQueriesCount = dispatchManager.getStats().getSubmittedQueries().getTotalCount();
            assertUpdate("CREATE TABLE test_query_logging_count AS SELECT 1 foo_1, 2 foo_2_4", 1);
            assertQuery("SELECT foo_1, foo_2_4 FROM test_query_logging_count", "SELECT 1, 2");
            assertUpdate("DROP TABLE test_query_logging_count");
            assertQueryFails("SELECT * FROM test_query_logging_count", ".*Table .* does not exist");

            // TODO: Figure out a better way of synchronization
            assertUntilTimeout(
                    () -> assertEquals(dispatchManager.getStats().getCompletedQueries().getTotalCount() - beforeCompletedQueriesCount, 4),
                    new Duration(1, MINUTES));
            assertEquals(dispatchManager.getStats().getSubmittedQueries().getTotalCount() - beforeSubmittedQueriesCount, 4);
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
    public void testExtraLargeQuerySuccess()
    {
        // Will have stack overflow if enabled
        Session pullLambdaExpressionDisabled = Session.builder(getSession())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "false")
                .build();
        assertQuery(pullLambdaExpressionDisabled, "SELECT " + Joiner.on(" AND ").join(nCopies(1000, "1 = 1")), "SELECT true");
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
    public void testViewAccessControlInvokerDefault()
    {
        skipTestUnless(supportsViews());

        Session viewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(new Identity("test_view_access_owner", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setSystemProperty("default_view_security_mode", INVOKER.name())
                .build();

        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW test_view_access AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        assertAccessAllowed(
                "SELECT * FROM test_view_access",
                privilege(viewOwnerSession.getUser(), "orders", SELECT_COLUMN));

        assertAccessDenied(
                "SELECT * FROM test_view_access",
                "Cannot select from columns.*",
                privilege(getSession().getUser(), "orders", SELECT_COLUMN));

        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW test_view_access1 SECURITY DEFINER AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        assertAccessAllowed(
                "SELECT * FROM test_view_access1",
                privilege(viewOwnerSession.getUser(), "orders", SELECT_COLUMN));

        assertAccessAllowed(
                "SELECT * FROM test_view_access1",
                privilege(getSession().getUser(), "orders", SELECT_COLUMN));
        assertAccessAllowed(viewOwnerSession, "DROP VIEW test_view_access");
        assertAccessAllowed(viewOwnerSession, "DROP VIEW test_view_access1");
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

        // TEST COLUMN-LEVEL PRIVILEGES
        // view creation permissions are only checked at query time, not at creation
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW test_view_access AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a table requires the view owner to have special view creation privileges for the table
        assertAccessDenied(
                "SELECT * FROM test_view_access",
                "View owner 'test_view_access_owner' cannot create view that selects from .*.orders.*",
                privilege(viewOwnerSession.getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify the view owner can select from the view even without special view creation privileges
        assertAccessAllowed(
                viewOwnerSession,
                "SELECT * FROM test_view_access",
                privilege(viewOwnerSession.getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a table does not require the session user to have SELECT privileges on the underlying table
        assertAccessAllowed(
                "SELECT * FROM test_view_access",
                privilege(getSession().getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM test_view_access",
                privilege(getSession().getUser(), "orders", SELECT_COLUMN));

        Session nestedViewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(new Identity("test_nested_view_access_owner", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        // view creation permissions are only checked at query time, not at creation
        assertAccessAllowed(
                nestedViewOwnerSession,
                "CREATE VIEW test_nested_view_access AS SELECT * FROM test_view_access",
                privilege("test_view_access", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a view requires the view owner of the outer view to have special view creation privileges for the inner view
        assertAccessDenied(
                "SELECT * FROM test_nested_view_access",
                "View owner 'test_nested_view_access_owner' cannot create view that selects from .*.test_view_access.*",
                privilege(nestedViewOwnerSession.getUser(), "test_view_access", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a view does not require the session user to have SELECT privileges for the inner view
        assertAccessAllowed(
                "SELECT * FROM test_nested_view_access",
                privilege(getSession().getUser(), "test_view_access", CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM test_nested_view_access",
                privilege(getSession().getUser(), "test_view_access", SELECT_COLUMN));

        // verify that INVOKER security runs as session user
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW test_invoker_view_access SECURITY INVOKER AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM test_invoker_view_access",
                privilege(viewOwnerSession.getUser(), "orders", SELECT_COLUMN));
        assertAccessDenied(
                "SELECT * FROM test_invoker_view_access",
                "Cannot select from columns \\[.*\\] in table .*.orders.*",
                privilege(getSession().getUser(), "orders", SELECT_COLUMN));

        assertAccessDenied("SHOW CREATE VIEW test_nested_view_access", "Cannot show create table for .*test_nested_view_access.*", privilege("test_nested_view_access", SHOW_CREATE_TABLE));
        assertAccessAllowed("SHOW CREATE VIEW test_nested_view_access", privilege("test_denied_access_view", SHOW_CREATE_TABLE));

        assertAccessAllowed(nestedViewOwnerSession, "DROP VIEW test_nested_view_access");
        assertAccessAllowed(viewOwnerSession, "DROP VIEW test_view_access");
        assertAccessAllowed(viewOwnerSession, "DROP VIEW test_invoker_view_access");
    }

    @Test
    public void testWrittenStats()
    {
        String sql = "CREATE TABLE test_written_stats AS SELECT * FROM nation";
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), sql);
        QueryInfo queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());

        assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
        assertEquals(queryInfo.getQueryStats().getWrittenOutputPositions(), 25L);
        assertTrue(queryInfo.getQueryStats().getWrittenOutputLogicalDataSize().toBytes() > 0L);

        sql = "INSERT INTO test_written_stats SELECT * FROM nation LIMIT 10";
        resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), sql);
        queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());

        assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
        assertEquals(queryInfo.getQueryStats().getWrittenOutputPositions(), 10L);
        assertTrue(queryInfo.getQueryStats().getWrittenOutputLogicalDataSize().toBytes() > 0L);

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

    @Test
    public void testPayloadJoinApplicability()
    {
        Session sessionNoOpt = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_PAYLOAD_JOINS, "false")
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "false")
                .build();

        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_PAYLOAD_JOINS, "true")
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "false")
                .build();

        assertUpdate("create table lineitem_map as select *, map(ARRAY[1,3], ARRAY[2,4]) as m1, map(ARRAY[1,3], ARRAY[2,4]) as m2 from lineitem", 60175);
        assertUpdate("create table part_map as select *, map(ARRAY[1,3], ARRAY[2,4]) as m3, map(ARRAY[1,3], ARRAY[2,4]) as m4 from part", 2000);

        final List<String> queries = getPayloadQueries("lineitem_map");

        for (String query : queries) {
            MaterializedResult resultExplainQuery = computeActual(session, "EXPLAIN " + query);
            MaterializedResult resultExplainQueryNoOpt = computeActual(sessionNoOpt, "EXPLAIN " + query);
            String explainNoOpt = sanitizePlan((String) getOnlyElement(resultExplainQueryNoOpt.getOnlyColumnAsSet()));
            String explainWithOpt = sanitizePlan((String) getOnlyElement(resultExplainQuery.getOnlyColumnAsSet()));
            assertNotEquals(explainWithOpt, explainNoOpt, "Couldn't optimize query: " + query);

            MaterializedResult materializedRows = computeActual(session, query);
            assertEquals(materializedRows.getRowCount(), 60175);
        }

        // Queries that we don't handle because of unsupported operators or because intermediate queries use columns that will be hidden by the rewrite
        String[] nonOptimizableQueries = {
                "SELECT l.* FROM (select * from lineitem inner join part using (partkey) where orderkey <= 60000) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)",
                "with lm as (select if (quantity > 1, quantity, 1) as q1, * from lineitem_map), lm2 as (select if (q1 > 2, q1, 1) as q2, quantity,partkey,suppkey,linenumber,m1,m2 from lm left join orders o on (lm.orderkey=o.orderkey)) SELECT l.* FROM (select q2+3 as q3,partkey,suppkey,linenumber,m1,m2 from lm2) l left join part p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select rand(100) as pk100 from (select orderkey+1 as ok1, * from lineitem) l left join orders o on (l.ok1 = o.orderkey+1)) l left join part p on (l.pk100=p.partkey)",
                "SELECT l.*, p.m3, p.m4 FROM (select * from lineitem where false) l left join orders o on (l.orderkey = o.orderkey) left join part_map p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select distinct orderkey, partkey, quantity from lineitem) l left join orders o on (l.orderkey = o.orderkey) left join part_map p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select orderkey, partkey, sum(tax) as ss from lineitem group by orderkey, partkey) l left join orders o on (l.orderkey = o.orderkey) left join part_map p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select orderkey, partkey, sum(1) as ss from lineitem group by orderkey, partkey, suppkey) l left join orders o on (l.orderkey = o.orderkey) left join part_map p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select distinct orderkey, partkey from lineitem) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select orderkey, partkey, sum(1) as ss from lineitem group by orderkey, partkey) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select m1, orderkey,partkey from lineitem_map where false) l left join orders o on (l.orderkey+1 = o.orderkey+1) left join part p on (l.partkey+1=p.partkey+1)",
                "SELECT l.* FROM (select m1, orderkey,partkey from lineitem_map where false) l left join orders o on (l.orderkey+1 = o.orderkey+1) inner join part p on (l.partkey+1=p.partkey+1)",
                "SELECT l.* FROM (select m1, orderkey,partkey from lineitem_map where false) l inner join orders o on (l.orderkey+1 = o.orderkey+1) left join part p on (l.partkey+1=p.partkey+1)",
                "SELECT l.* FROM (select m1, orderkey,partkey from lineitem_map where false) l left join orders o on (l.orderkey+1 = o.orderkey+1) left join part p on (l.partkey+1=p.partkey+1) union select m1, orderkey,partkey from lineitem_map",
                "SELECT l.* FROM (SELECT partkey,orderkey FROM UNNEST(ARRAY[1, 2], ARRAY[3, 4]) t(orderkey, partkey)) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select orderkey, partkey, count(*) from (select l.* from lineitem_map l left join orders o on (l.orderkey = o.orderkey)) group by orderkey, partkey) l left join part p on (l.partkey=p.partkey)",
        };

        for (String query : nonOptimizableQueries) {
            MaterializedResult resultExplainQuery = computeActual(session, "EXPLAIN " + query);
            MaterializedResult resultExplainQueryNoOpt = computeActual(sessionNoOpt, "EXPLAIN " + query);
            String explainNoOpt = sanitizePlan((String) getOnlyElement(resultExplainQueryNoOpt.getOnlyColumnAsSet()));
            String explainWithOpt = sanitizePlan((String) getOnlyElement(resultExplainQuery.getOnlyColumnAsSet()));
            assertEquals(explainWithOpt, explainNoOpt, "Query was optimized: " + query);
        }

        MaterializedResult countStarQuery = computeActual(session, "select count(t.m1) from (SELECT l.* FROM lineitem_map l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)) t");
        assertEquals((Long) countStarQuery.getOnlyValue(), Long.valueOf(60175L));
    }

    @Test
    public void testPayloadJoinCorrectness()
    {
        Session sessionNoOpt = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_PAYLOAD_JOINS, "false")
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "false")
                .build();

        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_PAYLOAD_JOINS, "true")
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "false")
                .build();

        assertUpdate("create table lineitem_small as select *, map(ARRAY[1,3], ARRAY[2,4]) as m1, map(ARRAY[1,3], ARRAY[2,4]) as m2 from lineitem order by partkey, orderkey limit 1000", 1000);

        final List<String> queries = getPayloadQueries("lineitem_small");

        for (String query : queries) {
            MaterializedResult resultExplainQuery = computeActual(session, "EXPLAIN " + query);
            MaterializedResult resultExplainQueryNoOpt = computeActual(sessionNoOpt, "EXPLAIN " + query);
            String explainNoOpt = sanitizePlan((String) getOnlyElement(resultExplainQueryNoOpt.getOnlyColumnAsSet()));
            String explainWithOpt = sanitizePlan((String) getOnlyElement(resultExplainQuery.getOnlyColumnAsSet()));
            assertNotEquals(explainWithOpt, explainNoOpt, "Couldn't optimize query: " + query);
            assertQueryWithSameQueryRunner(session, query, sessionNoOpt);
        }
    }

    private static List<String> getPayloadQueries(String tableName)
    {
        String[] queries = {
                "SELECT l.* FROM LINEITEM_TABLE l left join orders o on (l.orderkey+1 = o.orderkey+1) left join part p on (l.partkey+1=p.partkey+1)",
                "SELECT l.* FROM LINEITEM_TABLE l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)",
                "SELECT l.*, p.m3 FROM LINEITEM_TABLE l left join orders o on (l.orderkey = o.orderkey) left join (select *, map(array[1,2], array[11,22]) as m3 from part) p on (l.partkey=p.partkey)",
                "SELECT l.*, p.m3 FROM LINEITEM_TABLE l left join orders o on (l.orderkey = o.orderkey) left join (select *, map(array[partkey], array[size]) as m3 from part) p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select * from LINEITEM_TABLE where linenumber < 9) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select * from LINEITEM_TABLE where orderkey <= 60000) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)",
                "SELECT cast (l.orderkey as int) as orderkey, l.partkey, MAP_CONCAT(CAST(l.m1 as map<int, real> ), cast(l.m2 as map<int, real>)) as m2 FROM (select * from LINEITEM_TABLE) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)",

                "SELECT l.orderkey, l.partkey, MAP_CONCAT(CAST(l.m1 as map<int, real> ), cast(l.m2 as map<int, real>)) as m2 FROM (select * from LINEITEM_TABLE where linenumber < 9) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)",

                "SELECT l.*, p.m3, p.m4 FROM LINEITEM_TABLE l left join orders o on (l.orderkey = o.orderkey) left join (select *, map(ARRAY[1,3], ARRAY[2,4]) as m3, map(ARRAY[1,3], ARRAY[2,4]) as m4 from part) p on (l.partkey=p.partkey) union all SELECT l.*,map(ARRAY[1,3], ARRAY[2,4]),map(ARRAY[1,3], ARRAY[2,4]) FROM LINEITEM_TABLE l where false",
                "SELECT l.*, p.brand, p.size FROM LINEITEM_TABLE l left join orders o on (l.orderkey+1= o.orderkey+1) left join part p on (l.partkey=p.partkey)",
                "SELECT l.*, p.brand, p.size FROM LINEITEM_TABLE l left join orders o on (cast(l.orderkey as int) = o.orderkey) left join part p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select orderkey+1 as ok1, * from LINEITEM_TABLE) l left join orders o on (l.ok1 = o.orderkey+1) left join part p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select *, cast(orderkey as int) as ok from LINEITEM_TABLE) l left join (select *, cast(orderkey as int) as ok from orders) o on (l.ok = o.ok) left join (select *, cast(partkey as int) as pk from part) p on (l.ok=p.pk)",
                "with lm as (select quantity + 1 as q1, * from LINEITEM_TABLE) SELECT l.* FROM (select * from lm) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select *, cast(orderkey as int) as ok from LINEITEM_TABLE) l left join (select *, cast(orderkey as int) as ok from orders) o on (l.ok = o.ok) left join (select *, cast(partkey as int) as pk from part) p on (l.ok=p.pk)",
                "SELECT l.* FROM LINEITEM_TABLE l left join orders o on (cast(l.orderkey as varchar) = cast(o.orderkey as varchar)) left join part p on (l.partkey=p.partkey)",
                "SELECT l.* FROM (select *, cast(orderkey as int) as ok from LINEITEM_TABLE) l left join (select *, cast(orderkey as int) as ok from orders) o on (l.ok = o.ok) left join (select *, cast(partkey as int) as pk from part) p on (l.ok=p.pk)",
                "SELECT l.* FROM LINEITEM_TABLE l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey) left join (select 1 as m) mm on p.partkey=mm.m",
                "SELECT l.* FROM LINEITEM_TABLE l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey) left join (select 1 as m) mm on p.partkey+1=mm.m",
                "select * from (SELECT l.* FROM LINEITEM_TABLE l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)) t, (select * from nation where name='JAPAN')",
        };
        return Arrays.stream(queries).map(q -> q.replace("LINEITEM_TABLE", tableName)).collect(toImmutableList());
    }

    @Test
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "true")
                .build();

        Session disabled = Session.builder(getSession())
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "false")
                .build();
        assertUpdate("create table lineitem_map_cast as select *, map(ARRAY[1,3], ARRAY[2,4]) as m1, map(ARRAY[1,3], ARRAY[2,4]) as m2 from lineitem", 60175);
        assertQueryWithSameQueryRunner(session, "SELECT l.* FROM lineitem_map_cast l left join orders o on (cast(l.orderkey as varchar) = cast(o.orderkey as varchar)) left join part p on (l.partkey=p.partkey)", disabled);
    }

    @Test
    public void testStringFilters()
    {
        assertUpdate("CREATE TABLE test_charn_filter (shipmode CHAR(10))");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_charn_filter"));
        assertTableColumnNames("test_charn_filter", "shipmode");
        assertUpdate("INSERT INTO test_charn_filter SELECT shipmode FROM lineitem", 60175);

        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'AIR'", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'AIR    '", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'AIR       '", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'AIR            '", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'NONEXIST'", "VALUES (0)");

        assertUpdate("CREATE TABLE test_varcharn_filter (shipmode VARCHAR(10))");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_varcharn_filter"));
        assertTableColumnNames("test_varcharn_filter", "shipmode");
        assertUpdate("INSERT INTO test_varcharn_filter SELECT shipmode FROM lineitem", 60175);

        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR'", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR    '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR       '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR            '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'NONEXIST'", "VALUES (0)");
    }

    @Test
    public void testTrackCTEs()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .build();

        String query = "with tbl as (select * from lineitem), tbl2 as (select * from tbl) select * from tbl, tbl2";
        MaterializedResult materializedResult = computeActual(session, "explain " + query);
        String explain = (String) getOnlyElement(materializedResult.getOnlyColumnAsSet());

        checkCTEInfo(explain, "tbl", 2, false, false);
        checkCTEInfo(explain, "tbl2", 1, false, false);
    }

    @Test
    public void testTrackCTEsAndViews()
    {
        skipTestUnless(supportsViews());

        Session session = Session.builder(getSession())
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .build();
        assertUpdate("CREATE VIEW v as select 'view' as col");

        MaterializedResult resultExplainQuery = computeActual(session, "EXPLAIN with cte1 as (select * from v), v as (select 2 as x) select * from cte1, v, v");
        String explainString = (String) resultExplainQuery.getOnlyValue();

        checkCTEInfo(explainString, "cte1", 1, false, false);

        // view "catalog.schema.v" is referenced once, and the cte "v" twice in the above query
        checkCTEInfo(explainString, "v", 2, false, false);

        String viewName = format("%s.%s.v", getSession().getCatalog().get(), getSession().getSchema().get());
        checkCTEInfo(explainString, viewName, 1, true, false);
    }

    @Test
    public void testTrackCTEsNoSessionCatalog()
    {
        skipTestUnless(supportsViews());

        Session session = getSession();
        String catalog = session.getCatalog().get();
        String schema = session.getSchema().get();

        Session sessionNoCatalog = Session.builder(getSession())
                .setCatalog(null)
                .setSchema(null)
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .build();
        assertUpdate("CREATE OR REPLACE VIEW v as select 'view' as col");

        String sqlNoSchema = "EXPLAIN with cte1 as (select * from v), v as (select 2 as x) select * from cte1, v, v";
        assertQueryFails(sessionNoCatalog, sqlNoSchema, ".*Schema must be specified when session schema is not set");

        String viewName = format("%s.%s.v", catalog, schema);
        String sql = format("EXPLAIN with cte1 as (select * from %s), v as (select 2 as x) select * from cte1, v, v", viewName);
        MaterializedResult resultExplainQuery = computeActual(sessionNoCatalog, sql);
        String explainString = (String) resultExplainQuery.getOnlyValue();

        checkCTEInfo(explainString, "cte1", 1, false, false);

        // view "catalog.schema.v" is referenced once, and the cte "v" twice in the above query
        checkCTEInfo(explainString, "v", 2, false, false);
        checkCTEInfo(explainString, viewName, 1, true, false);
    }

    @Test
    public void testShardedJoinOptimization()
    {
        Session defaultSession = getSession();

        Session session = Session.builder(defaultSession)
                .setSystemProperty(SHARDED_JOINS_STRATEGY, "ALWAYS")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();

        String[] queries = {
                "select * from lineitem l join orders o on (l.orderkey=o.orderkey)",
                "select * from lineitem l join orders o on (l.orderkey=o.orderkey) join part p on (l.partkey=p.partkey)",
                "select * from lineitem l LEFT JOIN orders o on (l.orderkey=o.orderkey)"
        };

        for (String query : queries) {
            MaterializedResult resultExplainQuery = computeActual(session, "EXPLAIN " + query);
            assert (((String) resultExplainQuery.getOnlyValue()).contains("random"));

            assertQuery(session, query);
        }

        String[] notSupportedQueries = {
                "select * from lineitem l right join orders o on (l.orderkey=o.orderkey)",
                "select * from lineitem l full join orders o on (l.orderkey=o.orderkey)"
        };

        for (String query : notSupportedQueries) {
            MaterializedResult resultExplainQuery = computeActual(session, "EXPLAIN " + query);
            assert (!((String) resultExplainQuery.getOnlyValue()).contains("random"));

            assertQueryWithSameQueryRunner(session, query, defaultSession);
        }
    }

    @Test
    public void testSessionPropertyDecode()
    {
        assertQueryFails(
                Session.builder(getSession())
                        .setSystemProperty("task_writer_count", "abc" /*number is expected*/)
                        .build(),
                "SELECT 1",
                ".*task_writer_count is invalid.*");
    }

    protected void checkCTEInfo(String explain, String name, int frequency, boolean isView, boolean isMaterialized)
    {
        String regex = "CTEInfo.*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(explain);
        assertTrue(matcher.find());

        String cteInfo = matcher.group();
        assertTrue(cteInfo.contains(name + ": " + frequency + " (is_view: " + isView + ")" + " (is_materialized: " + isMaterialized + ")"));
    }

    private String sanitizePlan(String explain)
    {
        return explain
                .replaceAll("hashvalue_[0-9][0-9][0-9]", "hashvalueXXX")
                .replaceAll("hashvalue_[0-9][0-9]", "hashvalueXXX")
                .replaceAll("sum_[0-9][0-9]", "sumXXX")
                .replaceAll("\\[PlanNodeId (\\d+(?:,\\d+)*)\\]", "")
                .replaceAll("Values => .*\n", "\n");
    }

    @Test
    public void testViewWithUUID()
    {
        skipTestUnless(supportsViews());

        @Language("SQL") String query = "SELECT * FROM (VALUES (CAST(0 AS INTEGER), NULL), (CAST(1 AS INTEGER), UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59')) AS t (rum, c1)";

        // Create View with UUID type in Hive
        assertQuerySucceeds("CREATE VIEW test_hive_view AS " + query);

        // Select UUID from the view
        MaterializedResult result = computeActual("SELECT c1 FROM test_hive_view WHERE rum = 1");

        // Verify the result set is not empty
        assertTrue(result.getMaterializedRows().size() > 0, "Result set is empty");
        assertEquals(result.getTypes(), ImmutableList.of(UUID));
        assertEquals(result.getOnlyValue(), "12151fd2-7586-11e9-8f9e-2a86e4085a59");

        // Drop the view after the test
        assertQuerySucceeds("DROP VIEW test_hive_view");
    }
}
