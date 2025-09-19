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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.ITERATIVE_OPTIMIZER_TIMEOUT;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZER_USE_HISTOGRAMS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public abstract class TestIcebergDistributedQueries
        extends AbstractTestDistributedQueries
{
    private final CatalogType catalogType;
    private final Map<String, String> extraConnectorProperties;

    protected TestIcebergDistributedQueries(CatalogType catalogType, Map<String, String> extraConnectorProperties)
    {
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
        this.extraConnectorProperties = requireNonNull(extraConnectorProperties, "extraConnectorProperties is null");
    }

    protected TestIcebergDistributedQueries(CatalogType catalogType)
    {
        this(catalogType, ImmutableMap.of());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(catalogType)
                .setExtraConnectorProperties(extraConnectorProperties)
                .build().getQueryRunner();
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        // Not null columns are not yet supported by the connector
        return false;
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Override
    public void testDescribeOutput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .build();

        // VarcharType on Iceberg cannot record its length parameter.
        // So we will get types of `varchar` for column `name` and `comment` rather than `varchar(25)` and `varchar(152)`
        //  comparing with the overridden method in parent class.
        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
                .build();

        // VarcharType on Iceberg cannot record its length parameter.
        // So we will get a type of `varchar` for column `name` rather than `varchar(25)`
        //  comparing with the overridden method in parent class.
        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // Catalog iceberg only supports writes using autocommit
    }

    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // Catalog iceberg only supports writes using autocommit
    }

    /**
     * Increased the optimizer timeout from 15000ms to 25000ms
     */
    @Override
    public void testLargeIn()
    {
        String longValues = range(0, 5000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        Session session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "25000ms")
                .build();
        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey IN (mod(1000, orderkey), " + longValues + ")");
        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey NOT IN (mod(1000, orderkey), " + longValues + ")");

        String varcharValues = range(0, 5000)
                .mapToObj(i -> "'" + i + "'")
                .collect(joining(", "));
        assertQuery(session, "SELECT orderkey FROM orders WHERE cast(orderkey AS VARCHAR) IN (" + varcharValues + ")");
        assertQuery(session, "SELECT orderkey FROM orders WHERE cast(orderkey AS VARCHAR) NOT IN (" + varcharValues + ")");

        String arrayValues = range(0, 5000)
                .mapToObj(i -> format("ARRAY[%s, %s, %s]", i, i + 1, i + 2))
                .collect(joining(", "));
        assertQuery(session, "SELECT ARRAY[0, 0, 0] in (ARRAY[0, 0, 0], " + arrayValues + ")", "values true");
        assertQuery(session, "SELECT ARRAY[0, 0, 0] in (" + arrayValues + ")", "values false");
    }

    /**
     * Increased the optimizer timeouts from 30000ms and 20000ms to 40000ms and 30000ms respectively
     */
    @Override
    public void testLargeInWithHistograms()
    {
        String longValues = range(0, 10_000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        String query = "select orderpriority, sum(totalprice) from lineitem join orders on lineitem.orderkey = orders.orderkey where orders.orderkey in (" + longValues + ") group by 1";
        Session session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "40000ms")
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                .build();
        assertQuerySucceeds(session, query);
        session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "30000ms")
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "false")
                .build();
        assertQuerySucceeds(session, query);
    }

    @Override
    public void testStringFilters()
    {
        // Type not supported for Iceberg: CHAR(10). Only test VARCHAR(10).
        // Retained exactly the latter half of the overridden method in parent class.
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
    public void testRenameView()
    {
        skipTestUnless(supportsViews());
        assertQuerySucceeds("CREATE TABLE iceberg_test_table (_string VARCHAR, _integer INTEGER)");
        assertUpdate("CREATE VIEW test_view_to_be_renamed AS SELECT * FROM iceberg_test_table");
        assertUpdate("ALTER VIEW IF EXISTS test_view_to_be_renamed RENAME TO test_view_renamed");
        assertUpdate("CREATE VIEW test_view2_to_be_renamed AS SELECT * FROM iceberg_test_table");
        assertUpdate("ALTER VIEW test_view2_to_be_renamed RENAME TO test_view2_renamed");
        assertQuerySucceeds("SELECT * FROM test_view_renamed");
        assertQuerySucceeds("SELECT * FROM test_view2_renamed");
        assertUpdate("DROP VIEW test_view_renamed");
        assertUpdate("DROP VIEW test_view2_renamed");
        assertUpdate("DROP TABLE iceberg_test_table");
    }

    @Test
    public void testRenameViewIfNotExists()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        skipTestUnless(supportsViews());
        assertQueryFails("ALTER VIEW test_rename_view_not_exist RENAME TO test_renamed_view_not_exist",
                format("line 1:1: View '%s.%s.test_rename_view_not_exist' does not exist", catalog, schema));
        assertQuerySucceeds("ALTER VIEW IF EXISTS test_rename_view_not_exist RENAME TO test_renamed_view_not_exist");
    }

    @Test
    public void testSupportedIsolationLevelForTransaction()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableNameForIsolationLevel = "test_supported_isolation_level";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableNameForIsolationLevel));

        // Does not support serializable isolation in Iceberg connector
        Session txnSession = assertStartTransaction(session, "START TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        assertQueryFails(txnSession, format("insert into %s values(1, '1001')", tableNameForIsolationLevel),
                "Connector supported isolation level REPEATABLE READ does not meet requested isolation level SERIALIZABLE");
        session = assertEndTransaction(txnSession, "rollback");

        // Support repeatable_read(snapshot) isolation in Iceberg connector
        txnSession = assertStartTransaction(session, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        assertQuery(txnSession, format("select count(*) from %s", tableNameForIsolationLevel), "values(0)");
        assertUpdate(txnSession, format("insert into %s values(1, '1001')", tableNameForIsolationLevel), 1);
        assertQuery(txnSession, format("select * from %s", tableNameForIsolationLevel), "values(1, '1001')");
        session = assertEndTransaction(txnSession, "commit");
        assertQuery(session, format("select * from %s", tableNameForIsolationLevel), "values(1, '1001')");
        assertQuery(getSession(), format("select * from %s", tableNameForIsolationLevel), "values(1, '1001')");

        // Support read committed isolation in Iceberg connector
        txnSession = assertStartTransaction(session, "START TRANSACTION ISOLATION LEVEL READ COMMITTED");
        assertQuery(txnSession, format("select * from %s", tableNameForIsolationLevel), "values(1, '1001')");
        assertUpdate(txnSession, format("insert into %s values(2, '1002')", tableNameForIsolationLevel), 1);
        assertUpdate(txnSession, format("insert into %s values(3, '1003')", tableNameForIsolationLevel), 1);
        assertQuery(txnSession, format("select * from %s", tableNameForIsolationLevel), "values(1, '1001'), (2, '1002'), (3, '1003')");
        session = assertEndTransaction(txnSession, "commit");
        assertQuery(session, format("select * from %s", tableNameForIsolationLevel), "values(1, '1001'), (2, '1002'), (3, '1003')");
        assertQuery(getSession(), format("select * from %s", tableNameForIsolationLevel), "values(1, '1001'), (2, '1002'), (3, '1003')");

        // Support read uncommitted isolation in Iceberg connector
        txnSession = assertStartTransaction(session, "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        assertQuery(txnSession, format("select * from %s", tableNameForIsolationLevel), "values(1, '1001'), (2, '1002'), (3, '1003')");
        assertUpdate(txnSession, format("delete from %s where a < 2", tableNameForIsolationLevel), 1);
        assertUpdate(txnSession, format("update %s set a = a + 10 where b > '1002'", tableNameForIsolationLevel), 1);
        assertQuery(txnSession, format("select * from %s", tableNameForIsolationLevel), "values(2, '1002'), (13, '1003')");
        session = assertEndTransaction(txnSession, "commit");
        assertQuery(session, format("select * from %s", tableNameForIsolationLevel), "values(2, '1002'), (13, '1003')");
        assertQuery(getSession(), format("select * from %s", tableNameForIsolationLevel), "values(2, '1002'), (13, '1003')");

        assertUpdate("drop table if exists " + tableNameForIsolationLevel);
    }

    @Test
    public void testNotAllowCertainDDLStatementInNonAutoCommitTransaction()
    {
        Session session = getQueryRunner().getDefaultSession();
        String catalog = session.getCatalog().get();
        String schema = session.getSchema().get();
        String tableNameForCreate = "test_non_autocommit_table_for_create";

        // CREATE TABLE
        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, format("create table %s(a int, b varchar)", tableNameForCreate),
                "CREATE TABLE cannot be called within a transaction \\(use autocommit mode\\) in Iceberg connector\\.");
        session = assertEndTransaction(txnSession, "rollback");
        assertQueryFails(session, "select * from " + tableNameForCreate,
                format("Table %s.%s.%s does not exist", catalog, schema, tableNameForCreate));

        String tableNameForDDL = "test_non_autocommit_table_for_ddl";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableNameForDDL));
        assertUpdate(session, format("insert into %s values(1, '1001'), (2, '1002')", tableNameForDDL), 2);

        // DROP TABLE
        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, "drop table " + tableNameForDDL,
                "DROP TABLE cannot be called within a transaction \\(use autocommit mode\\) in Iceberg connector\\.");
        session = assertEndTransaction(txnSession, "rollback");
        assertQuery(session, "select * from " + tableNameForDDL, "values(1, '1001'), (2, '1002')");

        // RENAME TABLE
        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, "alter table " + tableNameForDDL + " rename to test_rename_to_new_table",
                "RENAME TABLE cannot be called within a transaction \\(use autocommit mode\\) in Iceberg connector\\.");
        session = assertEndTransaction(txnSession, "rollback");
        assertQuery(session, "select * from " + tableNameForDDL, "values(1, '1001'), (2, '1002')");

        // CREATE SCHEMA
        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, "create schema " + catalog + ".test_new_create_schema",
                "CREATE SCHEMA cannot be called within a transaction \\(use autocommit mode\\) in Iceberg connector\\.");
        session = assertEndTransaction(txnSession, "rollback");

        // DROP SCHEMA
        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, "drop schema " + catalog + "." + schema,
                "DROP SCHEMA cannot be called within a transaction \\(use autocommit mode\\) in Iceberg connector\\.");
        session = assertEndTransaction(txnSession, "rollback");

        // RENAME SCHEMA
        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, "alter schema " + catalog + "." + schema + " rename to new_schema_name",
                "RENAME SCHEMA cannot be called within a transaction \\(use autocommit mode\\) in Iceberg connector\\.");
        session = assertEndTransaction(txnSession, "rollback");

        assertQuery(session, "select * from " + tableNameForDDL, "values(1, '1001'), (2, '1002')");
        assertUpdate(session, "drop table " + tableNameForDDL);
    }

    @Test
    public void testNotAllowViewDDLInNonAutoCommitTransaction()
    {
        skipTestUnless(supportsViews());
        Session session = getQueryRunner().getDefaultSession();
        String catalog = session.getCatalog().get();
        String schema = session.getSchema().get();
        String tableNameForDDL = "test_table_for_create_view";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableNameForDDL));
        assertUpdate(session, format("insert into %s values(1, '1001'), (2, '1002')", tableNameForDDL), 2);
        String viewNameForDDL = "test_non_autocommit_view_for_ddl";

        // CREATE VIEW
        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, format("create view %s as select * from %s", viewNameForDDL, tableNameForDDL),
                "CREATE VIEW cannot be called within a transaction \\(use autocommit mode\\) in Iceberg connector\\.");
        session = assertEndTransaction(txnSession, "rollback");
        assertQueryFails(session, "select * from " + viewNameForDDL,
                format("Table %s.%s.%s does not exist", catalog, schema, viewNameForDDL));

        assertUpdate(session, format("create view %s as select * from %s", viewNameForDDL, tableNameForDDL));

        // DROP VIEW
        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, "drop view " + viewNameForDDL,
                "DROP VIEW cannot be called within a transaction \\(use autocommit mode\\) in Iceberg connector\\.");
        session = assertEndTransaction(txnSession, "rollback");

        // RENAME VIEW
        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, "alter view " + viewNameForDDL + " rename to test_rename_to_new_view",
                "RENAME VIEW cannot be called within a transaction \\(use autocommit mode\\) in Iceberg connector\\.");
        session = assertEndTransaction(txnSession, "rollback");

        assertQuery(session, "select * from " + viewNameForDDL, "values(1, '1001'), (2, '1002')");
        assertUpdate(session, "drop view " + viewNameForDDL);
        assertUpdate(session, "drop table " + tableNameForDDL);
    }

    @Test
    public void testNotAllowCallProceduresInNonAutoCommitTransaction()
    {
        Session session = getQueryRunner().getDefaultSession();
        String catalog = session.getCatalog().get();
        String schema = session.getSchema().get();
        String tableNameForProcedure = "test_non_autocommit_table_for_procedure";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableNameForProcedure));
        assertUpdate(session, format("insert into %s values(1, '1001'), (2, '1002')", tableNameForProcedure), 2);

        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, format("CALL %s.system.remove_orphan_files('%s', '%s', TIMESTAMP '2025-05-31 00:00:00.000')", catalog, schema, tableNameForProcedure),
                "Procedures cannot be called within a transaction \\(use autocommit mode\\)");
        session = assertEndTransaction(txnSession, "rollback");

        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQueryFails(txnSession, format("CALL %s.system.expire_snapshots('%s', '%s', TIMESTAMP '1984-12-08 00:10:00.000')", catalog, schema, tableNameForProcedure),
                "Procedures cannot be called within a transaction \\(use autocommit mode\\)");
        session = assertEndTransaction(txnSession, "rollback");

        assertUpdate(session, "drop table " + tableNameForProcedure);
    }

    @Test
    public void testNotAllowMultiTableWritesInNonAutocommitTransaction()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableName1 = "test_non_autocommit_table1";
        String tableName2 = "test_non_autocommit_table2";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableName1));
        assertUpdate(session, format("insert into %s values(1, '1001'), (2, '1002'), (3, '1003')", tableName1), 3);
        assertUpdate(session, format("create table %s(a int, b varchar)", tableName2));
        assertUpdate(session, format("insert into %s values(1, '1001'), (3, '1003'), (5, '1005')", tableName2), 3);

        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQuery(txnSession, "select * from " + tableName1, "values(1, '1001'), (2, '1002'), (3, '1003')");
        assertUpdate(txnSession, format("insert into %s values(4, '1004')", tableName1), 1);

        assertQuery(txnSession, "select * from " + tableName2, "values(1, '1001'), (3, '1003'), (5, '1005')");
        assertQueryFails(txnSession, format("insert into %s values(2, '1002')", tableName2),
                "Not allowed to open write transactions on multiple tables");
        session = assertEndTransaction(txnSession, "rollback");
        assertQuery(session, "select * from " + tableName1, "values(1, '1001'), (2, '1002'), (3, '1003')");
        assertQuery(session, "select * from " + tableName2, "values(1, '1001'), (3, '1003'), (5, '1005')");

        assertUpdate("drop table " + tableName1);
        assertUpdate("drop table " + tableName2);
    }

    @Test
    public void testSingleTableMultipleInsertsTransaction()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableName = "test_non_autocommit_table_for_inserts";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableName));

        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertUpdate(txnSession, format("insert into %s values(1, '1001')", tableName), 1);
        assertUpdate(txnSession, format("insert into %s values(2, '1002')", tableName), 1);
        assertUpdate(txnSession, format("insert into %s values(3, '1003'), (4, '1004')", tableName), 2);
        session = assertEndTransaction(txnSession, "rollback");
        assertQuery(session, "select count(*) from " + tableName, "values(0)");

        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertUpdate(txnSession, format("insert into %s values(1, '1001')", tableName), 1);
        assertUpdate(txnSession, format("insert into %s values(2, '1002')", tableName), 1);
        assertUpdate(txnSession, format("insert into %s values(3, '1003'), (4, '1004')", tableName), 2);

        // Can read its own writes
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
        assertQuery(getSession(), "select count(*) from " + tableName, "values(0)");
        session = assertEndTransaction(txnSession, "commit");
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");

        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testSingleTableMixedOperationsTransaction()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableName = "test_non_autocommit_table_for_mix_operations";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableName));
        assertUpdate(session, format("insert into %s values(1, '1001'), (2, '1002'), (3, '1003')", tableName), 3);

        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");
        assertUpdate(txnSession, format("insert into %s values(4, '1004')", tableName), 1);

        // Can read its own writes
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
        assertUpdate(txnSession, format("delete from %s where a > 2", tableName), 2);
        assertUpdate(txnSession, format("update %s set a = 1000 + a where b < '1004'", tableName), 2);
        assertQuery(txnSession, "select * from " + tableName, "values(1001, '1001'), (1002, '1002')");

        session = assertEndTransaction(txnSession, "rollback");
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");

        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");
        assertUpdate(txnSession, format("insert into %s values(4, '1004')", tableName), 1);

        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
        assertUpdate(txnSession, format("delete from %s where a > 1", tableName), 3);
        assertUpdate(txnSession, format("insert into %s values(5, '1005')", tableName), 1);
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (5, '1005')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");

        session = assertEndTransaction(txnSession, "commit");
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (5, '1005')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (5, '1005')");

        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (5, '1005')");
        assertUpdate(txnSession, format("update %s set a = a + 1000 where b < '1005'", tableName), 1);
        assertUpdate(txnSession, format("insert into %s values(2, '1002')", tableName), 1);
        assertUpdate(txnSession, format("insert into %s values(3, '1003')", tableName), 1);
        assertQuery(txnSession, "select * from " + tableName, "values(1001, '1001'), (2, '1002'), (3, '1003'), (5, '1005')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (5, '1005')");

        session = assertEndTransaction(txnSession, "commit");
        assertQuery(session, "select * from " + tableName, "values(1001, '1001'), (2, '1002'), (3, '1003'), (5, '1005')");
        assertQuery(getSession(), "select * from " + tableName, "values(1001, '1001'), (2, '1002'), (3, '1003'), (5, '1005')");

        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testInsertBySubqueryInMixedOperationsTransaction()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableName = "test_non_autocommit_table_for_insert_by_subquery_operations";
        assertUpdate(session, format("create table %s as select * from lineitem with no data", tableName), 0);
        long totalCount = (long) getQueryRunner().execute(session, "select count(*) from lineitem").getOnlyValue();
        long rowCountWithReturnFlag = (long) getQueryRunner().execute(session, "select count(*) from lineitem where returnflag = 'N'").getOnlyValue();

        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQuery(txnSession, "select count(*) from " + tableName, "values(0)");
        assertUpdate(txnSession, format("insert into %s select * from lineitem", tableName), totalCount);

        // Can read its own writes
        assertQuery(txnSession, "select count(*) from " + tableName, format("values(%s)", totalCount));
        assertUpdate(txnSession, format("delete from %s where returnflag = 'N'", tableName), rowCountWithReturnFlag);
        assertQuery(txnSession, "select count(*) from " + tableName, format("values(%s)", totalCount - rowCountWithReturnFlag));
        assertQuery(getSession(), "select count(*) from " + tableName, "values(0)");
        session = assertEndTransaction(txnSession, "rollback");
        assertQuery(session, "select count(*) from " + tableName, "values(0)");
        assertQuery(getSession(), "select count(*) from " + tableName, "values(0)");

        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertUpdate(txnSession, format("insert into %s select * from lineitem", tableName), totalCount);
        // Can read its own writes
        assertQuery(txnSession, "select count(*) from " + tableName, format("values(%s)", totalCount));
        assertUpdate(txnSession, format("delete from %s where returnflag = 'N'", tableName), rowCountWithReturnFlag);
        assertQuery(txnSession, "select count(*) from " + tableName, format("values(%s)", totalCount - rowCountWithReturnFlag));
        assertQuery(getSession(), "select count(*) from " + tableName, "values(0)");
        session = assertEndTransaction(txnSession, "commit");
        assertQuery(session, "select count(*) from " + tableName, format("values(%s)", totalCount - rowCountWithReturnFlag));
        assertQuery(getSession(), "select count(*) from " + tableName, format("values(%s)", totalCount - rowCountWithReturnFlag));

        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testMixedOperationsOnSinglePartitionTable()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableName = "test_non_autocommit_partition_table_for_mix_operations";
        assertUpdate(session, format("create table %s(a int, b varchar) with(partitioning = ARRAY['a'])", tableName));
        assertUpdate(session, format("insert into %s values(1, '1001'), (2, '1002'), (3, '1003')", tableName), 3);

        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");
        assertUpdate(txnSession, format("insert into %s values(4, '1004')", tableName), 1);

        // Can read its own writes
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");

        // This should do metadata delete
        assertUpdate(txnSession, format("delete from %s where a > 2", tableName), 2);
        assertUpdate(txnSession, format("update %s set a = 1000 + a where b < '1004'", tableName), 2);
        assertQuery(txnSession, "select * from " + tableName, "values(1001, '1001'), (1002, '1002')");

        session = assertEndTransaction(txnSession, "rollback");
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");

        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");
        assertUpdate(txnSession, format("insert into %s values(4, '1004')", tableName), 1);

        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");

        // This should do metadata delete
        assertUpdate(txnSession, format("delete from %s where a > 1", tableName), 3);
        assertUpdate(txnSession, format("insert into %s values(5, '1005')", tableName), 1);
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (5, '1005')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");

        session = assertEndTransaction(txnSession, "commit");
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (5, '1005')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (5, '1005')");

        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001'), (5, '1005')");
        assertUpdate(txnSession, format("update %s set a = a + 1000 where b < '1005'", tableName), 1);
        assertUpdate(txnSession, format("insert into %s values(2, '1002')", tableName), 1);
        assertUpdate(txnSession, format("insert into %s values(3, '1003')", tableName), 1);
        assertQuery(txnSession, "select * from " + tableName, "values(1001, '1001'), (2, '1002'), (3, '1003'), (5, '1005')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (5, '1005')");

        session = assertEndTransaction(txnSession, "commit");
        assertQuery(session, "select * from " + tableName, "values(1001, '1001'), (2, '1002'), (3, '1003'), (5, '1005')");
        assertQuery(getSession(), "select * from " + tableName, "values(1001, '1001'), (2, '1002'), (3, '1003'), (5, '1005')");

        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testSingleTableSchemaConflictTransaction()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableName = "test_schema_conflict_for_single_table";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableName));
        assertUpdate(session, format("insert into %s values(1, '1001')", tableName), 1);

        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertUpdate(txnSession, format("delete from %s", tableName), 1);
        assertUpdate(txnSession, format("insert into %s values(2, '1002')", tableName), 1);
        assertUpdate(txnSession, format("alter table %s rename column b to b_new", tableName));
        assertUpdate(txnSession, format("insert into %s(b_new) values('1003')", tableName), 1);
        assertQuery(txnSession, "select * from " + tableName, "values(null, '1003'), (2, '1002')");
        assertQueryFails(txnSession, format("alter table %s drop column b", tableName), ".* Column 'b' does not exist");

        session = assertEndTransaction(txnSession, "rollback");
        assertQuery(session, "select * from " + tableName, "values(1, '1001')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001')");

        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testSingleTableSchemaConflictBetweenTransactions()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableName = "test_schema_conflict_between_transactions";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableName));
        assertUpdate(session, format("insert into %s values(1, '1001')", tableName), 1);

        Session txnSession1 = assertStartTransaction(session, "START TRANSACTION");
        Session txnSession2 = assertStartTransaction(session, "START TRANSACTION");
        assertUpdate(txnSession1, format("delete from %s", tableName), 1);
        assertUpdate(txnSession1, format("insert into %s values(2, '1002')", tableName), 1);
        assertUpdate(txnSession2, format("alter table %s drop column b", tableName));
        assertUpdate(txnSession2, format("insert into %s values 5", tableName), 1);
        assertUpdate(txnSession1, format("alter table %s rename column b to b_new", tableName));
        assertUpdate(txnSession1, format("insert into %s(b_new) values('1003')", tableName), 1);
        assertQuery(txnSession1, "select * from " + tableName, "values(null, '1003'), (2, '1002')");
        assertQuery(txnSession2, "select * from " + tableName, "values(1), (5)");

        // Commit transaction2 which updates the table for dropping column "b" (which causes in-progress transaction1 commit fail)
        session = assertEndTransaction(txnSession2, "commit");

        // Fail to commit transaction1 which includes the action for renaming column "b"
        assertQueryFails(txnSession1, "commit", "Table metadata refresh is required");

        assertQuery(session, "select * from " + tableName, "values(1), (5)");
        assertQuery(getSession(), "select * from " + tableName, "values(1), (5)");

        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testSingleTableSchemaUpdateVisibilityTransaction()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableName = "test_schema_update_visibility_for_single_table";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableName));
        assertUpdate(session, format("insert into %s values(1, '1001')", tableName), 1);

        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001')");
        assertUpdate(txnSession, format("alter table %s add column c double", tableName));
        assertUpdate(txnSession, format("insert into %s values(2, '1002', 1.2)", tableName), 1);
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001', null), (2, '1002', 1.2)");
        assertUpdate(txnSession, format("alter table %s drop column a", tableName));
        assertUpdate(txnSession, format("insert into %s values('1003', 1.3)", tableName), 1);
        assertQuery(txnSession, "select * from " + tableName, "values('1001', null), ('1002', 1.2), ('1003', 1.3)");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001')");

        session = assertEndTransaction(txnSession, "rollback");
        assertQuery(session, "select * from " + tableName, "values(1, '1001')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001')");

        txnSession = assertStartTransaction(session, "START TRANSACTION");
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001')");
        assertUpdate(txnSession, format("alter table %s add column c double", tableName));
        assertUpdate(txnSession, format("insert into %s values(2, '1002', 1.2)", tableName), 1);
        assertQuery(txnSession, "select * from " + tableName, "values(1, '1001', null), (2, '1002', 1.2)");
        assertUpdate(txnSession, format("alter table %s drop column a", tableName));
        assertUpdate(txnSession, format("insert into %s values('1003', 1.3)", tableName), 1);
        assertQuery(txnSession, "select * from " + tableName, "values('1001', null), ('1002', 1.2), ('1003', 1.3)");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001')");

        session = assertEndTransaction(txnSession, "commit");
        assertQuery(session, "select * from " + tableName, "values('1001', null), ('1002', 1.2), ('1003', 1.3)");
        assertQuery(getSession(), "select * from " + tableName, "values('1001', null), ('1002', 1.2), ('1003', 1.3)");

        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testMultipleConcurrentTransactionsIsolation()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableName = "test_multiple_concurrent_transactions_isolation";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableName));
        assertUpdate(session, format("insert into %s values(1, '1001'), (2, '1002'), (3, '1003')", tableName), 3);

        Session txnSession1 = assertStartTransaction(session, "START TRANSACTION");
        Session txnSession2 = assertStartTransaction(session, "START TRANSACTION");

        assertQuery(txnSession1, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");
        assertQuery(txnSession2, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");
        assertUpdate(txnSession1, format("insert into %s values(4, '1004')", tableName), 1);
        assertUpdate(txnSession2, format("insert into %s values(5, '1005')", tableName), 1);

        // transaction1 can just read its own writes
        assertQuery(txnSession1, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
        // transaction2 can just read its own writes
        assertQuery(txnSession2, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (5, '1005')");
        // Cannot read any change outside transaction1 and transaction2
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");

        Session session1 = assertEndTransaction(txnSession1, "commit");
        // Can read the writes of transaction1 from outside
        assertQuery(session1, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
        // transaction2 can still just read its own writes, unaware of outside change
        assertQuery(txnSession2, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (5, '1005')");

        Session session2 = assertEndTransaction(txnSession2, "commit");
        // Can read the writes of transaction1 and transaction2 from outside
        assertQuery(session1, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004'), (5, '1005')");
        assertQuery(session2, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004'), (5, '1005')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004'), (5, '1005')");

        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testMultipleConcurrentTransactionsWriteSkew()
    {
        Session session = getQueryRunner().getDefaultSession();
        String tableName = "test_multiple_concurrent_transactions_skew";
        assertUpdate(session, format("create table %s(a int, b varchar)", tableName));
        assertUpdate(session, format("insert into %s values(1, '1001'), (2, '1002'), (3, '1003')", tableName), 3);

        Session txnSession1 = assertStartTransaction(session, "START TRANSACTION");
        Session txnSession2 = assertStartTransaction(session, "START TRANSACTION");

        assertUpdate(txnSession1, format("insert into %s values(4, '1004')", tableName), 1);
        assertUpdate(txnSession2, format("insert into %s values(5, '1005')", tableName), 1);
        assertUpdate(txnSession1, format("delete from %s where a > 2", tableName), 2);
        assertUpdate(txnSession2, format("update %s set a = a + 10 where b < '1003'", tableName), 2);

        // transaction1 can just read its own writes
        assertQuery(txnSession1, "select * from " + tableName, "values(1, '1001'), (2, '1002')");
        // transaction2 can just read its own writes
        assertQuery(txnSession2, "select * from " + tableName, "values(11, '1001'), (12, '1002'), (3, '1003'), (5, '1005')");
        // Cannot read any change outside transaction1 and transaction2
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");

        // transaction1 commit successfully
        Session session1 = assertEndTransaction(txnSession1, "commit");
        // Can read the writes of transaction1 from outside
        assertQuery(session1, "select * from " + tableName, "values(1, '1001'), (2, '1002')");
        assertQuery(getSession(), "select * from " + tableName, "values(1, '1001'), (2, '1002')");
        // transaction2 can just read its own writes, unaware of outside change
        assertQuery(txnSession2, "select * from " + tableName, "values(11, '1001'), (12, '1002'), (3, '1003'), (5, '1005')");

        // The commission of transaction2 lead in some kind of write skew
        Session session2 = assertEndTransaction(txnSession2, "commit");
        assertQuery(session1, "select * from " + tableName, "values(11, '1001'), (12, '1002'), (5, '1005')");
        assertQuery(session2, "select * from " + tableName, "values(11, '1001'), (12, '1002'), (5, '1005')");
        assertQuery(getSession(), "select * from " + tableName, "values(11, '1001'), (12, '1002'), (5, '1005')");

        assertUpdate("drop table " + tableName);
    }
}
