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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.mysql.MySqlOptions;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestMySqlIntegrationMixedCaseTest
        extends AbstractTestQueryFramework
{
    private static final MySqlOptions MY_SQL_OPTIONS = MySqlOptions.builder()
            .build();

    private final TestingMySqlServer mysqlServer;

    public TestMySqlIntegrationMixedCaseTest()
            throws Exception
    {
        this.mysqlServer = new TestingMySqlServer("testuser", "testpass", ImmutableList.of("tpch", "Mixed_Test_Database"), MY_SQL_OPTIONS);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMySqlQueryRunner(mysqlServer, ImmutableMap.of("case-sensitive-name-matching", "true"), TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        mysqlServer.close();
    }

    public void testDescribeTable()
    {
        // CI tests run on Linux, where MySQL is case-sensitive by default (lower_case_table_names=0),
        // treating "orders" and "ORDERS" as different tables.
        // Since the test runs with case-sensitive-name-matching=true, ensure "ORDERS" exists if not already present.
        try {
            execute("CREATE TABLE IF NOT EXISTS tpch.ORDERS AS SELECT * FROM tpch.orders");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // we need specific implementation of this tests due to specific Presto<->Mysql varchar length mapping.
        MaterializedResult actualColumns = computeActual("DESC ORDERS").toTestTypes();

        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", 19L, null, null)
                .row("custkey", "bigint", "", "", 19L, null, null)
                .row("orderstatus", "varchar(255)", "", "", null, null, 255L)
                .row("totalprice", "double", "", "", 53L, null, null)
                .row("orderdate", "date", "", "", null, null, null)
                .row("orderpriority", "varchar(255)", "", "", null, null, 255L)
                .row("clerk", "varchar(255)", "", "", null, null, 255L)
                .row("shippriority", "integer", "", "", 10L, null, null)
                .row("comment", "varchar(255)", "", "", null, null, 255L)
                .build();
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    public void testCreateTable()
    {
        Session session = testSessionBuilder()
                .setCatalog("mysql")
                .setSchema("Mixed_Test_Database")
                .build();

        getQueryRunner().execute(session, "CREATE TABLE TEST_CREATE(name VARCHAR(50), id int)");
        assertTrue(getQueryRunner().tableExists(session, "TEST_CREATE"));

        getQueryRunner().execute(session, "CREATE TABLE IF NOT EXISTS test_create(name VARCHAR(50), id int)");
        assertTrue(getQueryRunner().tableExists(session, "test_create"));

        assertUpdate(session, "DROP TABLE IF EXISTS TEST_CREATE");
        assertFalse(getQueryRunner().tableExists(session, "TEST_CREATE"));

        assertUpdate(session, "DROP TABLE IF EXISTS test_create");
        assertFalse(getQueryRunner().tableExists(session, "test_create"));
    }

    @Test
    public void testCreateTableAs()
    {
        Session session = testSessionBuilder()
                .setCatalog("mysql")
                .setSchema("Mixed_Test_Database")
                .build();

        getQueryRunner().execute(session, "CREATE TABLE TEST_CREATEAS AS SELECT * FROM tpch.region");
        assertTrue(getQueryRunner().tableExists(session, "TEST_CREATEAS"));

        getQueryRunner().execute(session, "CREATE TABLE IF NOT EXISTS test_createas AS SELECT * FROM tpch.region");
        assertTrue(getQueryRunner().tableExists(session, "test_createas"));

        getQueryRunner().execute(session, "CREATE TABLE TEST_CREATEAS_Join AS SELECT c.custkey, o.orderkey FROM " +
                "tpch.customer c INNER JOIN tpch.orders o ON c.custkey = o.custkey WHERE c.mktsegment = 'BUILDING'");
        assertTrue(getQueryRunner().tableExists(session, "TEST_CREATEAS_Join"));

        assertQueryFails("CREATE TABLE Mixed_Test_Database.TEST_CREATEAS_FAIL_Join AS SELECT c.custkey, o.orderkey FROM " +
                "tpch.customer c INNER JOIN tpch.ORDERS1 o ON c.custkey = o.custkey WHERE c.mktsegment = 'BUILDING'", "Table mysql.tpch.ORDERS1 does not exist"); //failure scenario since tpch.ORDERS1 doesn't exist
        assertFalse(getQueryRunner().tableExists(session, "TEST_CREATEAS_FAIL_Join"));

        getQueryRunner().execute(session, "CREATE TABLE Test_CreateAs_Mixed_Join AS SELECT Cus.custkey, Ord.orderkey FROM " +
                "tpch.customer Cus INNER JOIN tpch.orders Ord ON Cus.custkey = Ord.custkey WHERE Cus.mktsegment = 'BUILDING'");
        assertTrue(getQueryRunner().tableExists(session, "Test_CreateAs_Mixed_Join"));
    }

    @Test
    public void testInsert()
    {
        Session session = testSessionBuilder()
                .setCatalog("mysql")
                .setSchema("Mixed_Test_Database")
                .build();

        getQueryRunner().execute(session, "CREATE TABLE Test_Insert (x bigint, y varchar(100))");
        getQueryRunner().execute(session, "INSERT INTO Test_Insert VALUES (123, 'test')");
        assertTrue(getQueryRunner().tableExists(session, "Test_Insert"));
        assertQuery("SELECT * FROM Mixed_Test_Database.Test_Insert", "SELECT 123 x, 'test' y");

        getQueryRunner().execute(session, "CREATE TABLE IF NOT EXISTS TEST_INSERT (x bigint, y varchar(100))");
        getQueryRunner().execute(session, "INSERT INTO TEST_INSERT VALUES (1234, 'test1')");
        assertTrue(getQueryRunner().tableExists(session, "TEST_INSERT"));

        getQueryRunner().execute(session, "DROP TABLE IF EXISTS Test_Insert");
        getQueryRunner().execute(session, "DROP TABLE IF EXISTS TEST_INSERT");
    }

    @Test
    public void testSelectInformationSchemaColumnIsNullable()
    {
        assertUpdate("CREATE TABLE test_column (name VARCHAR NOT NULL, email VARCHAR)");
        assertQueryFails("SELECT is_nullable FROM Information_Schema.columns WHERE table_name = 'test_column'", "Schema Information_Schema does not exist");
        assertQuery("SELECT is_nullable FROM information_schema.columns WHERE table_name = 'test_column'", "VALUES 'NO','YES'");
    }

    @Test
    public void testDuplicatedRowCreateTable()
    {
        assertQueryFails("CREATE TABLE test (a integer, a integer)",
                "line 1:31: Column name 'a' specified more than once");
        assertQueryFails("CREATE TABLE TEST (a integer, a integer)",
                "line 1:31: Column name 'a' specified more than once");
        assertQueryFails("CREATE TABLE test (a integer, orderkey integer, LIKE orders INCLUDING PROPERTIES)",
                "line 1:49: Column name 'orderkey' specified more than once");

        assertQueryFails("CREATE TABLE test (a integer, A integer)",
                "Duplicate column name 'A'");
        assertQueryFails("CREATE TABLE TEST (a integer, A integer)",
                "Duplicate column name 'A'");
        assertQueryFails("CREATE TABLE test (a integer, OrderKey integer, LIKE orders INCLUDING PROPERTIES)",
                "Duplicate column name 'orderkey'");
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(mysqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
