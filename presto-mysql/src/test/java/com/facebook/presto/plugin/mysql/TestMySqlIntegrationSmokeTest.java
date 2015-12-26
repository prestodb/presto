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
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import io.airlift.testing.mysql.TestingMySqlServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestMySqlIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingMySqlServer mysqlServer;

    public TestMySqlIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingMySqlServer("testuser", "testpass", "tpch", "test_database"));
    }

    public TestMySqlIntegrationSmokeTest(TestingMySqlServer mysqlServer)
            throws Exception
    {
        super(createMySqlQueryRunner(mysqlServer, ORDERS));
        this.mysqlServer = mysqlServer;
    }

    @Override
    public void testDescribeTable()
            throws Exception
    {
        // we need specific implementation of this tests due to specific Presto<->Mysql varchar length mapping.
        MaterializedResult actualColumns = computeActual("DESC ORDERS").toJdbcTypes();

        // some connectors don't support dates, and some do not support parametrized varchars, so we check multiple options
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "")
                .row("custkey", "bigint", "")
                .row("orderstatus", "varchar(255)", "")
                .row("totalprice", "double", "")
                .row("orderdate", "date", "")
                .row("orderpriority", "varchar(255)", "")
                .row("clerk", "varchar(255)", "")
                .row("shippriority", "integer", "")
                .row("comment", "varchar(255)", "")
                .build();
        assertEquals(actualColumns, expectedColumns);
    }

    @Override
    public void testViewAccessControl()
    {
        // jdbc connector does not support views
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        closeAllRuntimeException(mysqlServer);
    }

    @Test
    public void testInsert()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testNameEscaping()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("mysql")
                .setSchema("test_database")
                .build();

        assertFalse(queryRunner.tableExists(session, "test_table"));

        assertUpdate(session, "CREATE TABLE test_table AS SELECT 123 x", 1);
        assertTrue(queryRunner.tableExists(session, "test_table"));

        assertQuery(session, "SELECT * FROM test_table", "SELECT 123");

        assertUpdate(session, "DROP TABLE test_table");
        assertFalse(queryRunner.tableExists(session, "test_table"));
    }

    @Test
    public void testMySqlTinyint1()
            throws Exception
    {
        execute("CREATE TABLE tpch.mysql_test_tinyint1 (c_tinyint tinyint(1))");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM mysql_test_tinyint1");
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), TINYINT)
                .row("c_tinyint", "tinyint", "").build();

        assertEquals(actual, expected);

        execute("INSERT INTO tpch.mysql_test_tinyint1 VALUES (127), (-128)");
        MaterializedResult materializedRows = computeActual("SELECT * FROM tpch.mysql_test_tinyint1 WHERE c_tinyint = 127");
        assertEquals(materializedRows.getRowCount(), 1);
        MaterializedRow row = getOnlyElement(materializedRows);

        assertEquals(row.getFields().size(), 1);
        assertEquals(row.getField(0), (byte) 127);

        assertUpdate("DROP TABLE mysql_test_tinyint1");
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
