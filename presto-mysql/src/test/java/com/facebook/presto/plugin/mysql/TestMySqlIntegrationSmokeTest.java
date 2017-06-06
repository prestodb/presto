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
import com.facebook.presto.spi.type.VarcharType;
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
import java.sql.Timestamp;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.ORDERS;
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
        super(() -> createMySqlQueryRunner(mysqlServer, ORDERS));
        this.mysqlServer = mysqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        mysqlServer.close();
    }

    @Override
    public void testDescribeTable()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_describe_table (c_bigint bigint, c_varchar_255 varchar(255), c_varchar_15 varchar(15), c_double double, c_date date, c_integer integer, c_timestamp timestamp)");
        MaterializedResult actualColumns = computeActual("DESC test_describe_table").toJdbcTypes();
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("c_bigint", "bigint", "", "")
                .row("c_varchar_255", "varchar(255)", "", "")
                .row("c_varchar_15", "varchar(15)", "", "")
                .row("c_double", "double", "", "")
                .row("c_date", "date", "", "")
                .row("c_integer", "integer", "", "")
                .row("c_timestamp", "timestamp", "", "")
                .build();
        assertEquals(actualColumns, expectedColumns);
        assertUpdate("DROP TABLE test_describe_table");
    }

    @Test
    public void testCreateTableFromSelect()
            throws Exception
    {
        execute("CREATE TABLE tpch.temp_table_with_timestamp_column (c_timestamp timestamp)");
        assertUpdate(getQueryRunner().getDefaultSession(), "INSERT INTO temp_table_with_timestamp_column VALUES (date_parse('1970-12-01 00:00:01','%Y-%m-%d %H:%i:%s'))", 1);
        assertUpdate(getQueryRunner().getDefaultSession(), "CREATE TABLE test_create_table_as_select AS SELECT * FROM temp_table_with_timestamp_column", 1);
        assertUpdate("DROP TABLE temp_table_with_timestamp_column");
        assertUpdate("DROP TABLE test_create_table_as_select");
    }

    @Test
    public void testInsert()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert (c_bigint bigint, c_varchar_100 varchar(100), c_timestamp timestamp)");
        assertUpdate(getQueryRunner().getDefaultSession(), "INSERT INTO test_insert VALUES (123, 'test', date_parse('1970-12-01 00:00:01','%Y-%m-%d %H:%i:%s'))", 1);
        assertUpdate(getQueryRunner().getDefaultSession(), "INSERT INTO test_insert VALUES (123, 'test', date_parse('2030-01-01 23:59:59','%Y-%m-%d %H:%i:%s'))", 1);
        MaterializedResult actual = computeActual("SELECT * FROM test_insert");
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), BIGINT, VarcharType.createVarcharType(100), TIMESTAMP)
                .row((long) 123, "test", Timestamp.valueOf("1970-12-01 00:00:01"))
                .row((long) 123, "test", Timestamp.valueOf("2030-01-01 23:59:59"))
                .build();
        assertEquals(actual, expected);
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

        assertFalse(getQueryRunner().tableExists(session, "test_table"));

        assertUpdate(session, "CREATE TABLE test_table AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(session, "test_table"));

        assertQuery(session, "SELECT * FROM test_table", "SELECT 123");

        assertUpdate(session, "DROP TABLE test_table");
        assertFalse(getQueryRunner().tableExists(session, "test_table"));
    }

    @Test
    public void testMySqlTinyint1()
            throws Exception
    {
        execute("CREATE TABLE tpch.mysql_test_tinyint1 (c_tinyint tinyint(1))");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM mysql_test_tinyint1");
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("c_tinyint", "tinyint", "", "")
                .build();

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
