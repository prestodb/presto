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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.mysql.MySqlOptions;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestMySqlIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private static final MySqlOptions MY_SQL_OPTIONS = MySqlOptions.builder()
            .build();

    private final TestingMySqlServer mysqlServer;

    public TestMySqlIntegrationSmokeTest()
            throws Exception
    {
        this.mysqlServer = new TestingMySqlServer("testuser", "testpass", ImmutableList.of("tpch", "test_database"), MY_SQL_OPTIONS);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMySqlQueryRunner(mysqlServer, ORDERS);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        mysqlServer.close();
    }

    @Override
    public void testDescribeTable()
    {
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
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE test_drop AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_drop"));

        assertUpdate("DROP TABLE test_drop");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop"));
    }

    @Test
    public void testIgnoredSchemas()
    {
        MaterializedResult actual = computeActual("SHOW SCHEMAS");
        assertFalse(actual.getMaterializedRows().stream().anyMatch(schemaResult -> schemaResult.getField(0).equals("mysql")));
    }

    @Test
    public void testViews()
            throws SQLException
    {
        execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        execute("DROP VIEW IF EXISTS tpch.test_view");
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
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("c_tinyint", "tinyint", "", "", 3L, null, null)
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

    @Test
    public void testMysqlTimestamp()
    {
        // Do not support timestamp with time zone in mysql connector
        assertQueryFails("CREATE TABLE test_timestamp (x timestamp with time zone)", "Unsupported column type: timestamp with time zone");

        assertUpdate("CREATE TABLE test_timestamp (x timestamp)");
        assertUpdate("INSERT INTO test_timestamp VALUES (timestamp '1970-01-01 00:00:00')", 1);
        assertUpdate("INSERT INTO test_timestamp VALUES (timestamp '2017-05-01 10:12:34')", 1);
        assertUpdate("INSERT INTO test_timestamp VALUES (timestamp '2018-06-02 11:13:45.123')", 1);
        assertQuery("SELECT * FROM test_timestamp", "VALUES CAST('1970-01-01 00:00:00' AS TIMESTAMP)," +
                " CAST('2017-05-01 10:12:34' AS TIMESTAMP)," +
                " CAST('2018-06-02 11:13:45.123' AS TIMESTAMP)");

        assertUpdate("CREATE TABLE test_timestamp2 (x timestamp)");
        assertUpdate("INSERT INTO test_timestamp2 SELECT * from test_timestamp", 3);
        assertQuery("SELECT * FROM test_timestamp2", "VALUES CAST('1970-01-01 00:00:00' AS TIMESTAMP)," +
                " CAST('2017-05-01 10:12:34' AS TIMESTAMP)," +
                " CAST('2018-06-02 11:13:45.123' AS TIMESTAMP)");
        assertUpdate("DROP TABLE test_timestamp");
        assertUpdate("DROP TABLE test_timestamp2");
    }

    @Test
    public void testMysqlGeometry()
            throws SQLException
    {
        execute("CREATE TABLE tpch.test_geometry (g GEOMETRY)");

        execute("INSERT INTO tpch.test_geometry VALUES (ST_GeomFromText('POINT(1 2)'))");
        execute("INSERT INTO tpch.test_geometry VALUES (ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'))");
        execute("INSERT INTO tpch.test_geometry VALUES (ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))");

        assertQuery(
                "SELECT CAST(g AS VARCHAR) FROM test_geometry",
                "VALUES " +
                        "CAST('POINT (1 2)' AS VARCHAR), " +
                        "CAST('LINESTRING (0 0, 5 5, 10 10)' AS VARCHAR), " +
                        "CAST('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))' AS VARCHAR)");

        assertUpdate("DROP TABLE tpch.test_geometry");
    }

    @Test
    public void testMysqlDecimal()
    {
        assertUpdate("CREATE TABLE test_decimal (d DECIMAL(10, 2))");

        assertUpdate("INSERT INTO test_decimal VALUES (123.45)", 1);
        assertUpdate("INSERT INTO test_decimal VALUES (67890.12)", 1);
        assertUpdate("INSERT INTO test_decimal VALUES (0.99)", 1);

        assertQuery(
                "SELECT d FROM test_decimal WHERE d<200.00 AND d>0.00",
                "VALUES " +
                        "CAST('123.45' AS DECIMAL), " +
                        "CAST('0.99' AS DECIMAL)");

        assertUpdate("DROP TABLE test_decimal");
    }

    @Test
    public void testMysqlTime()
    {
        assertUpdate("CREATE TABLE test_time (datatype_time time)");

        assertUpdate("INSERT INTO test_time VALUES (time '01:02:03.456')", 1);

        assertQuery(
                "SELECT datatype_time FROM test_time",
                "VALUES " +
                        "CAST('01:02:03.456' AS time)");

        assertUpdate("DROP TABLE test_time");
    }

    @Test
    public void testMysqlUnsupportedTimeTypes()
    {
        assertQueryFails("CREATE TABLE test_timestamp_with_timezone (timestamp_with_time_zone timestamp with time zone)", "Unsupported column type: timestamp with time zone");
        assertQueryFails("CREATE TABLE test_time_with_timezone (time_with_with_time_zone time with time zone)", "Unsupported column type: time with time zone");
    }

    @Test
    public void testCharTrailingSpace()
            throws Exception
    {
        execute("CREATE TABLE tpch.char_trailing_space (x char(10))");
        assertUpdate("INSERT INTO char_trailing_space VALUES ('test')", 1);

        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test'", "VALUES 'test'");
        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test  '", "VALUES 'test'");
        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test        '", "VALUES 'test'");

        assertEquals(getQueryRunner().execute("SELECT * FROM char_trailing_space WHERE x = char ' test'").getRowCount(), 0);

        Map<String, String> properties = ImmutableMap.of("deprecated.legacy-char-to-varchar-coercion", "true");
        Map<String, String> connectorProperties = ImmutableMap.of("connection-url", mysqlServer.getJdbcUrl());

        try (QueryRunner queryRunner = new DistributedQueryRunner(getSession(), 3, properties)) {
            queryRunner.installPlugin(new MySqlPlugin());
            queryRunner.createCatalog("mysql", "mysql", connectorProperties);

            assertEquals(queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = char 'test'").getRowCount(), 0);
            assertEquals(queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = char 'test  '").getRowCount(), 0);
            assertEquals(queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = char 'test       '").getRowCount(), 0);

            MaterializedResult result = queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = char 'test      '");
            assertEquals(result.getRowCount(), 1);
            assertEquals(result.getMaterializedRows().get(0).getField(0), "test      ");
        }

        assertUpdate("DROP TABLE char_trailing_space");
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        String createTableFormat = "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                "   %s date,\n" +
                "   %s date NOT NULL\n" +
                ")";
        @Language("SQL") String createTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                "column_a",
                "column_b");
        @Language("SQL") String expectedCreateTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                "\"column_a\"",
                "\"column_b\"");
        assertUpdate(createTableSql);
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), expectedCreateTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_a) VALUES (date '2012-12-31')", "NULL value not allowed for NOT NULL column: column_b");
        assertQueryFails("INSERT INTO test_insert_not_null (column_a, column_b) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_b");

        assertUpdate("ALTER TABLE test_insert_not_null ADD COLUMN column_c BIGINT NOT NULL");

        createTableFormat = "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                "   %s date,\n" +
                "   %s date NOT NULL,\n" +
                "   %s bigint NOT NULL\n" +
                ")";
        createTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                "column_a",
                "column_b",
                "column_c");
        expectedCreateTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                "\"column_a\"",
                "\"column_b\"",
                "\"column_c\"");
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), expectedCreateTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_b) VALUES (date '2012-12-31')", "NULL value not allowed for NOT NULL column: column_c");
        assertQueryFails("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_c");

        assertUpdate("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (date '2012-12-31', 1)", 1);
        assertUpdate("INSERT INTO test_insert_not_null (column_a, column_b, column_c) VALUES (date '2013-01-01', date '2013-01-02', 2)", 1);
        assertQuery(
                "SELECT * FROM test_insert_not_null",
                "VALUES (NULL, CAST('2012-12-31' AS DATE), 1), (CAST('2013-01-01' AS DATE), CAST('2013-01-02' AS DATE), 2)");

        assertUpdate("DROP TABLE test_insert_not_null");
    }

    @Test
    public void testColumnComment()
            throws Exception
    {
        execute("create table tpch.test_column_comment (column_a char(3) comment 'first field', column_b int comment '', column_c int)");
        assertQuery(
                "SELECT column_name, comment FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_column_comment'",
                "VALUES ('column_a', 'first field'), ('column_b', null), ('column_c', null)");

        assertUpdate("DROP TABLE test_column_comment");
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(mysqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Test
    public void testSelectInformationSchemaColumnIsNullable()
    {
        assertUpdate("CREATE TABLE test_column (name VARCHAR NOT NULL, email VARCHAR)");
        assertQuery("SELECT is_nullable FROM information_schema.columns WHERE table_name = 'test_column'", "VALUES 'NO','YES'");
    }
}
