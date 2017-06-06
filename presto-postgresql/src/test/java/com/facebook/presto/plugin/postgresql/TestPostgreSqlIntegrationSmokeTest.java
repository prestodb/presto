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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertTrue;

@Test
public class TestPostgreSqlIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestPostgreSqlIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "tpch"));
    }

    public TestPostgreSqlIntegrationSmokeTest(TestingPostgreSqlServer postgreSqlServer)
            throws Exception
    {
        super(() -> PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgreSqlServer, ORDERS));
        this.postgreSqlServer = postgreSqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        postgreSqlServer.close();
    }

    @Test
    public void testDescribeTable()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_decribe_table (c_timestamp timestamp without time zone, c_timestamp_tz timestamp with time zone)");
        MaterializedResult actualColumns = computeActual(getQueryRunner().getDefaultSession(), "DESC test_decribe_table").toJdbcTypes();
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("c_timestamp", "timestamp", "", "")
                .row("c_timestamp_tz", "timestamp", "", "")
                .build();
        assertEquals(actualColumns, expectedColumns);
        assertUpdate("DROP TABLE test_decribe_table");
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
        execute("CREATE TABLE tpch.test_insert (c_bigint bigint, c_varchar varchar, c_timestamp timestamp)");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test', date_parse('1970-12-01 00:00:01','%Y-%m-%d %H:%i:%s'))", 1);
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test', date_parse('2030-01-01 23:59:59','%Y-%m-%d %H:%i:%s'))", 1);
        MaterializedResult actual = computeActual("SELECT * FROM test_insert");
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), BIGINT, VARCHAR, TIMESTAMP)
                .row((long) 123, "test", Timestamp.valueOf("1970-12-01 00:00:01"))
                .row((long) 123, "test", Timestamp.valueOf("2030-01-01 23:59:59"))
                .build();
        assertEquals(actual, expected);
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testMaterializedView()
            throws Exception
    {
        execute("CREATE MATERIALIZED VIEW tpch.test_mv as SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_mv"));
        assertQuery("SELECT orderkey FROM test_mv", "SELECT orderkey FROM orders");
        execute("DROP MATERIALIZED VIEW tpch.test_mv");
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
